// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Pin mutations are evaluated against current membership on every commit attempt.
use super::Transaction;
use crate::format::{Manifest, OngoingWorkPin, PinTime};
use lance_core::{Error, Result, deepsize::DeepSizeOf};
use std::collections::HashSet;

/// Retention mutations. Passing an expiry time never removes a pin by itself.
#[derive(Debug, Clone, PartialEq, Eq, DeepSizeOf)]
pub enum PinOperation {
    /// Acquire the transaction's exact read snapshot; cannot rebase to another source.
    Register {
        /// UUID of the source snapshot's transaction.
        source_transaction_uuid: String,
        /// Fixed request time.
        created_at: PinTime,
        /// Initial expiry, strictly after creation.
        expires_at: PinTime,
        /// Optional immutable diagnostic label.
        job_label: Option<String>,
    },
    /// Extend a live pin. Never recreates a revoked pin.
    Renew {
        /// Existing registration UUID.
        pin_id: String,
        /// Fixed requested expiry.
        expires_at: PinTime,
    },
    /// Idempotently remove an existing registration.
    Release {
        /// Registration UUID.
        pin_id: String,
    },
    /// Compare-and-remove expired pins; renewal invalidates an old candidate.
    Expire {
        /// Inspection time, fixed across retries.
        observed_at: PinTime,
        /// Registration UUID and exact expiry observed during inspection.
        candidates: Vec<(String, PinTime)>,
    },
}
impl PinOperation {
    /// Wire operation name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Register { .. } => "RegisterPin",
            Self::Renew { .. } => "RenewPin",
            Self::Release { .. } => "ReleasePin",
            Self::Expire { .. } => "ExpirePins",
        }
    }
    /// Apply to the final commit target, returning whether membership changed.
    pub fn apply(&self, transaction: &Transaction, manifest: &mut Manifest) -> Result<bool> {
        manifest.validate_ongoing_work_pins()?;
        match self {
            Self::Register {
                source_transaction_uuid,
                created_at,
                expires_at,
                job_label,
            } => {
                let pin = OngoingWorkPin {
                    id: transaction.uuid.clone(),
                    source_version: transaction.read_version,
                    source_transaction_uuid: source_transaction_uuid.clone(),
                    created_at: *created_at,
                    expires_at: *expires_at,
                    job_label: job_label.clone(),
                };
                pin.validate()?;
                if let Some(existing) = manifest.ongoing_work_pins.iter().find(|p| p.id == pin.id) {
                    if existing.source_version != pin.source_version
                        || existing.source_transaction_uuid != pin.source_transaction_uuid
                    {
                        return Err(Error::invalid_input(
                            "pin ID belongs to a different source snapshot",
                        ));
                    }
                    return Ok(false);
                }
                if manifest.version != transaction.read_version {
                    return Err(Error::commit_conflict_source(
                        manifest.version,
                        "pin registration requires the current source snapshot".into(),
                    ));
                }
                manifest.ongoing_work_pins.push(pin);
                manifest.writer_feature_flags |= crate::feature_flags::FLAG_ONGOING_WORK_PINS;
                Ok(true)
            }
            Self::Renew { pin_id, expires_at } => {
                expires_at.validate()?;
                let pin = manifest
                    .ongoing_work_pins
                    .iter_mut()
                    .find(|p| &p.id == pin_id)
                    .ok_or_else(|| {
                        Error::invalid_input(format!(
                            "ongoing-work pin {pin_id} is no longer active"
                        ))
                    })?;
                if *expires_at <= pin.expires_at {
                    return Ok(false);
                }
                pin.expires_at = *expires_at;
                Ok(true)
            }
            Self::Release { pin_id } => {
                if pin_id.is_empty() {
                    return Err(Error::invalid_input("empty ongoing-work pin ID"));
                }
                let before = manifest.ongoing_work_pins.len();
                manifest.ongoing_work_pins.retain(|p| &p.id != pin_id);
                Ok(before != manifest.ongoing_work_pins.len())
            }
            Self::Expire {
                observed_at,
                candidates,
            } => {
                observed_at.validate()?;
                let mut seen = HashSet::new();
                for (id, expiry) in candidates {
                    expiry.validate()?;
                    if id.is_empty() || !seen.insert(id) || expiry > observed_at {
                        return Err(Error::invalid_input(
                            "invalid ongoing-work pin expiration candidates",
                        ));
                    }
                }
                let before = manifest.ongoing_work_pins.len();
                manifest.ongoing_work_pins.retain(|p| {
                    !candidates
                        .iter()
                        .any(|(id, expiry)| id == &p.id && expiry == &p.expires_at)
                });
                Ok(before != manifest.ongoing_work_pins.len())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::pb;
    use crate::transaction::Operation;
    use std::sync::Arc;

    fn time(seconds: i64) -> PinTime {
        PinTime {
            seconds,
            nanos: 123,
        }
    }
    fn state() -> (Manifest, Transaction) {
        let mut manifest = Manifest::new(
            Default::default(),
            Arc::new(vec![]),
            Default::default(),
            Default::default(),
        );
        let request = Transaction::new(
            1,
            Operation::Pin(PinOperation::Register {
                source_transaction_uuid: "source".into(),
                created_at: time(1),
                expires_at: time(2),
                job_label: None,
            }),
            None,
        );
        if let Operation::Pin(op) = &request.operation {
            op.apply(&request, &mut manifest).unwrap();
        }
        (manifest, request)
    }

    #[test]
    fn ongoing_work_expiration_compare_and_remove() {
        let (mut manifest, request) = state();
        let expiration = PinOperation::Expire {
            observed_at: time(3),
            candidates: vec![(request.uuid.clone(), time(2))],
        };
        PinOperation::Renew {
            pin_id: request.uuid.clone(),
            expires_at: time(4),
        }
        .apply(&request, &mut manifest)
        .unwrap();
        assert!(!expiration.apply(&request, &mut manifest).unwrap());
        assert_eq!(manifest.ongoing_work_pins[0].expires_at, time(4));
        // Expiration winning the ordering prevents a later renewal from recreating it.
        let expiration = PinOperation::Expire {
            observed_at: time(5),
            candidates: vec![(request.uuid.clone(), time(4))],
        };
        assert!(expiration.apply(&request, &mut manifest).unwrap());
        let error = PinOperation::Renew {
            pin_id: request.uuid,
            expires_at: time(6),
        }
        .apply(
            &Transaction::new(1, Operation::ReserveFragments { num_fragments: 0 }, None),
            &mut manifest,
        )
        .unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(error.to_string().contains("no longer active"));
    }

    #[test]
    fn ongoing_work_wire_round_trip_preserves_nanoseconds() {
        let (manifest, request) = state();
        let decoded = Manifest::try_from(pb::Manifest::from(&manifest)).unwrap();
        assert_eq!(decoded.ongoing_work_pins, manifest.ongoing_work_pins);
        let decoded = Transaction::try_from(pb::Transaction::try_from(&request).unwrap()).unwrap();
        assert_eq!(decoded, request);
    }

    #[rstest::rstest]
    #[case::negative_nanos(PinTime { seconds: 1, nanos: -1 })]
    #[case::overflow_nanos(PinTime { seconds: 1, nanos: 1_000_000_000 })]
    #[case::overflow_seconds(PinTime { seconds: 253402300800, nanos: 0 })]
    fn ongoing_work_rejects_invalid_timestamps(#[case] timestamp: PinTime) {
        let error = timestamp.validate().unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(error.to_string().contains("timestamp"));
    }
}
