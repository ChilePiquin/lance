// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Durable retention requirements for unfinished work.

use super::{Manifest, pb};
use lance_core::{Error, Result, deepsize::DeepSizeOf};
use std::collections::HashSet;

/// A validated protobuf UTC timestamp, compared without losing nanosecond precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, DeepSizeOf)]
pub struct PinTime {
    /// Seconds since the Unix epoch, within the protobuf timestamp range.
    pub seconds: i64,
    /// Nanoseconds within the second, in `0..1_000_000_000`.
    pub nanos: i32,
}
impl PinTime {
    /// Validate the protobuf timestamp range.
    pub fn validate(self) -> Result<()> {
        if !(-62135596800..=253402300799).contains(&self.seconds)
            || !(0..1_000_000_000).contains(&self.nanos)
        {
            return Err(Error::invalid_input("invalid ongoing-work pin timestamp"));
        }
        Ok(())
    }
    /// Convert a required wire timestamp, rejecting missing or invalid values.
    pub fn from_proto(value: Option<prost_types::Timestamp>) -> Result<Self> {
        let value =
            value.ok_or_else(|| Error::invalid_input("missing ongoing-work pin timestamp"))?;
        let value = Self {
            seconds: value.seconds,
            nanos: value.nanos,
        };
        value.validate()?;
        Ok(value)
    }
    /// Convert to the wire representation without truncation.
    pub fn to_proto(self) -> prost_types::Timestamp {
        prost_types::Timestamp {
            seconds: self.seconds,
            nanos: self.nanos,
        }
    }
}

/// A current-manifest retention consumer. Expiry only permits committed revocation.
#[derive(Debug, Clone, PartialEq, Eq, DeepSizeOf)]
pub struct OngoingWorkPin {
    /// Registration transaction UUID. Retries preserve this ID.
    pub id: String,
    /// Exact snapshot used by the job.
    pub source_version: u64,
    /// Transaction UUID that published the source snapshot.
    pub source_transaction_uuid: String,
    /// Fixed registration request time.
    pub created_at: PinTime,
    /// Earliest time cleanup may commit expiration.
    pub expires_at: PinTime,
    /// Optional immutable diagnostic context.
    pub job_label: Option<String>,
}
impl OngoingWorkPin {
    /// Validate a complete retention record.
    pub fn validate(&self) -> Result<()> {
        self.created_at.validate()?;
        self.expires_at.validate()?;
        if self.id.is_empty()
            || self.source_version == 0
            || self.source_transaction_uuid.is_empty()
            || self.expires_at <= self.created_at
        {
            return Err(Error::invalid_input(
                "incomplete or invalid ongoing-work pin",
            ));
        }
        Ok(())
    }
}
impl TryFrom<pb::OngoingWorkPin> for OngoingWorkPin {
    type Error = Error;
    fn try_from(p: pb::OngoingWorkPin) -> Result<Self> {
        let pin = Self {
            id: p.id.unwrap_or_default(),
            source_version: p.source_version.unwrap_or_default(),
            source_transaction_uuid: p.source_transaction_uuid.unwrap_or_default(),
            created_at: PinTime::from_proto(p.created_at)?,
            expires_at: PinTime::from_proto(p.expires_at)?,
            job_label: p.job_label,
        };
        pin.validate()?;
        Ok(pin)
    }
}
impl From<&OngoingWorkPin> for pb::OngoingWorkPin {
    fn from(p: &OngoingWorkPin) -> Self {
        Self {
            id: Some(p.id.clone()),
            source_version: Some(p.source_version),
            source_transaction_uuid: Some(p.source_transaction_uuid.clone()),
            created_at: Some(p.created_at.to_proto()),
            expires_at: Some(p.expires_at.to_proto()),
            job_label: p.job_label.clone(),
        }
    }
}
impl Manifest {
    /// Validate authoritative membership before writes or destructive maintenance.
    pub fn validate_ongoing_work_pins(&self) -> Result<()> {
        if !self.ongoing_work_pins.is_empty()
            && self.writer_feature_flags & crate::feature_flags::FLAG_ONGOING_WORK_PINS == 0
        {
            return Err(Error::invalid_input(
                "ongoing-work pins require their writer capability",
            ));
        }
        let mut ids = HashSet::new();
        for pin in &self.ongoing_work_pins {
            pin.validate()?;
            if !ids.insert(&pin.id) {
                return Err(Error::invalid_input(format!(
                    "duplicate ongoing-work pin {}",
                    pin.id
                )));
            }
        }
        Ok(())
    }
    /// Inclusive history horizon; wall-clock expiry does not remove protection.
    pub fn ongoing_work_horizon(&self) -> Option<u64> {
        self.ongoing_work_pins
            .iter()
            .map(|p| p.source_version)
            .min()
    }
}
