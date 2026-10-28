// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Retain a build's source and movement history until publication or revocation.
use super::write::CommitBuilder;
use crate::Dataset;
use futures::{FutureExt, future::BoxFuture};
use lance_core::{Error, Result};
use lance_table::format::{Manifest, is_detached_version};
pub use lance_table::format::{OngoingWorkPin, PinTime};
use lance_table::io::manifest::read_manifest;
use lance_table::transaction::{Operation, PinOperation, Transaction};
use std::sync::Arc;

impl Dataset {
    /// Resolve current membership without a version hint or a historical cache hit.
    pub(crate) async fn current_for_ongoing_work(&self) -> Result<Self> {
        if is_detached_version(self.manifest.version) {
            return Err(Error::invalid_input(
                "detached snapshots cannot own ongoing-work pins",
            ));
        }
        let location = self
            .commit_handler
            .resolve_current_location(&self.base, &self.object_store)
            .await?;
        let manifest = read_manifest(&self.object_store, &location.path, location.size).await?;
        if manifest.branch != self.manifest.branch {
            return Err(Error::invalid_input(
                "ongoing-work pin resolved a different branch",
            ));
        }
        manifest.validate_ongoing_work_pins()?;
        Self::checkout_manifest(
            self.object_store.clone(),
            self.base.clone(),
            self.uri.clone(),
            Arc::new(manifest),
            location,
            self.session(),
            self.commit_handler.clone(),
            self.file_reader_options.clone(),
            self.store_params.as_deref().cloned(),
            self.base_store_params.clone(),
        )
    }

    /// Identify this snapshot independently of the version-keyed transaction cache.
    pub(crate) async fn ongoing_work_source_id(&self) -> Result<String> {
        let filename_id = source_id_from_filename(&self.manifest)?;
        let inline_id = if self.manifest.transaction_section.is_some() {
            self.read_transaction_from_storage(&self.manifest, &self.manifest_location)
                .await?
                .map(|t| t.uuid)
        } else {
            None
        };
        match (filename_id, inline_id) {
            (Some(a), Some(b)) if a != b => Err(Error::invalid_input(
                "source transaction identities disagree",
            )),
            (Some(id), _) | (_, Some(id)) if !id.is_empty() => Ok(id),
            _ => Err(Error::invalid_input(
                "snapshot has no identifiable source transaction",
            )),
        }
    }

    /// Prepare a registration request for this exact snapshot. Persist this transaction
    /// before sending it so an ambiguous outcome can be recovered with the same UUID.
    ///
    /// ```
    /// # use lance::{Dataset, Result, dataset::ongoing_work::PinTime};
    /// # async fn example(ds: &Dataset, now: PinTime, expiry: PinTime) -> Result<()> {
    /// let request = ds.prepare_ongoing_work_pin(now, expiry, Some("index build".into())).await?;
    /// // Persist request in the job coordinator before registration.
    /// let pin = ds.register_ongoing_work_pin(request).await?;
    /// ds.release_ongoing_work_pin(&pin.id).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prepare_ongoing_work_pin(
        &self,
        created_at: PinTime,
        expires_at: PinTime,
        job_label: Option<String>,
    ) -> Result<Transaction> {
        let source_transaction_uuid = self.ongoing_work_source_id().await?;
        let request = Transaction::new(
            self.manifest.version,
            Operation::Pin(PinOperation::Register {
                source_transaction_uuid,
                created_at,
                expires_at,
                job_label,
            }),
            None,
        );
        let mut state = self.manifest.as_ref().clone();
        if let Operation::Pin(op) = &request.operation {
            op.apply(&request, &mut state)?;
        }
        Ok(request)
    }

    /// Register or recover the persisted request. Work must not begin until this succeeds.
    /// See [`Self::prepare_ongoing_work_pin`] for an example.
    pub async fn register_ongoing_work_pin(&self, request: Transaction) -> Result<OngoingWorkPin> {
        if !matches!(
            request.operation,
            Operation::Pin(PinOperation::Register { .. })
        ) {
            return Err(Error::invalid_input(
                "expected a pin registration transaction",
            ));
        }
        let branch = self.branch_identifier().await?;
        let id = request.uuid.clone();
        CommitBuilder::new(Arc::new(self.clone()))
            .with_skip_auto_cleanup(true)
            .execute(request)
            .await?;
        let current = self.current_for_ongoing_work().await?;
        if current.branch_identifier().await? != branch {
            return Err(Error::invalid_input(
                "branch changed during pin registration",
            ));
        }
        let pin = current
            .manifest
            .ongoing_work_pins
            .iter()
            .find(|p| p.id == id)
            .ok_or_else(|| Error::invalid_input("registration is no longer active"))?
            .clone();
        current.validate_ongoing_work_source(&pin).await?;
        Ok(pin)
    }

    /// Extend an active registration; a revoked registration cannot be renewed.
    /// See [`Self::prepare_ongoing_work_pin`] for the acquisition lifecycle.
    pub async fn renew_ongoing_work_pin(&self, id: &str, expires_at: PinTime) -> Result<()> {
        self.commit_pin_mutation(PinOperation::Renew {
            pin_id: id.into(),
            expires_at,
        })
        .await
    }
    /// Release only after publishing the result. Missing IDs are successful no-ops.
    /// See [`Self::prepare_ongoing_work_pin`] for an example.
    pub async fn release_ongoing_work_pin(&self, id: &str) -> Result<()> {
        self.commit_pin_mutation(PinOperation::Release { pin_id: id.into() })
            .await
    }
    /// Commit expiration decisions against their exact observed expiry values.
    /// A concurrent renewal invalidates an old decision.
    pub fn expire_ongoing_work_pins(&self, observed_at: PinTime) -> BoxFuture<'_, Result<()>> {
        async move {
            let current = self.current_for_ongoing_work().await?;
            let candidates = current
                .manifest
                .ongoing_work_pins
                .iter()
                .filter(|p| p.expires_at <= observed_at)
                .map(|p| (p.id.clone(), p.expires_at))
                .collect();
            current
                .commit_pin_mutation(PinOperation::Expire {
                    observed_at,
                    candidates,
                })
                .await
        }
        .boxed()
    }
    async fn commit_pin_mutation(&self, operation: PinOperation) -> Result<()> {
        CommitBuilder::new(Arc::new(self.clone()))
            .with_skip_auto_cleanup(true)
            .execute(Transaction::new(
                self.manifest.version,
                Operation::Pin(operation),
                None,
            ))
            .await?;
        Ok(())
    }
    pub(crate) async fn validate_ongoing_work_source(&self, pin: &OngoingWorkPin) -> Result<()> {
        let source = self.checkout_version(pin.source_version).await?;
        if source.ongoing_work_source_id().await? != pin.source_transaction_uuid {
            return Err(Error::invalid_input(format!(
                "source identity changed for ongoing-work pin {}",
                pin.id
            )));
        }
        Ok(())
    }
}

fn source_id_from_filename(manifest: &Manifest) -> Result<Option<String>> {
    let Some(filename) = manifest
        .transaction_file
        .as_deref()
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    let invalid = || Error::invalid_input("invalid source transaction filename");
    if filename.contains('/') || filename.contains('\\') {
        return Err(invalid());
    }
    let (version, id) = filename
        .strip_suffix(".txn")
        .and_then(|s| s.split_once('-'))
        .ok_or_else(invalid)?;
    if version.is_empty()
        || !version.bytes().all(|b| b.is_ascii_digit())
        || version.parse::<u64>().is_err()
        || id.is_empty()
    {
        return Err(invalid());
    }
    Ok(Some(id.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::index::frag_reuse::cleanup_frag_reuse_index;
    use crate::dataset::optimize::{CompactionOptions, compact_files};
    use crate::index::DatasetIndexExt;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};
    use arrow_array::types::Int32Type;
    use lance_index::{
        IndexType,
        scalar::{FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams},
    };

    fn time(seconds: i64) -> PinTime {
        PinTime { seconds, nanos: 0 }
    }
    async fn dataset() -> Dataset {
        lance_datagen::gen_batch()
            .col("value", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(4), FragmentRowCount::from(4))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn ongoing_work_registration_recovery_and_revocation() {
        let ds = dataset().await;
        let request = ds
            .prepare_ongoing_work_pin(time(1), time(10), Some("build".into()))
            .await
            .unwrap();
        let pin = ds.register_ongoing_work_pin(request.clone()).await.unwrap();
        ds.renew_ongoing_work_pin(&pin.id, time(20)).await.unwrap();
        let recovered = ds.register_ongoing_work_pin(request).await.unwrap();
        assert_eq!(recovered.expires_at, time(20));
        assert_eq!(recovered.source_version, ds.version_id());
        ds.expire_ongoing_work_pins(time(10)).await.unwrap();
        assert_eq!(
            ds.current_for_ongoing_work()
                .await
                .unwrap()
                .manifest
                .ongoing_work_pins
                .len(),
            1
        );
        ds.expire_ongoing_work_pins(time(20)).await.unwrap();
        let current = ds.current_for_ongoing_work().await.unwrap();
        assert!(current.manifest.ongoing_work_pins.is_empty());
        assert_ne!(
            current.manifest.writer_feature_flags
                & lance_table::feature_flags::FLAG_ONGOING_WORK_PINS,
            0
        );
        let error = ds
            .renew_ongoing_work_pin(&pin.id, time(30))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(error.to_string().contains("no longer active"));
        let version = current.version_id();
        ds.release_ongoing_work_pin(&pin.id).await.unwrap();
        assert_eq!(
            ds.current_for_ongoing_work().await.unwrap().version_id(),
            version
        );
    }

    #[tokio::test]
    async fn ongoing_work_restore_preserves_current_membership() {
        let mut source = dataset().await;
        let request = source
            .prepare_ongoing_work_pin(time(1), time(10), None)
            .await
            .unwrap();
        let pin = source.register_ongoing_work_pin(request).await.unwrap();
        source.restore().await.unwrap();
        let current = source.current_for_ongoing_work().await.unwrap();
        assert_eq!(current.manifest.ongoing_work_pins, vec![pin.clone()]);
        current.manifest.validate_ongoing_work_pins().unwrap();
        let mut pinned_snapshot = current.clone();
        current.release_ongoing_work_pin(&pin.id).await.unwrap();
        pinned_snapshot.restore().await.unwrap();
        let restored = source.current_for_ongoing_work().await.unwrap();
        assert!(restored.manifest.ongoing_work_pins.is_empty());
        assert_ne!(
            restored.manifest.writer_feature_flags
                & lance_table::feature_flags::FLAG_ONGOING_WORK_PINS,
            0
        );
    }

    #[tokio::test]
    async fn ongoing_work_registration_cannot_rebase_to_another_source() {
        let ds = dataset().await;
        let request = ds
            .prepare_ongoing_work_pin(time(1), time(10), None)
            .await
            .unwrap();
        let first = ds
            .prepare_ongoing_work_pin(time(1), time(10), None)
            .await
            .unwrap();
        ds.register_ongoing_work_pin(first).await.unwrap();
        let error = ds.register_ongoing_work_pin(request).await.unwrap_err();
        assert!(matches!(error, Error::CommitConflict { .. }));
        assert!(error.to_string().contains("source is no longer current"));
    }

    #[rstest::rstest]
    #[case::no_committed_index(false)]
    #[case::disjoint_committed_index(true)]
    #[tokio::test]
    async fn ongoing_work_compaction_cleanup_publish(
        #[case] old_index: bool,
        #[values(false, true)] tagged: bool,
    ) {
        let mut ds = dataset().await;
        if tagged {
            let manifest = Arc::make_mut(&mut ds.manifest);
            manifest.reader_feature_flags |= lance_table::feature_flags::FLAG_FRAGMENT_REUSE_INDEX;
            manifest.writer_feature_flags |= lance_table::feature_flags::FLAG_FRAGMENT_REUSE_INDEX;
            crate::index::frag_reuse_reader::tests::persist_fixture(&mut ds, vec![]).await;
            ds.apply_commit(
                Transaction::new(
                    ds.version_id(),
                    Operation::ReserveFragments { num_fragments: 0 },
                    None,
                ),
                &Default::default(),
                &Default::default(),
            )
            .await
            .unwrap();
        }
        let params = ScalarIndexParams::default();
        if old_index {
            // Keep an old index whose bitmap is disjoint from the rewrite below.
            let old = ds
                .create_index_builder(&["value"], IndexType::Scalar, &params)
                .name("value_idx".into())
                .fragments(vec![0])
                .execute_uncommitted()
                .await
                .unwrap();
            ds.commit_existing_index_segments("value_idx", "value", vec![old])
                .await
                .unwrap();
        }
        let request = ds
            .prepare_ongoing_work_pin(time(1), time(253402300000), None)
            .await
            .unwrap();
        let pin = ds.register_ongoing_work_pin(request).await.unwrap();
        let staged = ds
            .create_index_builder(&["value"], IndexType::Scalar, &params)
            .name("value_idx".into())
            .replace(true)
            .execute_uncommitted()
            .await
            .unwrap();
        let mut current = ds.current_for_ongoing_work().await.unwrap();
        compact_files(
            &mut current,
            CompactionOptions {
                target_rows_per_fragment: 8,
                defer_index_remap: true,
                excluded_fragment_ids: if old_index { vec![0] } else { vec![] },
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        compact_files(
            &mut current,
            CompactionOptions {
                target_rows_per_fragment: 100,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        let before = current.version_id();
        assert!(
            crate::index::load_all_indices(&current)
                .await
                .unwrap()
                .iter()
                .any(|index| index.name == lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME)
        );
        cleanup_frag_reuse_index(&mut current).await.unwrap();
        assert_eq!(current.version_id(), before);
        // Exercise physical cleanup while the source and full replay suffix are old.
        current
            .cleanup_old_versions(chrono::Duration::seconds(-1), Some(false), Some(false))
            .await
            .unwrap();
        assert!(current.checkout_version(pin.source_version).await.is_ok());
        ds.commit_existing_index_segments_with_pin("value_idx", "value", vec![staged], &pin.id)
            .await
            .unwrap();
        ds.release_ongoing_work_pin(&pin.id).await.unwrap();
        assert_eq!(ds.count_rows(Some("value = 3".into())).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn ongoing_work_revocation_fences_staged_publication() {
        let mut ds = dataset().await;
        let request = ds
            .prepare_ongoing_work_pin(time(1), time(10), None)
            .await
            .unwrap();
        let pin = ds.register_ongoing_work_pin(request).await.unwrap();
        let params = ScalarIndexParams::default();
        let staged = ds
            .create_index_builder(&["value"], IndexType::Scalar, &params)
            .name("value_idx".into())
            .execute_uncommitted()
            .await
            .unwrap();
        ds.release_ongoing_work_pin(&pin.id).await.unwrap();
        let error = ds
            .commit_existing_index_segments_with_pin("value_idx", "value", vec![staged], &pin.id)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(error.to_string().contains("no longer active"));
    }
    #[tokio::test]
    async fn ongoing_work_fts_rebuild_survives_cleanup() {
        let mut ds = lance_datagen::gen_batch()
            .col(
                "text",
                lance_datagen::array::cycle_utf8_literals(&["alpha", "beta"]),
            )
            .into_ram_dataset(FragmentCount::from(4), FragmentRowCount::from(4))
            .await
            .unwrap();
        let params = InvertedIndexParams::default();
        let old = ds
            .create_index_builder(&["text"], IndexType::Inverted, &params)
            .name("text_idx".into())
            .fragments(vec![0])
            .execute_uncommitted()
            .await
            .unwrap();
        ds.commit_existing_index_segments("text_idx", "text", vec![old])
            .await
            .unwrap();
        let request = ds
            .prepare_ongoing_work_pin(time(1), time(253402300000), None)
            .await
            .unwrap();
        let pin = ds.register_ongoing_work_pin(request).await.unwrap();
        let staged = ds
            .create_index_builder(&["text"], IndexType::Inverted, &params)
            .name("text_idx".into())
            .replace(true)
            .execute_uncommitted()
            .await
            .unwrap();
        let mut current = ds.current_for_ongoing_work().await.unwrap();
        compact_files(
            &mut current,
            CompactionOptions {
                target_rows_per_fragment: 100,
                defer_index_remap: true,
                excluded_fragment_ids: vec![0],
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        cleanup_frag_reuse_index(&mut current).await.unwrap();
        ds.commit_existing_index_segments_with_pin("text_idx", "text", vec![staged], &pin.id)
            .await
            .unwrap();
        ds.release_ongoing_work_pin(&pin.id).await.unwrap();
        let hits = ds
            .scan()
            .full_text_search(FullTextSearchQuery::new("alpha".into()))
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(hits.num_rows(), 8);
    }
    #[tokio::test]
    async fn ongoing_work_rejects_rewrite_planned_without_recording() {
        let ds = dataset().await;
        let old = ds.fragments()[0].clone();
        let mut new = old.clone();
        new.id = 4;
        let rewrite = Transaction::new(
            ds.version_id(),
            Operation::Rewrite {
                groups: vec![lance_table::transaction::RewriteGroup {
                    old_fragments: vec![old],
                    new_fragments: vec![new],
                }],
                rewritten_indices: vec![],
                frag_reuse_index: None,
            },
            None,
        );
        let request = ds
            .prepare_ongoing_work_pin(time(1), time(253402300000), None)
            .await
            .unwrap();
        ds.register_ongoing_work_pin(request).await.unwrap();
        let error = CommitBuilder::new(Arc::new(ds))
            .execute(rewrite)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::RetryableCommitConflict { .. }));
        assert!(
            error
                .to_string()
                .contains("requires fragment reuse recording")
        );
    }

    #[tokio::test]
    async fn ongoing_work_rejects_trim_planned_before_registration() {
        let ds = dataset().await;
        let request = ds
            .prepare_ongoing_work_pin(time(1), time(253402300000), None)
            .await
            .unwrap();
        let pin = ds.register_ongoing_work_pin(request).await.unwrap();
        let mut current = ds.current_for_ongoing_work().await.unwrap();
        compact_files(
            &mut current,
            CompactionOptions {
                target_rows_per_fragment: 100,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        current.release_ongoing_work_pin(&pin.id).await.unwrap();
        let current = current.current_for_ongoing_work().await.unwrap();
        let indices = crate::index::load_all_indices(&current).await.unwrap();
        let fri = indices
            .iter()
            .find(|i| i.name == lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME)
            .unwrap()
            .clone();
        let trim = Transaction::new(
            current.version_id(),
            Operation::CreateIndex {
                new_indices: vec![],
                removed_indices: vec![fri],
            },
            None,
        );
        let request = current
            .prepare_ongoing_work_pin(time(1), time(253402300000), None)
            .await
            .unwrap();
        current.register_ongoing_work_pin(request).await.unwrap();
        let error = CommitBuilder::new(Arc::new(current))
            .execute(trim)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::RetryableCommitConflict { .. }));
        assert!(
            error
                .to_string()
                .contains("prevents fragment reuse history replacement")
        );
    }
}
