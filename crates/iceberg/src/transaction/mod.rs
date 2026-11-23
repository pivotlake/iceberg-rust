// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This module contains transaction api.
//!
//! The transaction API enables changes to be made to an existing table.
//!
//! Note that this may also have side effects, such as producing new manifest
//! files.
//!
//! Below is a basic example using the "fast-append" action:
//!
//! ```ignore
//! use iceberg::transaction::{ApplyTransactionAction, Transaction};
//! use iceberg::Catalog;
//!
//! // Create a transaction.
//! let tx = Transaction::new(my_table);
//!
//! // Create a `FastAppendAction` which will not rewrite or append
//! // to existing metadata. This will create a new manifest.
//! let action = tx.fast_append().add_data_files(my_data_files);
//!
//! // Apply the fast-append action to the given transaction, returning
//! // the newly updated `Transaction`.
//! let tx = action.apply(tx).unwrap();
//!
//!
//! // End the transaction by committing to an `iceberg::Catalog`
//! // implementation. This will cause a table update to occur.
//! let table = tx
//!     .commit(&some_catalog_impl)
//!     .await
//!     .unwrap();
//! ```

/// The `ApplyTransactionAction` trait provides an `apply` method
/// that allows users to apply a transaction action to a `Transaction`.
mod action;

pub use action::*;
mod append;
mod expire_snapshots;
mod manifest_merge;
mod merging;
mod rewrite;
mod snapshot;
mod sort_order;
mod update_location;
mod update_properties;
mod update_schema;
mod update_statistics;
mod upgrade_format_version;
mod written_files;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder, RetryableWithContext};
pub use update_schema::AddColumn;

use crate::error::Result;
use crate::spec::TableProperties;
use crate::table::Table;
use crate::transaction::action::BoxedTransactionAction;
pub use crate::transaction::append::FastAppendAction;
pub use crate::transaction::expire_snapshots::ExpireSnapshotsAction;
pub use crate::transaction::rewrite::{PreparedRewrite, RewriteFilesAction};
pub use crate::transaction::sort_order::ReplaceSortOrderAction;
pub use crate::transaction::update_location::UpdateLocationAction;
pub use crate::transaction::update_properties::UpdatePropertiesAction;
pub use crate::transaction::update_schema::UpdateSchemaAction;
pub use crate::transaction::update_statistics::UpdateStatisticsAction;
pub use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::transaction::written_files::{
    WrittenFiles, collect_unreferenced_paths, collect_written_paths,
};
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
#[derive(Clone)]
pub struct Transaction {
    table: Table,
    actions: Vec<BoxedTransactionAction>,
    /// The files written by commit attempts whose outcome is unknown. They
    /// may be live, so no later attempt deletes them.
    unknown_outcome_files: HashSet<String>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            actions: vec![],
            unknown_outcome_files: HashSet::new(),
        }
    }

    fn update_table_metadata(table: Table, updates: &[TableUpdate]) -> Result<Table> {
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        Ok(table.with_metadata(Arc::new(metadata_builder.build()?.metadata)))
    }

    /// Applies an [`ActionCommit`] to the given [`Table`], returning a new [`Table`] with updated metadata.
    /// Also appends any derived [`TableUpdate`]s and [`TableRequirement`]s to the provided vectors.
    fn apply(
        table: Table,
        mut action_commit: ActionCommit,
        existing_updates: &mut Vec<TableUpdate>,
        existing_requirements: &mut Vec<TableRequirement>,
    ) -> Result<Table> {
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        for requirement in &requirements {
            requirement.check(Some(table.metadata()))?;
        }

        let updated_table = Self::update_table_metadata(table, &updates)?;

        existing_updates.extend(updates);
        existing_requirements.extend(requirements);

        Ok(updated_table)
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_table_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
    }

    /// Creates an update schema action.
    pub fn update_schema(&self) -> UpdateSchemaAction {
        UpdateSchemaAction::new()
    }

    /// Creates a fast append action.
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    /// Creates a rewrite files action for compaction.
    ///
    /// This action replaces a set of data files with a new set while keeping
    /// the logical table contents unchanged. The resulting snapshot uses
    /// [`Operation::Replace`](crate::spec::Operation::Replace).
    pub fn rewrite_files(&self) -> RewriteFilesAction {
        RewriteFilesAction::new(self.table.metadata().current_snapshot_id())
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(&self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction::new()
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Update the statistics of table
    pub fn update_statistics(&self) -> UpdateStatisticsAction {
        UpdateStatisticsAction::new()
    }

    /// Expire snapshots from the table metadata.
    pub fn expire_snapshots(&self) -> ExpireSnapshotsAction {
        ExpireSnapshotsAction::new()
    }

    /// Commit transaction.
    ///
    /// Every attempt writes its manifests and manifest lists before it asks
    /// the catalog to commit. What becomes of them depends on how the attempt
    /// ends:
    ///
    /// - It fails before the catalog is asked, or the catalog rejects it with
    ///   [`ErrorKind::CatalogCommitConflicts`], whether or not it is then
    ///   retried: every manifest and manifest list it wrote is deleted.
    /// - [`Catalog::update_table`] fails with any other error: the commit may
    ///   have been applied, so every file the attempt wrote is kept.
    /// - It commits: the manifests it wrote that none of the manifest lists it
    ///   wrote names are deleted. Those are the manifests an action wrote and
    ///   then merged into another. Its manifest lists are kept, and so is
    ///   every manifest named by the manifest list of any of its actions. An
    ///   action that wrote manifests and no manifest list keeps them all.
    ///
    /// A file the attempt did not write is never deleted, so a manifest of an
    /// earlier snapshot that this commit merged into another stays until the
    /// snapshots naming it expire. A file that cannot be deleted is logged and
    /// changes neither the table returned nor the error.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.actions.is_empty() {
            // nothing to commit
            return Ok(self.table);
        }

        let table_props = self.table.metadata().table_properties();

        let backoff = Self::build_backoff(table_props)?;
        let tx = self;

        (|mut tx: Transaction| async {
            let result = tx.do_commit(catalog).await;
            (tx, result)
        })
        .retry(backoff)
        .sleep(tokio::time::sleep)
        .context(tx)
        .when(|e| e.retryable())
        .await
        .1
    }

    fn build_backoff(props: TableProperties<'_>) -> Result<ExponentialBackoff> {
        Ok(ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(props.commit_min_retry_wait_ms()?))
            .with_max_delay(Duration::from_millis(props.commit_max_retry_wait_ms()?))
            .with_total_delay(Some(Duration::from_millis(
                props.commit_total_retry_timeout_ms()?,
            )))
            .with_max_times(props.commit_num_retries()?)
            .with_factor(2.0)
            .build())
    }

    async fn do_commit(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        let refreshed = catalog.load_table(self.table.identifier()).await?;

        if self.table.metadata() != refreshed.metadata()
            || self.table.metadata_location() != refreshed.metadata_location()
        {
            // current base is stale, use refreshed as base and re-apply transaction actions
            self.table = refreshed.clone();
        }

        let written_files: Vec<WrittenFiles> = self
            .actions
            .iter()
            .map(|_| WrittenFiles::default())
            .collect();
        let table_commit = match self.build_table_commit(&written_files).await {
            Ok(table_commit) => table_commit,
            Err(error) => {
                // The catalog was never asked, so nothing can reference the
                // files written so far.
                self.delete_unreferenced_files(collect_written_paths(&written_files))
                    .await;
                return Err(error);
            }
        };

        match catalog.update_table(table_commit).await {
            Ok(table) => {
                self.delete_unreferenced_files(collect_unreferenced_paths(&written_files))
                    .await;
                Ok(table)
            }
            Err(error) if is_commit_rejected(&error) => {
                self.delete_unreferenced_files(collect_written_paths(&written_files))
                    .await;
                Err(error)
            }
            Err(error) => {
                self.unknown_outcome_files
                    .extend(collect_written_paths(&written_files));
                Err(error)
            }
        }
    }

    /// Runs the actions on top of the transaction's table and returns the
    /// commit that carries their updates and requirements. The manifests and
    /// manifest lists each action writes are recorded in its entry of
    /// `written_files`, which has one per action.
    async fn build_table_commit(&self, written_files: &[WrittenFiles]) -> Result<TableCommit> {
        let mut current_table = self.table.clone();
        let mut existing_updates: Vec<TableUpdate> = vec![];
        let mut existing_requirements: Vec<TableRequirement> = vec![];

        for (action, action_files) in self.actions.iter().zip(written_files) {
            let action_commit = Arc::clone(action)
                .commit_recording_files(&current_table, action_files)
                .await?;
            // apply action commit to current_table
            current_table = Self::apply(
                current_table,
                action_commit,
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

        Ok(TableCommit::builder()
            .ident(self.table.identifier().to_owned())
            .updates(existing_updates)
            .requirements(existing_requirements)
            .build())
    }

    /// Deletes the files at `paths`, which an attempt wrote and nothing
    /// references: all the files of an attempt that did not commit, or the
    /// manifests an attempt that committed merged into others.
    ///
    /// A path an earlier attempt of unknown outcome also wrote is kept: that
    /// attempt may have committed, and the file may be live.
    async fn delete_unreferenced_files(&self, paths: Vec<String>) {
        let file_io = self.table.file_io();
        for path in paths {
            if self.unknown_outcome_files.contains(&path) {
                continue;
            }
            // A file that cannot be deleted stays in storage, referenced by
            // nothing; the commit ends as it would have without it.
            if let Err(error) = file_io.delete(&path).await {
                tracing::warn!(
                    "Failed to delete {path}, written by a commit to table {} and referenced by nothing: {error}",
                    self.table.identifier()
                );
            }
        }
    }
}

/// Whether `error`, returned by [`Catalog::update_table`], says the catalog
/// rejected the commit, so that the table is known to be unchanged.
///
/// Only [`ErrorKind::CatalogCommitConflicts`] says so. Any other error leaves
/// the outcome unknown: the catalog may have applied the commit and failed
/// afterwards, as when a response is lost or a server times out, which is what
/// Java reports with `CommitStateUnknownException`.
fn is_commit_rejected(error: &Error) -> bool {
    error.kind() == ErrorKind::CatalogCommitConflicts
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::catalog::MockCatalog;
    use crate::io::FileIO;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestEntryRef,
        ManifestFile, Struct, TableMetadata, TableProperties,
    };
    use crate::table::Table;
    use crate::test_utils::{make_encrypted_table, test_runtime};
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, Error, ErrorKind, TableCreation, TableIdent};

    /// Create a test data file with the given path, record count, and file size.
    pub(crate) fn make_data_file(table: &Table, path: &str, records: u64, size: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(records)
            .partition(Struct::from_iter([Some(Literal::long(1))]))
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .build()
            .unwrap()
    }

    /// Append data files to a table via fast_append and return the updated table.
    pub(crate) async fn append_files(
        catalog: &impl Catalog,
        table: &Table,
        files: Vec<DataFile>,
    ) -> Table {
        let tx = Transaction::new(table);
        let tx = tx.fast_append().add_data_files(files).apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Append data files to a table via a fast_append that merges manifests
    /// and return the updated table.
    pub(crate) async fn append_files_merging_manifests(
        catalog: &impl Catalog,
        table: &Table,
        files: Vec<DataFile>,
    ) -> Table {
        let tx = Transaction::new(table);
        let action = tx
            .fast_append()
            .with_manifest_merge(true)
            .add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Set table properties and return the updated table.
    pub(crate) async fn set_table_properties(
        catalog: &impl Catalog,
        table: &Table,
        properties: &[(&str, &str)],
    ) -> Table {
        let tx = Transaction::new(table);
        let mut action = tx.update_table_properties();
        for (key, value) in properties {
            action = action.set(key.to_string(), value.to_string());
        }
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// The manifests the table's current snapshot lists, in order.
    pub(crate) async fn load_current_manifests(table: &Table) -> Vec<ManifestFile> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        manifest_list.entries().to_vec()
    }

    /// Every entry of the manifests the table's current snapshot lists.
    pub(crate) async fn load_current_entries(table: &Table) -> Vec<ManifestEntryRef> {
        let mut entries = Vec::new();
        for manifest_file in load_current_manifests(table).await {
            let manifest = table.manifest_reader().read(&manifest_file).await.unwrap();
            entries.extend(manifest.entries().iter().cloned());
        }
        entries
    }

    /// The paths of the live files among `entries`, sorted.
    pub(crate) fn collect_live_paths(entries: &[ManifestEntryRef]) -> Vec<&str> {
        let mut paths: Vec<&str> = entries
            .iter()
            .filter(|entry| entry.is_alive())
            .map(|entry| entry.file_path())
            .collect();
        paths.sort();
        paths
    }

    /// The entry for `path` among `entries`.
    pub(crate) fn find_entry_by_path<'a>(
        entries: &'a [ManifestEntryRef],
        path: &str,
    ) -> &'a ManifestEntryRef {
        entries
            .iter()
            .find(|entry| entry.file_path() == path)
            .unwrap()
    }

    pub fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    pub(crate) async fn make_v1_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        make_minimal_table_in_catalog(
            catalog,
            "TableMetadataV1Valid.json",
            crate::spec::FormatVersion::V1,
        )
        .await
    }

    pub(crate) async fn make_v2_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        make_minimal_table_in_catalog(
            catalog,
            "TableMetadataV2ValidMinimal.json",
            crate::spec::FormatVersion::V2,
        )
        .await
    }

    pub(crate) async fn make_v3_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        make_minimal_table_in_catalog(
            catalog,
            "TableMetadataV3ValidMinimal.json",
            crate::spec::FormatVersion::V3,
        )
        .await
    }

    async fn make_minimal_table_in_catalog(
        catalog: &impl Catalog,
        metadata_file: &str,
        format_version: crate::spec::FormatVersion,
    ) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", uuid::Uuid::new_v4()), "test1".to_string()])
                .unwrap();

        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let base_metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(format_version)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Helper function to create a test table with retry properties
    pub(super) fn setup_test_table(num_retries: &str) -> Table {
        let table = make_v2_table();

        // Set retry properties
        let mut props = HashMap::new();
        props.insert("commit.retry.min-wait-ms".to_string(), "10".to_string());
        props.insert("commit.retry.max-wait-ms".to_string(), "100".to_string());
        props.insert(
            "commit.retry.total-timeout-ms".to_string(),
            "1000".to_string(),
        );
        props.insert(
            "commit.retry.num-retries".to_string(),
            num_retries.to_string(),
        );

        // Update table properties
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(props)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        table.with_metadata(Arc::new(metadata))
    }

    /// Helper function to create a transaction with a simple update action
    fn create_test_transaction(table: &Table) -> Transaction {
        let tx = Transaction::new(table);
        tx.update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .unwrap()
    }

    /// Helper function to set up a mock catalog with retryable errors
    fn setup_mock_catalog_with_retryable_errors(
        success_after_attempts: Option<u32>,
        expected_calls: usize,
    ) -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        let attempts = AtomicU32::new(0);
        mock_catalog
            .expect_update_table()
            .times(expected_calls)
            .returning_st(move |_| {
                if let Some(success_after_attempts) = success_after_attempts {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if attempts.load(Ordering::SeqCst) <= success_after_attempts {
                        Box::pin(async move {
                            Err(
                                Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                    .with_retryable(true),
                            )
                        })
                    } else {
                        Box::pin(async move { Ok(make_v2_table()) })
                    }
                } else {
                    // Always fail with retryable error
                    Box::pin(async move {
                        Err(
                            Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                .with_retryable(true),
                        )
                    })
                }
            });

        mock_catalog
    }

    /// Helper function to set up a mock catalog with non-retryable error
    fn setup_mock_catalog_with_non_retryable_error() -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        mock_catalog
            .expect_update_table()
            .times(1) // Should only be called once since error is not retryable
            .returning_st(move |_| {
                Box::pin(async move {
                    Err(Error::new(ErrorKind::Unexpected, "Non-retryable error")
                        .with_retryable(false))
                })
            });

        mock_catalog
    }

    #[tokio::test]
    async fn test_commit_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails twice then succeeds
        let mock_catalog = setup_mock_catalog_with_retryable_errors(Some(2), 3);

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_ok(), "Transaction should eventually succeed");
    }

    #[tokio::test]
    async fn test_commit_non_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails with non-retryable error
        let mock_catalog = setup_mock_catalog_with_non_retryable_error();

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail immediately");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "Non-retryable error");
            assert!(!err.retryable(), "Error should not be retryable");
        }
    }

    #[tokio::test]
    async fn test_commit_max_retries_exceeded() {
        // Create a test table with retry properties (only allow 2 retries)
        let table = setup_test_table("2");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that always fails with retryable error
        let mock_catalog = setup_mock_catalog_with_retryable_errors(None, 3); // Initial attempt + 2 retries = 3 total attempts

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail after max retries");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
            assert_eq!(err.message(), "Commit conflict");
            assert!(err.retryable(), "Error should be retryable");
        }
    }

    #[tokio::test]
    async fn test_transaction_snapshot_summary() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let mut file_seq = 0u32;
        let mut append_file = |table: &Table, record_count: u64, file_size: u64| {
            file_seq += 1;
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{file_seq}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(file_size)
                .record_count(record_count)
                .partition(Struct::from_iter([Some(Literal::long(1))]))
                .partition_spec_id(0)
                .build()
                .unwrap();
            let tx = Transaction::new(table);
            tx.fast_append()
                .add_data_files(vec![file])
                .apply(tx)
                .unwrap()
        };

        let table = append_file(&table, /*record_count=*/ 10, /*file_size=*/ 100)
            .commit(&catalog)
            .await
            .unwrap();
        let table = append_file(&table, /*record_count=*/ 20, /*file_size=*/ 200)
            .commit(&catalog)
            .await
            .unwrap();

        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;

        assert_eq!(summary.get("total-records").unwrap(), "30");
        assert_eq!(summary.get("total-data-files").unwrap(), "2");
        assert_eq!(summary.get("total-files-size").unwrap(), "300");
    }

    #[tokio::test]
    async fn test_commit_to_encrypted_table() {
        let table = make_encrypted_table().await.with_metadata_location(
            "memory:///table/metadata/00000-9c12d441-03fe-4693-9a96-a0705ddf69c1.metadata.json"
                .to_string(),
        );
        let refreshed_table = table.clone();
        let update_table = table.clone();
        let mut mock_catalog = MockCatalog::new();
        mock_catalog
            .expect_load_table()
            .times(1)
            .returning_st(move |_| {
                let refreshed_table = refreshed_table.clone();
                Box::pin(async move { Ok(refreshed_table) })
            });
        mock_catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |commit| {
                let update_table = update_table.clone();
                Box::pin(async move { commit.apply(update_table) })
            });

        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .unwrap();

        let updated_table = tx.commit(&mock_catalog).await.unwrap();

        assert_eq!(
            updated_table
                .metadata()
                .properties()
                .get(TableProperties::PROPERTY_ENCRYPTION_KEY_ID)
                .map(String::as_str),
            Some("master-1")
        );
        assert_eq!(
            updated_table
                .metadata()
                .properties()
                .get("test.key")
                .map(String::as_str),
            Some("test.value")
        );
        assert!(updated_table.encryption_manager().is_some());
    }
}

#[cfg(test)]
mod test_row_lineage {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[tokio::test]
    async fn test_fast_append_with_row_lineage() {
        // Helper function to create a data file with specified number of rows
        fn file_with_rows(record_count: u64) -> DataFile {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{record_count}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(record_count)
                .partition(Struct::from_iter([Some(Literal::long(0))]))
                .partition_spec_id(0)
                .build()
                .unwrap()
        }
        let catalog = new_memory_catalog().await;

        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Check initial state - next_row_id should be 0
        assert_eq!(table.metadata().next_row_id(), 0);

        // First fast append with 30 rows
        let tx = Transaction::new(&table);
        let data_file_30 = file_with_rows(30);
        let action = tx.fast_append().add_data_files(vec![data_file_30]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after first append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(0));
        assert_eq!(table.metadata().next_row_id(), 30);

        // Check written manifest for first_row_id
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        assert_eq!(manifest_list.entries().len(), 1);
        let manifest_file = &manifest_list.entries()[0];
        assert_eq!(manifest_file.first_row_id, Some(0));

        // Second fast append with 17 and 11 rows
        let tx = Transaction::new(&table);
        let data_file_17 = file_with_rows(17);
        let data_file_11 = file_with_rows(11);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_17, data_file_11]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after second append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(30));
        assert_eq!(table.metadata().next_row_id(), 30 + 17 + 11);

        // Check written manifest for first_row_id
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        assert_eq!(manifest_list.entries().len(), 2);
        let manifest_file = &manifest_list.entries()[1];
        assert_eq!(manifest_file.first_row_id, Some(30));
    }
}

#[cfg(test)]
mod test_uncommitted_files {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use uuid::Uuid;

    use crate::catalog::MockCatalog;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{Snapshot, TableProperties};
    use crate::table::Table;
    use crate::transaction::tests::{
        append_files, collect_live_paths, load_current_entries, load_current_manifests,
        make_data_file, make_v2_minimal_table_in_catalog, set_table_properties,
    };
    use crate::transaction::written_files::WrittenFiles;
    use crate::transaction::{
        ActionCommit, ApplyTransactionAction, Transaction, TransactionAction,
    };
    use crate::{Catalog, Error, ErrorKind, Result, TableCommit, TableUpdate};

    /// The files each `update_table` call found written by the attempt that
    /// made it, in call order.
    type AttemptFiles = Arc<Mutex<Vec<Vec<String>>>>;

    /// A V2 table in `catalog` holding one file, whose commits are retried
    /// `num_retries` times.
    async fn make_table_with_one_file(catalog: &impl Catalog, num_retries: &str) -> Table {
        let table = make_v2_minimal_table_in_catalog(catalog).await;
        let table = set_table_properties(catalog, &table, &[
            (TableProperties::PROPERTY_COMMIT_NUM_RETRIES, num_retries),
            (TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS, "1"),
        ])
        .await;
        let file = make_data_file(&table, "test/1.parquet", 10, 100);
        append_files(catalog, &table, vec![file]).await
    }

    /// A transaction appending one file at `path` to `table`.
    fn make_append_transaction(table: &Table, path: &str) -> Transaction {
        let tx = Transaction::new(table);
        let file = make_data_file(table, path, 10, 100);
        tx.fast_append()
            .add_data_files(vec![file])
            .apply(tx)
            .unwrap()
    }

    /// A V2 table in `catalog` holding two files in two manifests, which a
    /// commit that merges manifests merges into one.
    async fn make_table_with_two_manifests(catalog: &impl Catalog) -> Table {
        let table = make_table_with_one_file(catalog, "0").await;
        let table = set_table_properties(catalog, &table, &[(
            TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE,
            "2",
        )])
        .await;
        let file = make_data_file(&table, "test/2.parquet", 10, 100);
        append_files(catalog, &table, vec![file]).await
    }

    /// An action that writes a manifest at `manifest_path` and no manifest
    /// list.
    struct WriteLoneManifestAction {
        manifest_path: String,
    }

    #[async_trait]
    impl TransactionAction for WriteLoneManifestAction {
        async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
            self.commit_recording_files(table, &WrittenFiles::default())
                .await
        }

        async fn commit_recording_files(
            self: Arc<Self>,
            table: &Table,
            written_files: &WrittenFiles,
        ) -> Result<ActionCommit> {
            let output_file =
                written_files.new_manifest_output(table.file_io(), self.manifest_path.clone())?;
            output_file.write(Bytes::from_static(b"manifest")).await?;
            let updates = HashMap::from([("test.key".to_string(), "test.value".to_string())]);
            Ok(ActionCommit::new(
                vec![TableUpdate::SetProperties { updates }],
                vec![],
            ))
        }
    }

    fn make_conflict() -> Error {
        Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict").with_retryable(true)
    }

    /// The paths of the manifests named like `manifest_path` with a lower
    /// number, which the same commit wrote before it.
    fn list_earlier_manifest_paths(manifest_path: &str) -> Vec<String> {
        let (prefix, numbered_suffix) = manifest_path.rsplit_once("-m").unwrap();
        let (number, extension) = numbered_suffix.split_once('.').unwrap();
        (0..number.parse::<u64>().unwrap())
            .map(|earlier_number| format!("{prefix}-m{earlier_number}.{extension}"))
            .collect()
    }

    /// The files in storage that the commit producing `snapshot` wrote: its
    /// manifest list, the manifests it lists as its own, and the manifests it
    /// wrote before those and does not list.
    async fn collect_snapshot_files(table: &Table, snapshot: &Snapshot) -> Vec<String> {
        let snapshot = Arc::new(snapshot.clone());
        let manifest_list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        let mut paths = vec![snapshot.manifest_list().to_string()];
        for manifest in manifest_list.entries() {
            if manifest.added_snapshot_id != snapshot.snapshot_id() {
                continue;
            }
            for earlier_path in list_earlier_manifest_paths(&manifest.manifest_path) {
                let is_listed = paths.contains(&earlier_path);
                if !is_listed && table.file_io().exists(&earlier_path).await.unwrap() {
                    paths.push(earlier_path);
                }
            }
            paths.push(manifest.manifest_path.clone());
        }
        paths.sort();
        paths.dedup();
        paths
    }

    /// The files in storage that the attempt committing `updates` to `table`
    /// wrote, snapshot by snapshot.
    async fn collect_attempt_files(table: &Table, updates: &[TableUpdate]) -> Vec<String> {
        let mut paths = Vec::new();
        for update in updates {
            if let TableUpdate::AddSnapshot { snapshot } = update {
                paths.extend(collect_snapshot_files(table, snapshot).await);
            }
        }
        paths
    }

    /// The manifest list and the manifests of every snapshot of `table`.
    async fn collect_table_files(table: &Table) -> Vec<String> {
        let mut paths = Vec::new();
        for snapshot in table.metadata().snapshots() {
            paths.extend(collect_snapshot_files(table, snapshot).await);
        }
        paths
    }

    /// How many of `paths` are in the storage of `table`.
    async fn count_stored_files(table: &Table, paths: &[String]) -> usize {
        let mut stored_count = 0;
        for path in paths {
            if table.file_io().exists(path).await.unwrap() {
                stored_count += 1;
            }
        }
        stored_count
    }

    /// A catalog serving the tables of `catalog`, whose `update_table` records
    /// the files of the attempt in `attempt_files`, then fails with the next
    /// of `failures` or, once they are used up, commits to `catalog`.
    fn make_catalog_failing_commits<C: Catalog + 'static>(
        catalog: Arc<C>,
        failures: Vec<Error>,
        attempt_files: AttemptFiles,
    ) -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        let loading_catalog = catalog.clone();
        mock_catalog
            .expect_load_table()
            .returning_st(move |table_ident| {
                let catalog = loading_catalog.clone();
                let table_ident = table_ident.clone();
                Box::pin(async move { catalog.load_table(&table_ident).await })
            });

        let mut failures = failures.into_iter();
        mock_catalog
            .expect_update_table()
            .returning_st(move |mut commit| {
                let catalog = catalog.clone();
                let attempt_files = attempt_files.clone();
                let failure = failures.next();
                Box::pin(async move {
                    let table = catalog.load_table(commit.identifier()).await?;
                    let updates = commit.take_updates();
                    let written_files = collect_attempt_files(&table, &updates).await;
                    attempt_files.lock().unwrap().push(written_files);
                    match failure {
                        Some(error) => Err(error),
                        None => {
                            let commit = TableCommit::builder()
                                .ident(commit.identifier().clone())
                                .updates(updates)
                                .requirements(commit.take_requirements())
                                .build();
                            catalog.update_table(commit).await
                        }
                    }
                })
            });

        mock_catalog
    }

    /// A catalog serving `table`, whose `update_table` records the files of
    /// the attempt in `attempt_files` and applies the commit to `table`
    /// whatever the commit requires of it.
    fn make_catalog_accepting_commits(table: &Table, attempt_files: AttemptFiles) -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        let loaded_table = table.clone();
        mock_catalog.expect_load_table().returning_st(move |_| {
            let table = loaded_table.clone();
            Box::pin(async move { Ok(table) })
        });

        let base_table = table.clone();
        mock_catalog
            .expect_update_table()
            .returning_st(move |mut commit| {
                let table = base_table.clone();
                let attempt_files = attempt_files.clone();
                Box::pin(async move {
                    let updates = commit.take_updates();
                    let written_files = collect_attempt_files(&table, &updates).await;
                    attempt_files.lock().unwrap().push(written_files);
                    TableCommit::builder()
                        .ident(commit.identifier().clone())
                        .updates(updates)
                        .requirements(vec![])
                        .build()
                        .apply(table)
                })
            });

        mock_catalog
    }

    #[tokio::test]
    async fn test_commit_rejected_with_conflict_deletes_the_files_it_wrote() {
        let catalog = Arc::new(new_memory_catalog().await);
        let table = make_table_with_one_file(catalog.as_ref(), "0").await;
        let table_files = collect_table_files(&table).await;
        let attempt_files = AttemptFiles::default();
        let failing_catalog =
            make_catalog_failing_commits(catalog, vec![make_conflict()], attempt_files.clone());
        let tx = make_append_transaction(&table, "test/2.parquet");

        let error = tx.commit(&failing_catalog).await.unwrap_err();

        let attempt_files = attempt_files.lock().unwrap().clone();
        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
        assert_eq!(attempt_files.len(), 1);
        assert_eq!(attempt_files[0].len(), 2);
        assert_eq!(count_stored_files(&table, &attempt_files[0]).await, 0);
        assert_eq!(count_stored_files(&table, &table_files).await, 2);
    }

    #[tokio::test]
    async fn test_commit_retried_after_conflict_keeps_only_the_files_of_the_attempt_that_committed()
    {
        let catalog = Arc::new(new_memory_catalog().await);
        let table = make_table_with_one_file(catalog.as_ref(), "1").await;
        let attempt_files = AttemptFiles::default();
        let failing_catalog =
            make_catalog_failing_commits(catalog, vec![make_conflict()], attempt_files.clone());
        let tx = make_append_transaction(&table, "test/2.parquet");

        let table = tx.commit(&failing_catalog).await.unwrap();

        let attempt_files = attempt_files.lock().unwrap().clone();
        let current_snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(attempt_files.len(), 2);
        assert_eq!(attempt_files[0].len(), 2);
        assert_eq!(count_stored_files(&table, &attempt_files[0]).await, 0);
        assert_eq!(
            collect_snapshot_files(&table, current_snapshot).await,
            attempt_files[1]
        );
        assert_eq!(load_current_manifests(&table).await.len(), 2);
    }

    #[tokio::test]
    async fn test_commit_with_unknown_outcome_keeps_the_files_it_wrote() {
        let catalog = Arc::new(new_memory_catalog().await);
        let table = make_table_with_one_file(catalog.as_ref(), "0").await;
        let attempt_files = AttemptFiles::default();
        let lost_response = Error::new(ErrorKind::Unexpected, "Gateway timeout");
        let failing_catalog =
            make_catalog_failing_commits(catalog, vec![lost_response], attempt_files.clone());
        let tx = make_append_transaction(&table, "test/2.parquet");

        let error = tx.commit(&failing_catalog).await.unwrap_err();

        let attempt_files = attempt_files.lock().unwrap().clone();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert_eq!(attempt_files[0].len(), 2);
        assert_eq!(count_stored_files(&table, &attempt_files[0]).await, 2);
    }

    #[tokio::test]
    async fn test_successful_commit_keeps_the_files_it_wrote() {
        let catalog = Arc::new(new_memory_catalog().await);
        let table = make_table_with_one_file(catalog.as_ref(), "0").await;
        let attempt_files = AttemptFiles::default();
        let recording_catalog =
            make_catalog_failing_commits(catalog, vec![], attempt_files.clone());
        let tx = make_append_transaction(&table, "test/2.parquet");

        let table = tx.commit(&recording_catalog).await.unwrap();

        let attempt_files = attempt_files.lock().unwrap().clone();
        let table_files = collect_table_files(&table).await;
        assert_eq!(attempt_files[0].len(), 2);
        assert_eq!(count_stored_files(&table, &attempt_files[0]).await, 2);
        assert_eq!(count_stored_files(&table, &table_files).await, 4);
    }

    #[tokio::test]
    async fn test_rewrite_merging_manifests_rejected_with_conflict_deletes_the_files_it_wrote() {
        let catalog = Arc::new(new_memory_catalog().await);
        let table = make_table_with_one_file(catalog.as_ref(), "0").await;
        let table = set_table_properties(catalog.as_ref(), &table, &[(
            TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE,
            "2",
        )])
        .await;
        let kept_file = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(catalog.as_ref(), &table, vec![kept_file]).await;
        let table_files = collect_table_files(&table).await;
        let attempt_files = AttemptFiles::default();
        let failing_catalog =
            make_catalog_failing_commits(catalog, vec![make_conflict()], attempt_files.clone());
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .with_manifest_merge(true)
            .delete_file(make_data_file(&table, "test/1.parquet", 10, 100))
            .add_file(make_data_file(&table, "test/3.parquet", 10, 100));
        let tx = action.apply(tx).unwrap();

        let error = tx.commit(&failing_catalog).await.unwrap_err();

        // The attempt wrote a manifest list and one manifest: what it kept
        // of the manifest it filtered, the added file and the manifest it
        // kept whole, merged.
        let attempt_files = attempt_files.lock().unwrap().clone();
        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
        assert_eq!(attempt_files[0].len(), 2);
        assert_eq!(count_stored_files(&table, &attempt_files[0]).await, 0);
        assert_eq!(count_stored_files(&table, &table_files).await, 4);
    }

    #[tokio::test]
    async fn test_commit_failing_before_the_catalog_is_asked_deletes_the_files_it_wrote() {
        let catalog = Arc::new(new_memory_catalog().await);
        let table = make_table_with_one_file(catalog.as_ref(), "0").await;
        let attempt_files = AttemptFiles::default();
        let recording_catalog =
            make_catalog_failing_commits(catalog, vec![], attempt_files.clone());
        let commit_uuid = Uuid::now_v7();
        let first_manifest_path = format!(
            "{}/{commit_uuid}-m0.avro",
            table.metadata().metadata_location().unwrap()
        );
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files(vec![make_data_file(&table, "test/2.parquet", 10, 100)]);
        let tx = action.apply(tx).unwrap();
        let already_added_file = make_data_file(&table, "test/2.parquet", 10, 100);
        let action = tx.fast_append().add_data_files(vec![already_added_file]);
        let tx = action.apply(tx).unwrap();

        let error = tx.commit(&recording_catalog).await.unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(attempt_files.lock().unwrap().is_empty());
        assert!(!table.file_io().exists(&first_manifest_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_append_merging_manifests_deletes_the_manifest_it_merged_away() {
        let catalog = Arc::new(new_memory_catalog().await);
        let table = make_table_with_two_manifests(catalog.as_ref()).await;
        let table_files = collect_table_files(&table).await;
        let attempt_files = AttemptFiles::default();
        let recording_catalog =
            make_catalog_failing_commits(catalog, vec![], attempt_files.clone());
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .with_manifest_merge(true)
            .add_data_files(vec![make_data_file(&table, "test/3.parquet", 10, 100)]);
        let tx = action.apply(tx).unwrap();

        let table = tx.commit(&recording_catalog).await.unwrap();

        // The attempt wrote a manifest list and two manifests: the one for the
        // added file, and that one merged with the two the table had. The
        // manifest list names the merged one only.
        let attempt_files = attempt_files.lock().unwrap().clone();
        let manifests = load_current_manifests(&table).await;
        let entries = load_current_entries(&table).await;
        assert_eq!(attempt_files[0].len(), 3);
        assert_eq!(count_stored_files(&table, &attempt_files[0]).await, 2);
        assert_eq!(manifests.len(), 1);
        assert_eq!(collect_live_paths(&entries), [
            "test/1.parquet",
            "test/2.parquet",
            "test/3.parquet"
        ]);
        assert_eq!(count_stored_files(&table, &table_files).await, 4);
    }

    #[tokio::test]
    async fn test_rewrite_merging_manifests_writes_the_merged_manifest_alone() {
        let catalog = Arc::new(new_memory_catalog().await);
        let table = make_table_with_two_manifests(catalog.as_ref()).await;
        let table_files = collect_table_files(&table).await;
        let attempt_files = AttemptFiles::default();
        let recording_catalog =
            make_catalog_failing_commits(catalog, vec![], attempt_files.clone());
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .with_manifest_merge(true)
            .delete_file(make_data_file(&table, "test/1.parquet", 10, 100))
            .add_file(make_data_file(&table, "test/3.parquet", 10, 100));
        let tx = action.apply(tx).unwrap();

        let table = tx.commit(&recording_catalog).await.unwrap();

        // The attempt wrote a manifest list and one manifest, which the
        // list names: what it kept of the manifest it filtered, the added
        // file and the manifest it kept whole, merged.
        let attempt_files = attempt_files.lock().unwrap().clone();
        let manifests = load_current_manifests(&table).await;
        let entries = load_current_entries(&table).await;
        assert_eq!(attempt_files[0].len(), 2);
        assert_eq!(count_stored_files(&table, &attempt_files[0]).await, 2);
        assert_eq!(manifests.len(), 1);
        assert_eq!(collect_live_paths(&entries), [
            "test/2.parquet",
            "test/3.parquet"
        ]);
        assert_eq!(count_stored_files(&table, &table_files).await, 4);
    }

    #[tokio::test]
    async fn test_commit_of_two_appends_keeps_the_manifest_only_the_first_manifest_list_names() {
        let catalog = new_memory_catalog().await;
        let table = make_table_with_two_manifests(&catalog).await;
        let attempt_files = AttemptFiles::default();
        let accepting_catalog = make_catalog_accepting_commits(&table, attempt_files.clone());
        let tx = make_append_transaction(&table, "test/3.parquet");
        let action = tx
            .fast_append()
            .with_manifest_merge(true)
            .add_data_files(vec![make_data_file(&table, "test/4.parquet", 10, 100)]);
        let tx = action.apply(tx).unwrap();

        let table = tx.commit(&accepting_catalog).await.unwrap();

        // The first append wrote a manifest list and the manifest it names.
        // The second wrote a manifest list and two manifests: the one for its
        // file, and that one merged with every earlier manifest, the first
        // append's included. Its manifest list names the merged one only.
        let attempt_files = attempt_files.lock().unwrap().clone();
        let table_files = collect_table_files(&table).await;
        assert_eq!(attempt_files[0].len(), 5);
        assert_eq!(count_stored_files(&table, &attempt_files[0]).await, 4);
        assert_eq!(load_current_manifests(&table).await.len(), 1);
        assert_eq!(table_files.len(), 8);
        assert_eq!(count_stored_files(&table, &table_files).await, 8);
    }

    #[tokio::test]
    async fn test_commit_keeps_the_manifest_of_an_action_that_wrote_no_manifest_list() {
        let catalog = new_memory_catalog().await;
        let table = make_table_with_one_file(&catalog, "0").await;
        let manifest_path = format!(
            "{}/lone-m0.avro",
            table.metadata().metadata_location().unwrap()
        );
        let action = WriteLoneManifestAction {
            manifest_path: manifest_path.clone(),
        };
        let tx = action.apply(Transaction::new(&table)).unwrap();

        tx.commit(&catalog).await.unwrap();

        assert!(table.file_io().exists(&manifest_path).await.unwrap());
    }
}
