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

//! Shared engine for snapshot-producing operations that both add and delete files.
//!
//! [`MergingSnapshotProducer`] is the Rust equivalent of Java's
//! `MergingSnapshotProducer`. It handles manifest filtering, new manifest
//! creation, summary computation, and delegates the final snapshot commit to
//! [`SnapshotProducer`].

use std::cmp::Ordering as ValueOrdering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use futures::{StreamExt, stream};
use uuid::Uuid;

use crate::error::Result;
use crate::io::OutputFile;
use crate::spec::{
    DataFile, DataFileFormat, Datum, FieldSummary, FormatVersion, Literal, Manifest,
    ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus, ManifestWriter,
    ManifestWriterBuilder, Operation, PartitionSpec, SchemaRef, SnapshotSummaryCollector,
    StructType, Summary, TableProperties, Type, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::manifest_merge::ManifestMergeManager;
use crate::transaction::snapshot::SnapshotProducer;
use crate::transaction::written_files::WrittenFiles;
use crate::{Error, ErrorKind};

/// Create a manifest writer that handles encryption when available.
pub(crate) fn new_manifest_writer(
    table: &Table,
    output_file: OutputFile,
    snapshot_id: Option<i64>,
    content: ManifestContentType,
    schema: SchemaRef,
    partition_spec: PartitionSpec,
) -> Result<ManifestWriter> {
    let builder = if let Some(em) = table.encryption_manager() {
        ManifestWriterBuilder::new_from_encrypted(
            em.encrypt(output_file),
            snapshot_id,
            schema,
            partition_spec,
        )?
    } else {
        ManifestWriterBuilder::new(output_file, snapshot_id, schema, partition_spec)
    };

    match table.metadata().format_version() {
        FormatVersion::V1 => Ok(builder.build_v1()),
        FormatVersion::V2 => match content {
            ManifestContentType::Data => Ok(builder.build_v2_data()),
            ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
        },
        FormatVersion::V3 => match content {
            ManifestContentType::Data => Ok(builder.build_v3_data()),
            ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
        },
    }
}

/// Filters existing manifests by removing entries for deleted data files.
///
/// This is equivalent to Java's `ManifestFilterManager`. When a rewrite or
/// overwrite operation deletes files, the filter manager rewrites affected
/// manifests so that deleted entries are dropped and surviving entries are
/// re-emitted with [`ManifestStatus::Existing`].
///
/// Only the manifests that may list a file to delete are read: a manifest of
/// another partition spec, or whose partition summaries rule out the
/// partition of every file to delete, is kept as it is without being read.
/// The others are read [`MANIFEST_READ_CONCURRENCY`] at a time until every
/// file to delete has been found: a snapshot lists a live file in one
/// manifest, so the manifests after that are kept unread too. The manifests
/// the files to delete are said to be listed in are read first, and the
/// others, in the order the manifest list has them, only when a file was not
/// found where it was said to be.
pub(crate) struct ManifestFilterManager {
    /// The files to delete, by path.
    deleted_files: HashMap<String, DeletedFile>,
    fail_missing_delete_paths: bool,
}

/// A file to delete.
struct DeletedFile {
    file: DataFile,
    /// The path of the manifest the file is said to be listed in.
    manifest_path: Option<String>,
}

/// How many manifests [`ManifestFilterManager::filter_manifests`] reads at
/// once.
const MANIFEST_READ_CONCURRENCY: usize = 32;

impl ManifestFilterManager {
    pub(crate) fn new(fail_missing_delete_paths: bool) -> Self {
        Self {
            deleted_files: HashMap::new(),
            fail_missing_delete_paths,
        }
    }

    /// Delete `file`, which the manifest at `manifest_path` is said to
    /// list, when one is named.
    pub(crate) fn add_delete(&mut self, file: DataFile, manifest_path: Option<String>) {
        self.deleted_files
            .insert(file.file_path.clone(), DeletedFile {
                file,
                manifest_path,
            });
    }

    /// Whether a file to delete is said to be listed in `manifest_file`.
    fn is_named_by_deleted_file(&self, manifest_file: &ManifestFile) -> bool {
        self.deleted_files.values().any(|deleted| {
            deleted.manifest_path.as_deref() == Some(manifest_file.manifest_path.as_str())
        })
    }

    /// Whether `manifest_file`, a data manifest, may list a file to delete:
    /// it has the partition spec of one whose partition its partition
    /// summaries do not rule out. A manifest without summaries, or of a spec
    /// whose partition type cannot be told, may list any file of its spec.
    fn may_hold_deleted_file(&self, table: &Table, manifest_file: &ManifestFile) -> bool {
        let mut same_spec = self
            .deleted_files
            .values()
            .map(|deleted| &deleted.file)
            .filter(|file| file.partition_spec_id == manifest_file.partition_spec_id)
            .peekable();
        if same_spec.peek().is_none() {
            return false;
        }
        let Some(summaries) = manifest_file.partitions.as_deref() else {
            return true;
        };
        let metadata = table.metadata();
        let Some(partition_type) = metadata
            .partition_spec_by_id(manifest_file.partition_spec_id)
            .and_then(|spec| spec.partition_type(metadata.current_schema()).ok())
        else {
            return true;
        };
        same_spec.any(|file| may_hold_partition(summaries, &partition_type, file))
    }

    /// Filter `manifests` by removing entries whose file path is in the delete
    /// set. Returns the surviving manifests plus a [`SnapshotSummaryCollector`]
    /// that recorded metrics for every removed file.
    pub(crate) async fn filter_manifests(
        &self,
        table: &Table,
        manifests: Vec<ManifestFile>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        manifest_counter: &AtomicU64,
        written_files: &WrittenFiles,
    ) -> Result<(Vec<ManifestFile>, SnapshotSummaryCollector)> {
        if self.deleted_files.is_empty() {
            return Ok((manifests, SnapshotSummaryCollector::default()));
        }

        let mut removed_collector = SnapshotSummaryCollector::default();
        let mut found_paths: HashSet<String> = HashSet::new();

        // Only data manifests are filtered, and of those only the ones that
        // may list a file to delete; every other manifest passes through
        // unchanged and unread. The manifests the files to delete are said to
        // be listed in come first, and the others are read only when those
        // did not list every file.
        let data_manifests = || {
            manifests
                .iter()
                .enumerate()
                .filter(|(_, manifest_file)| manifest_file.content == ManifestContentType::Data)
        };
        let named: Vec<usize> = data_manifests()
            .filter(|(_, manifest_file)| self.is_named_by_deleted_file(manifest_file))
            .map(|(position, _)| position)
            .collect();
        let others: Vec<usize> = data_manifests()
            .filter(|(_, manifest_file)| {
                !self.is_named_by_deleted_file(manifest_file)
                    && self.may_hold_deleted_file(table, manifest_file)
            })
            .map(|(position, _)| position)
            .collect();
        // What takes the place of a manifest that lists a file to delete, by
        // the manifest's position: `None` when nothing is left to say about it.
        let mut filtered: HashMap<usize, Option<ManifestFile>> = HashMap::new();
        for candidates in [named, others] {
            let reads: Vec<_> = candidates
                .into_iter()
                .map(|position| read_candidate(table, &manifests[position], position))
                .collect();
            let mut reads = stream::iter(reads).buffered(MANIFEST_READ_CONCURRENCY);
            while found_paths.len() < self.deleted_files.len() {
                let Some((position, manifest)) = reads.next().await else {
                    break;
                };
                let manifest_file = &manifests[position];
                let manifest = manifest?;

                // Check whether this manifest contains any files we want to delete.
                let has_deletes = manifest
                    .entries()
                    .iter()
                    .any(|e| e.is_alive() && self.deleted_files.contains_key(e.file_path()));

                if !has_deletes {
                    // Manifest is unaffected — pass through verbatim.
                    continue;
                }

                // Resolve the partition spec for this manifest. After partition
                // evolution, old manifests may carry a non-default spec — we must
                // use the manifest's own spec so the rewritten manifest stays valid.
                let manifest_spec = table
                    .metadata()
                    .partition_spec_by_id(manifest_file.partition_spec_id)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Manifest references unknown partition spec {}",
                                manifest_file.partition_spec_id
                            ),
                        )
                    })?
                    .clone();
                let schema = table.metadata().current_schema().clone();

                // Rewrite: keep surviving entries as EXISTING, and record the
                // removed ones as DELETED so that the file they name can be
                // cleaned up when this snapshot expires.
                let mut surviving_entries: Vec<ManifestEntry> = Vec::new();
                let mut removed_entries: Vec<ManifestEntry> = Vec::new();
                for entry in manifest.entries() {
                    if entry.is_alive() && self.deleted_files.contains_key(entry.file_path()) {
                        // Record removal metrics.
                        found_paths.insert(entry.file_path().to_string());
                        removed_collector.remove_file(
                            entry.data_file(),
                            schema.clone(),
                            manifest_spec.clone(),
                        );
                        removed_entries.push(entry.as_ref().clone());
                    } else if entry.is_alive() {
                        // Surviving entry — re-emit as EXISTING with original ids preserved.
                        let seq = entry.sequence_number().ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Manifest entry for {} is missing sequence_number",
                                    entry.file_path()
                                ),
                            )
                        })?;
                        let existing = ManifestEntry::builder()
                            .status(ManifestStatus::Existing)
                            .snapshot_id(
                                entry
                                    .snapshot_id()
                                    .unwrap_or(manifest_file.added_snapshot_id),
                            )
                            .sequence_number(seq)
                            .file_sequence_number_opt(entry.file_sequence_number)
                            .data_file(entry.data_file().clone())
                            .build();
                        surviving_entries.push(existing);
                    }
                    // Already-deleted entries (status == Deleted) are dropped.
                }

                if surviving_entries.is_empty() && removed_entries.is_empty() {
                    // Nothing left to say about this manifest — omit entirely.
                    filtered.insert(position, None);
                    continue;
                }

                // Write the filtered manifest using the manifest's own partition spec.
                let new_manifest_path = format!(
                    "{}/{}-m{}.{}",
                    table.metadata().metadata_location()?,
                    commit_uuid,
                    manifest_counter.fetch_add(1, Ordering::Relaxed),
                    DataFileFormat::Avro,
                );
                let output_file =
                    written_files.new_manifest_output(table.file_io(), new_manifest_path)?;
                // Use the current snapshot_id so that the manifest list writer
                // can assign sequence numbers to this rewritten manifest.
                let mut writer = new_manifest_writer(
                    table,
                    output_file,
                    Some(snapshot_id),
                    ManifestContentType::Data,
                    schema.clone(),
                    manifest_spec.as_ref().clone(),
                )?;
                for entry in surviving_entries {
                    writer.add_existing_entry(entry)?;
                }
                for entry in removed_entries {
                    // `add_delete_entry` stamps the removing snapshot on the entry
                    // and keeps the file's own sequence numbers, which is what an
                    // expiring reader needs to attribute the removal.
                    writer.add_delete_entry(entry)?;
                }
                let new_manifest = writer.write_manifest_file().await?;
                filtered.insert(position, Some(new_manifest));
            }
        }

        let result: Vec<ManifestFile> = manifests
            .into_iter()
            .enumerate()
            .filter_map(
                |(position, manifest_file)| match filtered.remove(&position) {
                    Some(replacement) => replacement,
                    None => Some(manifest_file),
                },
            )
            .collect();

        // Validate that every delete target was found.
        if self.fail_missing_delete_paths {
            let missing: Vec<&str> = self
                .deleted_files
                .keys()
                .filter(|p| !found_paths.contains(p.as_str()))
                .map(String::as_str)
                .collect();
            if !missing.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Failed to find the following files to delete in the current snapshot: {}",
                        missing.join(", ")
                    ),
                ));
            }
        }

        Ok((result, removed_collector))
    }
}

/// Read `manifest_file`, the manifest at `position` of the manifests being
/// filtered.
async fn read_candidate(
    table: &Table,
    manifest_file: &ManifestFile,
    position: usize,
) -> (usize, Result<Manifest>) {
    let manifest = table.manifest_reader().read(manifest_file).await;
    (position, manifest)
}

/// Whether a manifest with the partition summaries `summaries`, one for each
/// field of `partition_type`, may list `file`: no summary rules out the
/// file's value of its field. A summary rules a value out when it is null
/// and the manifest has no null of the field, or when it lies below the
/// field's lower bound or above its upper bound. Anything that cannot be
/// compared rules nothing out.
fn may_hold_partition(
    summaries: &[FieldSummary],
    partition_type: &StructType,
    file: &DataFile,
) -> bool {
    let values = file.partition().fields();
    let fields = partition_type.fields();
    if values.len() != summaries.len() || fields.len() != summaries.len() {
        return true;
    }
    summaries
        .iter()
        .zip(values)
        .zip(fields)
        .all(|((summary, value), field)| {
            let Some(value) = value else {
                return summary.contains_null;
            };
            let (Literal::Primitive(value), Type::Primitive(value_type)) =
                (value, field.field_type.as_ref())
            else {
                return true;
            };
            let value = Datum::new(value_type.clone(), value.clone());
            let is_beyond = |bound: &Option<serde_bytes::ByteBuf>, beyond: ValueOrdering| {
                bound
                    .as_ref()
                    .and_then(|bound| Datum::try_from_bytes(bound, value_type.clone()).ok())
                    .is_some_and(|bound| value.partial_cmp(&bound) == Some(beyond))
            };
            !is_beyond(&summary.lower_bound, ValueOrdering::Less)
                && !is_beyond(&summary.upper_bound, ValueOrdering::Greater)
        })
}

/// Shared engine for operations that both add and remove data files.
///
/// This struct is the Rust equivalent of Java's `MergingSnapshotProducer`.
/// It manages:
/// - Tracking files to add and delete
/// - Filtering existing manifests to remove deleted files
/// - Writing new manifests for added files
/// - Computing snapshot summaries that account for both additions and removals
/// - Delegating final snapshot creation to [`SnapshotProducer`]
///
/// Concrete transaction actions like [`RewriteFilesAction`] own an instance
/// of this struct and configure it with the appropriate [`Operation`] type
/// and validation rules.
pub(crate) struct MergingSnapshotProducer {
    operation: Operation,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    filter_manager: ManifestFilterManager,
    commit_uuid: Uuid,
    data_sequence_number: Option<i64>,
    /// Whether added files are checked against the files the table already
    /// references, which reads every data manifest.
    check_duplicate: bool,
    /// Whether small data manifests are merged, see [`ManifestMergeManager`].
    manifest_merge: bool,
    /// Names the manifests this producer writes. It is never reset, so a
    /// commit retry cannot overwrite a manifest an earlier attempt wrote
    /// under the same commit uuid.
    manifest_counter: AtomicU64,
}

impl MergingSnapshotProducer {
    pub(crate) fn new(operation: Operation) -> Self {
        Self {
            operation,
            added_data_files: Vec::new(),
            deleted_data_files: Vec::new(),
            filter_manager: ManifestFilterManager::new(true),
            commit_uuid: Uuid::now_v7(),
            data_sequence_number: None,
            check_duplicate: true,
            manifest_merge: false,
            manifest_counter: AtomicU64::new(0),
        }
    }

    pub(crate) fn add_data_file(&mut self, file: DataFile) {
        self.added_data_files.push(file);
    }

    pub(crate) fn set_data_sequence_number(&mut self, sequence_number: i64) {
        self.data_sequence_number = Some(sequence_number);
    }

    pub(crate) fn set_check_duplicate(&mut self, check_duplicate: bool) {
        self.check_duplicate = check_duplicate;
    }

    pub(crate) fn set_manifest_merge(&mut self, manifest_merge: bool) {
        self.manifest_merge = manifest_merge;
    }

    /// Delete `file`, which the manifest at `manifest_path` is said to
    /// list, when one is named.
    pub(crate) fn delete_data_file(&mut self, file: DataFile, manifest_path: Option<String>) {
        self.filter_manager.add_delete(file.clone(), manifest_path);
        self.deleted_data_files.push(file);
    }

    pub(crate) fn has_added_data_files(&self) -> bool {
        !self.added_data_files.is_empty()
    }

    pub(crate) fn has_deleted_data_files(&self) -> bool {
        !self.deleted_data_files.is_empty()
    }

    /// Reject a data sequence number above the one the new snapshot will carry.
    fn validate_data_sequence_number(&self, table: &Table) -> Result<()> {
        let Some(sequence_number) = self.data_sequence_number else {
            return Ok(());
        };
        let snapshot_sequence_number = table.metadata().next_sequence_number();
        if sequence_number > snapshot_sequence_number {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Data sequence number {sequence_number} is greater than the snapshot's {snapshot_sequence_number}."
                ),
            ));
        }
        Ok(())
    }

    /// Produce manifests, compute summary, and commit a new snapshot. Every
    /// manifest and the manifest list written is recorded in `written_files`.
    pub(crate) async fn commit_snapshot(
        &self,
        table: &Table,
        written_files: &WrittenFiles,
    ) -> Result<ActionCommit> {
        self.validate_data_sequence_number(table)?;
        // Create the SnapshotProducer first so we can use its snapshot_id
        // for new manifests. This ensures the manifest list writer can
        // assign sequence numbers to manifests from this snapshot.
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid,
            HashMap::new(),
            self.added_data_files.clone(),
            written_files,
        );
        snapshot_producer.validate_added_data_files()?;
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }
        let snapshot_id = snapshot_producer.snapshot_id;

        // 1. Load existing manifests from the current snapshot.
        let existing_manifests = match table.metadata().current_snapshot() {
            Some(snapshot) => {
                let manifest_list = table.manifest_list_reader(snapshot).load().await?;
                manifest_list
                    .entries()
                    .iter()
                    // Java's third arm is the new snapshot's id, which no existing manifest has.
                    .filter(|e| e.has_added_files() || e.has_existing_files())
                    .cloned()
                    .collect()
            }
            None => Vec::new(),
        };

        // 2. Filter existing manifests — remove deleted file entries.
        let (mut filtered_manifests, removed_collector) = self
            .filter_manager
            .filter_manifests(
                table,
                existing_manifests,
                snapshot_id,
                self.commit_uuid,
                &self.manifest_counter,
                written_files,
            )
            .await?;

        // 3. Write a new manifest for added files.
        let mut added_manifests = Vec::new();
        if !self.added_data_files.is_empty() {
            added_manifests.push(
                self.write_added_manifest(table, snapshot_id, written_files)
                    .await?,
            );
        }

        // 4. Merge small data manifests, or list the added manifest last.
        let manifests = if self.manifest_merge {
            ManifestMergeManager::try_from_table(table)?
                .merge_manifests(
                    table,
                    snapshot_id,
                    added_manifests,
                    filtered_manifests,
                    || self.generate_manifest_file_path(table),
                    written_files,
                )
                .await?
        } else {
            filtered_manifests.extend(added_manifests);
            filtered_manifests
        };

        // 5. Compute summary (added + removed).
        let summary = self.build_summary(table, removed_collector)?;

        // 6. Delegate to SnapshotProducer for manifest list + snapshot creation.
        snapshot_producer
            .commit_with_manifests(manifests, summary)
            .await
    }

    /// The path of the next manifest this producer writes.
    fn generate_manifest_file_path(&self, table: &Table) -> Result<String> {
        Ok(format!(
            "{}/{}-m{}.{}",
            table.metadata().metadata_location()?,
            self.commit_uuid,
            self.manifest_counter.fetch_add(1, Ordering::Relaxed),
            DataFileFormat::Avro,
        ))
    }

    async fn write_added_manifest(
        &self,
        table: &Table,
        snapshot_id: i64,
        written_files: &WrittenFiles,
    ) -> Result<ManifestFile> {
        let new_manifest_path = self.generate_manifest_file_path(table)?;
        let output_file = written_files.new_manifest_output(table.file_io(), new_manifest_path)?;
        let schema = table.metadata().current_schema().clone();
        let partition_spec = table.metadata().default_partition_spec().as_ref().clone();

        let mut writer = new_manifest_writer(
            table,
            output_file,
            Some(snapshot_id),
            ManifestContentType::Data,
            schema,
            partition_spec,
        )?;

        let format_version = table.metadata().format_version();
        for data_file in &self.added_data_files {
            if let Some(sequence_number) = self.data_sequence_number {
                writer.add_file(data_file.clone(), sequence_number)?;
                continue;
            }
            let entry_builder = ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(data_file.clone());
            let entry = if format_version == FormatVersion::V1 {
                // V1 requires snapshot_id on each entry.
                entry_builder.snapshot_id(snapshot_id).build()
            } else {
                entry_builder.build()
            };
            writer.add_entry(entry)?;
        }

        writer.write_manifest_file().await
    }

    fn build_summary(
        &self,
        table: &Table,
        removed_collector: SnapshotSummaryCollector,
    ) -> Result<Summary> {
        let table_metadata = table.metadata_ref();
        let schema = table_metadata.current_schema().clone();
        let partition_spec = table_metadata.default_partition_spec().clone();

        // Add to the filter's collector rather than merging two: `merge` drops
        // partition metrics unless both sides trust them, and neither does.
        let mut collector = removed_collector;
        collector.set_partition_summary_limit(
            table_metadata
                .properties()
                .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
                .and_then(|limit| limit.parse::<u64>().ok())
                .unwrap_or(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT),
        );
        for file in &self.added_data_files {
            collector.add_file(file, schema.clone(), partition_spec.clone());
        }

        let summary = Summary {
            operation: self.operation.clone(),
            additional_properties: collector.build(),
        };

        let previous_snapshot = table_metadata.current_snapshot();
        update_snapshot_summaries(summary, previous_snapshot.map(|s| s.summary()), false)
    }
}
