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
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use futures::{StreamExt, stream};
use uuid::Uuid;

use crate::error::Result;
use crate::io::OutputFile;
use crate::spec::{
    DataFile, DataFileFormat, Datum, EncodedEntry, EncodedManifest, EntryCodec, FieldSummary,
    FormatVersion, Literal, Manifest, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, ManifestWriter, ManifestWriterBuilder, Operation, PartitionSpec, SchemaRef,
    SnapshotSummaryCollector, StructType, Summary, TableProperties, Type,
    update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::manifest_merge::{
    ManifestMergeManager, SnapshotManifest, UnwrittenManifest,
};
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
/// re-emitted with [`ManifestStatus::Existing`]. A rewritten manifest is
/// handed back unwritten ([`UnwrittenManifest`]): the commit writes it on
/// its own, or its entries into the manifest it is merged into.
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

/// A file a commit removed, and the partition spec of the manifest that
/// listed it.
pub(crate) struct RemovedFile {
    file: DataFile,
    partition_spec_id: i32,
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
    /// set. Returns the surviving manifests, the ones that listed a file to
    /// delete unwritten, plus every file removed.
    pub(crate) async fn filter_manifests(
        &self,
        table: &Table,
        manifests: Vec<ManifestFile>,
    ) -> Result<(Vec<SnapshotManifest>, Vec<RemovedFile>)> {
        if self.deleted_files.is_empty() {
            let manifests = manifests.into_iter().map(SnapshotManifest::Written);
            return Ok((manifests.collect(), Vec::new()));
        }

        let mut removed_files = Vec::new();
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
        let mut filtered: HashMap<usize, Option<UnwrittenManifest>> = HashMap::new();
        // What encodes the entries of the manifests of each partition spec:
        // none for a spec the table can write no manifest under.
        let mut codecs: HashMap<i32, Option<Arc<EntryCodec>>> = HashMap::new();
        for candidates in [named, others] {
            let reads: Vec<_> = candidates
                .into_iter()
                .map(|position| {
                    let partition_spec_id = manifests[position].partition_spec_id;
                    let codec = codecs
                        .entry(partition_spec_id)
                        .or_insert_with(|| {
                            EntryCodec::of_table(table, partition_spec_id).map(Arc::new)
                        })
                        .clone();
                    read_candidate(table, &manifests[position], position, codec)
                })
                .collect();
            let mut reads = stream::iter(reads).buffered(MANIFEST_READ_CONCURRENCY);
            while found_paths.len() < self.deleted_files.len() {
                let Some((position, entries)) = reads.next().await else {
                    break;
                };
                let manifest_file = &manifests[position];
                let (entries, codec) = match entries? {
                    CandidateEntries::Encoded(entries, codec) => (entries, codec),
                    CandidateEntries::Decoded(manifest) => {
                        let has_deletes = manifest.entries().iter().any(|e| {
                            e.is_alive() && self.deleted_files.contains_key(e.file_path())
                        });
                        if !has_deletes {
                            continue;
                        }
                        // After partition evolution, old manifests may carry
                        // a non-default spec: the rewritten manifest keeps
                        // the manifest's own spec, which the table must
                        // still be able to write under.
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Manifest references unknown partition spec {}",
                                manifest_file.partition_spec_id
                            ),
                        ));
                    }
                };

                // Check whether this manifest contains any files we want to delete.
                let has_deletes = entries
                    .iter()
                    .any(|e| e.is_alive() && self.deleted_files.contains_key(e.file_path()));

                if !has_deletes {
                    // Manifest is unaffected — pass through verbatim.
                    continue;
                }

                // Rewrite: keep surviving entries as EXISTING, and record the
                // removed ones as DELETED so that the file they name can be
                // cleaned up when this snapshot expires. Either is carried as
                // the bytes it was read as.
                let mut surviving_entries: Vec<EncodedEntry> = Vec::new();
                let mut removed_entries: Vec<EncodedEntry> = Vec::new();
                for mut entry in entries {
                    if !entry.is_alive() {
                        // Already-deleted entries (status == Deleted) are dropped.
                        continue;
                    }
                    if self.deleted_files.contains_key(entry.file_path()) {
                        found_paths.insert(entry.file_path().to_string());
                        removed_files.push(RemovedFile {
                            file: codec.decode(&entry)?.data_file,
                            partition_spec_id: manifest_file.partition_spec_id,
                        });
                        // The manifest writer stamps the removing snapshot
                        // on the entry and keeps the file's own sequence
                        // numbers, which is what an expiring reader needs to
                        // attribute the removal.
                        entry.status = ManifestStatus::Deleted;
                        removed_entries.push(entry);
                    } else {
                        // Surviving entry — re-emit as EXISTING with original ids preserved.
                        if entry.sequence_number().is_none() {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Manifest entry for {} is missing sequence_number",
                                    entry.file_path()
                                ),
                            ));
                        }
                        entry.status = ManifestStatus::Existing;
                        entry.snapshot_id = Some(
                            entry
                                .snapshot_id()
                                .unwrap_or(manifest_file.added_snapshot_id),
                        );
                        surviving_entries.push(entry);
                    }
                }

                if surviving_entries.is_empty() && removed_entries.is_empty() {
                    // Nothing left to say about this manifest — omit entirely.
                    filtered.insert(position, None);
                    continue;
                }

                // The filtered manifest is about as long as the one it
                // takes the place of.
                surviving_entries.extend(removed_entries);
                filtered.insert(
                    position,
                    Some(UnwrittenManifest {
                        partition_spec_id: manifest_file.partition_spec_id,
                        entries: surviving_entries,
                        codec,
                        estimated_length: manifest_file.manifest_length.max(0) as u64,
                    }),
                );
            }
        }

        let result: Vec<SnapshotManifest> = manifests
            .into_iter()
            .enumerate()
            .filter_map(
                |(position, manifest_file)| match filtered.remove(&position) {
                    Some(replacement) => replacement.map(SnapshotManifest::Unwritten),
                    None => Some(SnapshotManifest::Written(manifest_file)),
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

        Ok((result, removed_files))
    }
}

/// The entries of a manifest being filtered.
enum CandidateEntries {
    /// As the bytes the manifest that takes its place is written from, with
    /// what encoded them.
    Encoded(Vec<EncodedEntry>, Arc<EntryCodec>),
    /// Decoded, for a manifest the table can write no manifest in place of.
    Decoded(Manifest),
}

/// Read `manifest_file`, the manifest at `position` of the manifests being
/// filtered: its entries as `codec` lays them out, or the manifest decoded
/// when the table has no codec for it.
async fn read_candidate(
    table: &Table,
    manifest_file: &ManifestFile,
    position: usize,
    codec: Option<Arc<EntryCodec>>,
) -> (usize, Result<CandidateEntries>) {
    let entries = match codec {
        Some(codec) => EncodedManifest::load_entries(table, manifest_file, &codec, false)
            .await
            .map(|entries| CandidateEntries::Encoded(entries, codec)),
        None => table
            .manifest_reader()
            .read(manifest_file)
            .await
            .map(CandidateEntries::Decoded),
    };
    (position, entries)
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

impl PreparedManifests {
    /// The manifests of the base that the manifests written ahead take the
    /// place of.
    pub(crate) fn replaced_manifest_paths(&self) -> impl Iterator<Item = &str> {
        let kept_manifest_paths: HashSet<&str> = self
            .manifests
            .iter()
            .map(|manifest| manifest.manifest_path.as_str())
            .collect();
        self.base_manifest_paths
            .iter()
            .map(String::as_str)
            .filter(move |path| !kept_manifest_paths.contains(path))
    }

    /// The manifests the snapshot lists on top of `table`, or `None` when
    /// the manifests written ahead no longer stand on it.
    async fn list_on(&self, table: &Table) -> Result<Option<Vec<ManifestFile>>> {
        let metadata = table.metadata();
        if metadata.current_schema_id() != self.schema_id
            || metadata.default_partition_spec_id() != self.default_partition_spec_id
            || metadata.snapshot_by_id(self.snapshot_id).is_some()
        {
            return Ok(None);
        }
        let Some(snapshot) = metadata.current_snapshot() else {
            return Ok(None);
        };
        let manifest_list = table.manifest_list_reader(snapshot).load().await?;
        let current_manifest_paths: HashSet<&str> = manifest_list
            .entries()
            .iter()
            .map(|manifest| manifest.manifest_path.as_str())
            .collect();
        if self
            .replaced_manifest_paths()
            .any(|path| !current_manifest_paths.contains(path))
        {
            return Ok(None);
        }

        let gained_manifests = manifest_list
            .entries()
            .iter()
            .filter(|manifest| !self.base_manifest_paths.contains(&manifest.manifest_path))
            .filter(|manifest| manifest.has_added_files() || manifest.has_existing_files());
        let standing_manifests = self.manifests.iter().filter(|manifest| {
            !self.base_manifest_paths.contains(&manifest.manifest_path)
                || current_manifest_paths.contains(manifest.manifest_path.as_str())
        });
        Ok(Some(
            gained_manifests
                .chain(standing_manifests)
                .cloned()
                .collect(),
        ))
    }
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
    /// The manifests of the snapshot written ahead of the commit, when they
    /// were ([`prepare`](Self::prepare)).
    prepared: Option<PreparedManifests>,
    /// Whether an attempt has taken the prepared manifests. An attempt that
    /// does not commit deletes what it recorded as written, them included,
    /// so only the first attempt may list them.
    is_prepared_taken: AtomicBool,
}

/// The manifests of a snapshot, written ahead of its commit on top of the
/// snapshot the table had then, its base.
///
/// A commit lists them as they are when the table still lists every manifest
/// of the base they took the place of, and still has the schema and the
/// default partition spec they were written under. The manifests the table
/// gained since are listed first, and a manifest of the base the table no
/// longer lists is left out: whatever replaced it is among those gained.
/// Otherwise the commit writes the snapshot's manifests afresh.
pub(crate) struct PreparedManifests {
    snapshot_id: i64,
    schema_id: i32,
    default_partition_spec_id: i32,
    /// The manifests the base listed.
    base_manifest_paths: HashSet<String>,
    /// The manifests the snapshot lists on top of the base.
    manifests: Vec<ManifestFile>,
    removed_files: Vec<RemovedFile>,
    /// The manifests written to prepare.
    written_manifest_paths: Vec<String>,
    /// The files the snapshot removes and adds, which the commit that takes
    /// the manifests has to remove and add as well.
    deleted_paths: BTreeSet<String>,
    added_paths: BTreeSet<String>,
}

/// The manifests a snapshot lists, as [`MergingSnapshotProducer`] produced
/// them on top of a table.
struct ProducedManifests {
    /// The manifests the table's current snapshot lists.
    base_manifest_paths: HashSet<String>,
    manifests: Vec<ManifestFile>,
    removed_files: Vec<RemovedFile>,
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
            prepared: None,
            is_prepared_taken: AtomicBool::new(false),
        }
    }

    /// Commit with `prepared`, the manifests written ahead for the files
    /// this producer removes and adds.
    pub(crate) fn set_prepared(&mut self, prepared: PreparedManifests) {
        self.prepared = Some(prepared);
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
    ///
    /// The manifests written ahead are listed when they still stand on the
    /// table ([`PreparedManifests`]); they are recorded in `written_files`
    /// either way.
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

        if let Some(prepared) = &self.prepared
            && !self.is_prepared_taken.swap(true, Ordering::Relaxed)
        {
            written_files.record_manifests(&prepared.written_manifest_paths);
            self.validate_prepared_for_same_files(prepared)?;
            if let Some(manifests) = prepared.list_on(table).await? {
                let summary = self.build_summary(table, &prepared.removed_files)?;
                return snapshot_producer
                    .with_snapshot_id(prepared.snapshot_id)
                    .commit_with_manifests(manifests, summary)
                    .await;
            }
        }

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }
        let produced = self
            .produce_manifests(table, snapshot_producer.snapshot_id, written_files)
            .await?;
        let summary = self.build_summary(table, &produced.removed_files)?;
        snapshot_producer
            .commit_with_manifests(produced.manifests, summary)
            .await
    }

    /// Write the manifests of the snapshot ahead of its commit, on top of
    /// the snapshot `table` has now.
    pub(crate) async fn prepare(&self, table: &Table) -> Result<PreparedManifests> {
        self.validate_data_sequence_number(table)?;
        let written_files = WrittenFiles::default();
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid,
            HashMap::new(),
            self.added_data_files.clone(),
            &written_files,
        );
        snapshot_producer.validate_added_data_files()?;
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }
        let snapshot_id = snapshot_producer.snapshot_id;
        let produced = self
            .produce_manifests(table, snapshot_id, &written_files)
            .await?;
        Ok(PreparedManifests {
            snapshot_id,
            schema_id: table.metadata().current_schema_id(),
            default_partition_spec_id: table.metadata().default_partition_spec_id(),
            base_manifest_paths: produced.base_manifest_paths,
            manifests: produced.manifests,
            removed_files: produced.removed_files,
            written_manifest_paths: written_files.collect_manifest_paths(),
            deleted_paths: self.collect_deleted_paths(),
            added_paths: self.collect_added_paths(),
        })
    }

    fn collect_deleted_paths(&self) -> BTreeSet<String> {
        self.deleted_data_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect()
    }

    fn collect_added_paths(&self) -> BTreeSet<String> {
        self.added_data_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect()
    }

    /// Refuse `prepared` when it was written for other files than this
    /// producer removes and adds.
    fn validate_prepared_for_same_files(&self, prepared: &PreparedManifests) -> Result<()> {
        if prepared.deleted_paths != self.collect_deleted_paths()
            || prepared.added_paths != self.collect_added_paths()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The manifests written ahead are of other files than the commit removes and adds",
            ));
        }
        Ok(())
    }

    /// The manifests the snapshot `snapshot_id` lists on top of `table`,
    /// each one written.
    async fn produce_manifests(
        &self,
        table: &Table,
        snapshot_id: i64,
        written_files: &WrittenFiles,
    ) -> Result<ProducedManifests> {
        // 1. Load existing manifests from the current snapshot.
        let existing_manifests: Vec<ManifestFile> = match table.metadata().current_snapshot() {
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
        let base_manifest_paths = existing_manifests
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect();

        // 2. Filter existing manifests — remove deleted file entries.
        let (filtered_manifests, removed_files) = self
            .filter_manager
            .filter_manifests(table, existing_manifests)
            .await?;

        // 3. A new manifest for added files.
        let mut added_manifests = Vec::new();
        if !self.added_data_files.is_empty() {
            added_manifests.push(SnapshotManifest::Unwritten(
                self.build_added_manifest(table, snapshot_id)?,
            ));
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
            ManifestMergeManager::list_unmerged(
                table,
                snapshot_id,
                added_manifests,
                filtered_manifests,
                || self.generate_manifest_file_path(table),
                written_files,
            )
            .await?
        };
        Ok(ProducedManifests {
            base_manifest_paths,
            manifests,
            removed_files,
        })
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

    /// The manifest of the added files, under the default partition spec,
    /// not written yet.
    fn build_added_manifest(&self, table: &Table, snapshot_id: i64) -> Result<UnwrittenManifest> {
        /// About what one added file takes in a manifest.
        const ESTIMATED_ENTRY_BYTES: u64 = 512;

        let format_version = table.metadata().format_version();
        let partition_spec_id = table.metadata().default_partition_spec_id();
        let codec = Arc::new(
            EntryCodec::of_table(table, partition_spec_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "The default partition spec does not bind to the table's schema",
                )
            })?,
        );
        let entries = self
            .added_data_files
            .iter()
            .map(|data_file| {
                let entry_builder = ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(data_file.clone())
                    // A sequence number below zero is none: the file
                    // inherits the snapshot's.
                    .sequence_number_opt(
                        self.data_sequence_number
                            .filter(|sequence_number| *sequence_number >= 0),
                    );
                let entry = if format_version == FormatVersion::V1 {
                    // V1 requires snapshot_id on each entry.
                    entry_builder.snapshot_id(snapshot_id).build()
                } else {
                    entry_builder.build()
                };
                codec.encode(entry)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(UnwrittenManifest {
            partition_spec_id,
            entries,
            codec,
            estimated_length: self.added_data_files.len() as u64 * ESTIMATED_ENTRY_BYTES,
        })
    }

    fn build_summary(&self, table: &Table, removed_files: &[RemovedFile]) -> Result<Summary> {
        let table_metadata = table.metadata_ref();
        let schema = table_metadata.current_schema().clone();
        let partition_spec = table_metadata.default_partition_spec().clone();

        // Add to the collector of the removed files rather than merging two:
        // `merge` drops partition metrics unless both sides trust them, and
        // neither does.
        let mut collector = SnapshotSummaryCollector::default();
        for removed in removed_files {
            let removed_spec = table_metadata
                .partition_spec_by_id(removed.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Manifest references unknown partition spec {}",
                            removed.partition_spec_id
                        ),
                    )
                })?;
            collector.remove_file(&removed.file, schema.clone(), removed_spec.clone());
        }
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
