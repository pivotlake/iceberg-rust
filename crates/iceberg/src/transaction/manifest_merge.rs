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

//! Merging of small data manifests while a snapshot is produced.
//!
//! [`ManifestMergeManager`] is the Rust equivalent of Java's
//! `ManifestMergeManager`: every commit adds a manifest, and once enough small
//! ones have accumulated they are bin-packed into manifests of a target size,
//! so that the manifest list of a frequently written table stays short.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;

use futures::future::try_join_all;

use crate::error::Result;
use crate::spec::{
    Manifest, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus, TableProperties,
};
use crate::table::Table;
use crate::transaction::merging::new_manifest_writer;
use crate::{Error, ErrorKind};

/// Bin-packs the data manifests of a snapshot being produced.
///
/// The rules follow Java's `ManifestMergeManager`:
///
/// - The manifests the commit wrote for the files it adds come first, then the
///   manifests carried from the parent snapshot in manifest-list order. A
///   carried manifest without a live file is dropped unless the commit itself
///   wrote it.
/// - Only data manifests are merged; delete manifests are listed after them,
///   untouched.
/// - Data manifests are grouped by partition spec id, highest first; manifests
///   of different specs are never combined.
/// - A group is packed by manifest length from its end, the oldest manifest,
///   into bins of at most `target_size_bytes`, so the bin left under-filled is
///   the one at the front. A manifest larger than the target is a bin of its
///   own.
/// - A bin of one manifest is listed untouched. So is the bin holding the very
///   first manifest while it has fewer than `min_count_to_merge` manifests, so
///   that a commit does not rewrite the newest manifests every time. Every
///   other bin is rewritten as one manifest.
pub(crate) struct ManifestMergeManager {
    merge_enabled: bool,
    target_size_bytes: u64,
    min_count_to_merge: usize,
}

impl ManifestMergeManager {
    /// Create a manager from the table's `commit.manifest-merge.enabled`,
    /// `commit.manifest.target-size-bytes` and
    /// `commit.manifest.min-count-to-merge` properties.
    pub(crate) fn try_from_table(table: &Table) -> Result<Self> {
        let properties = table.metadata().properties();
        Ok(Self {
            merge_enabled: parse_property(
                properties,
                TableProperties::PROPERTY_COMMIT_MANIFEST_MERGE_ENABLED,
                TableProperties::PROPERTY_COMMIT_MANIFEST_MERGE_ENABLED_DEFAULT,
            )?,
            target_size_bytes: parse_property(
                properties,
                TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES,
                TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
            )?,
            min_count_to_merge: parse_property(
                properties,
                TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE,
                TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE_DEFAULT,
            )?,
        })
    }

    /// Returns the manifests the snapshot `snapshot_id`, being produced on top
    /// of `table`, lists.
    ///
    /// `new_manifests` are the manifests the commit wrote for the files it
    /// adds, `carried_manifests` the manifests of the parent snapshot the
    /// commit keeps, some of which it may have rewritten. A manifest that is
    /// not merged is returned as the same [`ManifestFile`], so earlier
    /// snapshots keep sharing it. `generate_manifest_path` names each manifest
    /// written.
    ///
    /// With merging disabled by the table, the carried manifests are returned
    /// as they are, followed by the new ones.
    pub(crate) async fn merge_manifests(
        &self,
        table: &Table,
        snapshot_id: i64,
        new_manifests: Vec<ManifestFile>,
        mut carried_manifests: Vec<ManifestFile>,
        mut generate_manifest_path: impl FnMut() -> Result<String> + Send,
    ) -> Result<Vec<ManifestFile>> {
        if !self.merge_enabled {
            carried_manifests.extend(new_manifests);
            return Ok(carried_manifests);
        }

        let (data_manifests, delete_manifests): (Vec<ManifestFile>, Vec<ManifestFile>) =
            new_manifests
                .into_iter()
                .chain(carried_manifests.into_iter().filter(|manifest| {
                    manifest.has_added_files()
                        || manifest.has_existing_files()
                        || manifest.added_snapshot_id == snapshot_id
                }))
                .partition(|manifest| manifest.content == ManifestContentType::Data);

        let mut positions_by_spec_id: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        for (position, manifest) in data_manifests.iter().enumerate() {
            positions_by_spec_id
                .entry(manifest.partition_spec_id)
                .or_default()
                .push(position);
        }

        let mut merged_manifests = Vec::with_capacity(data_manifests.len());
        for positions in positions_by_spec_id.into_values().rev() {
            let lengths: Vec<u64> = positions
                .iter()
                .map(|position| data_manifests[*position].manifest_length.max(0) as u64)
                .collect();
            for bin in pack_from_end(&lengths, self.target_size_bytes) {
                let holds_first_manifest = positions[bin.start] == 0;
                let bin: Vec<&ManifestFile> = positions[bin]
                    .iter()
                    .map(|position| &data_manifests[*position])
                    .collect();
                let is_below_min_count =
                    holds_first_manifest && bin.len() < self.min_count_to_merge;
                if bin.len() == 1 || is_below_min_count {
                    merged_manifests.extend(bin.into_iter().cloned());
                    continue;
                }
                match self
                    .merge_bin(table, snapshot_id, &bin, &mut generate_manifest_path)
                    .await?
                {
                    Some(merged_manifest) => merged_manifests.push(merged_manifest),
                    None => merged_manifests.extend(bin.into_iter().cloned()),
                }
            }
        }

        merged_manifests.extend(delete_manifests);
        Ok(merged_manifests)
    }

    /// Rewrite the manifests of `bin`, which share a partition spec, as one
    /// manifest.
    ///
    /// Returns `None`, leaving the bin as it is, when the merged manifest
    /// cannot be written: the table no longer has the spec, the spec no longer
    /// binds to the current schema, or a carried entry has no data sequence
    /// number to state.
    async fn merge_bin(
        &self,
        table: &Table,
        snapshot_id: i64,
        bin: &[&ManifestFile],
        generate_manifest_path: &mut (impl FnMut() -> Result<String> + Send),
    ) -> Result<Option<ManifestFile>> {
        let schema = table.metadata().current_schema();
        let Some(partition_spec) = table
            .metadata()
            .partition_spec_by_id(bin[0].partition_spec_id)
            .filter(|spec| spec.partition_type(schema).is_ok())
        else {
            return Ok(None);
        };

        let inputs = try_join_all(
            bin.iter()
                .map(|manifest_file| read_merge_input(table, snapshot_id, manifest_file)),
        )
        .await?;
        let mut merged_entries = Vec::new();
        for entry in inputs.iter().flat_map(|manifest| manifest.entries()) {
            let Some(merged_entry) = to_merged_entry(entry, snapshot_id) else {
                continue;
            };
            if merged_entry.status != ManifestStatus::Added
                && merged_entry.sequence_number.is_none()
            {
                return Ok(None);
            }
            merged_entries.push(merged_entry);
        }

        let mut writer = new_manifest_writer(
            table,
            table.file_io().new_output(generate_manifest_path()?)?,
            Some(snapshot_id),
            ManifestContentType::Data,
            schema.clone(),
            partition_spec.as_ref().clone(),
        )?;
        for entry in merged_entries {
            match entry.status {
                ManifestStatus::Added => writer.add_entry(entry)?,
                ManifestStatus::Existing => writer.add_existing_entry(entry)?,
                ManifestStatus::Deleted => writer.add_delete_entry(entry)?,
            }
        }
        writer.write_manifest_file().await.map(Some)
    }
}

/// Pack items of the given `lengths` into bins of at most `target_size_bytes`,
/// each a range of positions, keeping the items in order.
///
/// The items are taken from the last to the first, and a bin is closed when
/// the next item does not fit, so the bin left under-filled is the first one.
/// An item longer than the target is a bin of its own.
fn pack_from_end(lengths: &[u64], target_size_bytes: u64) -> Vec<Range<usize>> {
    let mut bins = Vec::new();
    let mut bin_end = lengths.len();
    let mut bin_size_bytes = 0u64;
    for (position, length) in lengths.iter().enumerate().rev() {
        let is_bin_empty = position + 1 == bin_end;
        if !is_bin_empty && bin_size_bytes.saturating_add(*length) > target_size_bytes {
            bins.push(position + 1..bin_end);
            bin_end = position + 1;
            bin_size_bytes = 0;
        }
        bin_size_bytes = bin_size_bytes.saturating_add(*length);
    }
    if bin_end > 0 {
        bins.push(0..bin_end);
    }
    bins.reverse();
    bins
}

/// Read `manifest_file` with everything its entries inherit resolved: the
/// snapshot that added a file, its sequence numbers and, on a table with row
/// lineage, its `first_row_id`.
///
/// A manifest the commit in progress wrote is not in a manifest list yet and
/// has no `first_row_id` of its own, so its entries are read with the
/// `first_row_id` they state.
async fn read_merge_input(
    table: &Table,
    snapshot_id: i64,
    manifest_file: &ManifestFile,
) -> Result<Manifest> {
    if manifest_file.added_snapshot_id == snapshot_id {
        table
            .manifest_reader()
            .read_uncommitted(manifest_file)
            .await
    } else {
        table.manifest_reader().read(manifest_file).await
    }
}

/// `entry`, read by [`read_merge_input`], as the merged manifest written by
/// the snapshot `snapshot_id` carries it; `None` when it is not carried.
///
/// - A file the snapshot adds stays `ADDED`. Its `first_row_id` is left to be
///   inherited from the merged manifest.
/// - A file the snapshot removes stays `DELETED`; a removal by an earlier
///   snapshot is recorded by that snapshot's own manifests and is dropped.
/// - Every other file is `EXISTING`, with the snapshot that added it, its
///   sequence numbers and its `first_row_id` stated, since the merged manifest
///   is listed under another snapshot, sequence number and `first_row_id`.
fn to_merged_entry(entry: &ManifestEntry, snapshot_id: i64) -> Option<ManifestEntry> {
    let is_of_this_snapshot = entry.snapshot_id() == Some(snapshot_id);
    let mut merged_entry = entry.clone();
    match entry.status() {
        ManifestStatus::Deleted if !is_of_this_snapshot => return None,
        ManifestStatus::Deleted => {}
        ManifestStatus::Added if is_of_this_snapshot => {
            merged_entry.data_file.first_row_id = None;
        }
        ManifestStatus::Added | ManifestStatus::Existing => {
            merged_entry.status = ManifestStatus::Existing;
        }
    }
    Some(merged_entry)
}

/// The value of the table property `key`, or `default` when it is not set.
fn parse_property<T: std::str::FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
    default: T,
) -> Result<T> {
    let Some(value) = properties.get(key) else {
        return Ok(default);
    };
    value.parse().map_err(|_| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid value for table property {key}: {value}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use crate::Catalog;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{ManifestStatus, TableProperties};
    use crate::table::Table;
    use crate::transaction::tests::{
        append_files, append_files_merging_manifests, collect_live_paths, find_entry_by_path,
        load_current_entries, load_current_manifests, make_data_file,
        make_v2_minimal_table_in_catalog, make_v3_minimal_table_in_catalog, set_table_properties,
    };

    const MIN_COUNT_TO_MERGE: &str = TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE;

    /// Append one file of `records` rows at `path`, without merging manifests.
    async fn append_file(catalog: &impl Catalog, table: &Table, path: &str, records: u64) -> Table {
        let file = make_data_file(table, path, records, 100);
        append_files(catalog, table, vec![file]).await
    }

    /// Append one file of `records` rows at `path`, merging manifests.
    async fn append_file_merging_manifests(
        catalog: &impl Catalog,
        table: &Table,
        path: &str,
        records: u64,
    ) -> Table {
        let file = make_data_file(table, path, records, 100);
        append_files_merging_manifests(catalog, table, vec![file]).await
    }

    #[tokio::test]
    async fn test_merge_below_min_count_lists_manifests_untouched() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = set_table_properties(&catalog, &table, &[(MIN_COUNT_TO_MERGE, "3")]).await;
        let table = append_file(&catalog, &table, "test/1.parquet", 10).await;
        let manifests_before = load_current_manifests(&table).await;

        let table = append_file_merging_manifests(&catalog, &table, "test/2.parquet", 10).await;

        let manifests = load_current_manifests(&table).await;
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].added_files_count, Some(1));
        assert_eq!(manifests[1], manifests_before[0]);
    }

    #[tokio::test]
    async fn test_merge_at_min_count_lists_every_file_once_in_one_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = set_table_properties(&catalog, &table, &[(MIN_COUNT_TO_MERGE, "3")]).await;
        let table = append_file(&catalog, &table, "test/1.parquet", 10).await;
        let table = append_file(&catalog, &table, "test/2.parquet", 10).await;

        let table = append_file_merging_manifests(&catalog, &table, "test/3.parquet", 10).await;

        let manifests = load_current_manifests(&table).await;
        let entries = load_current_entries(&table).await;
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].added_files_count, Some(1));
        assert_eq!(manifests[0].existing_files_count, Some(2));
        assert_eq!(manifests[0].deleted_files_count, Some(0));
        assert_eq!(entries.len(), 3);
        assert_eq!(collect_live_paths(&entries), [
            "test/1.parquet",
            "test/2.parquet",
            "test/3.parquet"
        ]);
    }

    #[tokio::test]
    async fn test_merge_carries_snapshot_id_and_sequence_numbers() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = set_table_properties(&catalog, &table, &[(MIN_COUNT_TO_MERGE, "3")]).await;
        let table = append_file(&catalog, &table, "test/1.parquet", 10).await;
        let table = append_file(&catalog, &table, "test/2.parquet", 10).await;
        let entries_before = load_current_entries(&table).await;

        let table = append_file_merging_manifests(&catalog, &table, "test/3.parquet", 10).await;

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifests = load_current_manifests(&table).await;
        let entries = load_current_entries(&table).await;
        for (path, sequence_number) in [("test/1.parquet", 1), ("test/2.parquet", 2)] {
            let carried = find_entry_by_path(&entries, path);
            let original = find_entry_by_path(&entries_before, path);
            assert_eq!(carried.status(), ManifestStatus::Existing);
            assert_eq!(carried.snapshot_id(), original.snapshot_id());
            assert_ne!(carried.snapshot_id(), Some(snapshot.snapshot_id()));
            assert_eq!(carried.sequence_number(), Some(sequence_number));
            assert_eq!(carried.file_sequence_number, Some(sequence_number));
        }
        let added = find_entry_by_path(&entries, "test/3.parquet");
        assert_eq!(added.status(), ManifestStatus::Added);
        assert_eq!(added.snapshot_id(), Some(snapshot.snapshot_id()));
        assert_eq!(added.sequence_number(), Some(snapshot.sequence_number()));
        assert_eq!(manifests[0].added_snapshot_id, snapshot.snapshot_id());
        assert_eq!(manifests[0].sequence_number, snapshot.sequence_number());
        assert_eq!(manifests[0].min_sequence_number, 1);
    }

    #[tokio::test]
    async fn test_merge_keeps_first_row_ids() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_table_properties(&catalog, &table, &[(MIN_COUNT_TO_MERGE, "3")]).await;
        let table = append_file(&catalog, &table, "test/1.parquet", 10).await;
        let table = append_file(&catalog, &table, "test/2.parquet", 20).await;

        let table = append_file_merging_manifests(&catalog, &table, "test/3.parquet", 5).await;

        let manifests = load_current_manifests(&table).await;
        let entries = load_current_entries(&table).await;
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].first_row_id, Some(30));
        for (path, first_row_id) in [
            ("test/1.parquet", 0),
            ("test/2.parquet", 10),
            ("test/3.parquet", 30),
        ] {
            let data_file = find_entry_by_path(&entries, path).data_file();
            assert_eq!(data_file.first_row_id(), Some(first_row_id), "{path}");
        }
    }

    #[tokio::test]
    async fn test_merge_of_a_merged_manifest_keeps_first_row_ids() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = set_table_properties(&catalog, &table, &[(MIN_COUNT_TO_MERGE, "3")]).await;
        let table = append_file(&catalog, &table, "test/1.parquet", 10).await;
        let table = append_file(&catalog, &table, "test/2.parquet", 20).await;
        let table = append_file_merging_manifests(&catalog, &table, "test/3.parquet", 5).await;
        let table = append_file(&catalog, &table, "test/4.parquet", 7).await;
        let entries_before = load_current_entries(&table).await;

        let table = append_file_merging_manifests(&catalog, &table, "test/5.parquet", 3).await;

        let manifests = load_current_manifests(&table).await;
        let entries = load_current_entries(&table).await;
        assert_eq!(manifests.len(), 1);
        for path in [
            "test/1.parquet",
            "test/2.parquet",
            "test/3.parquet",
            "test/4.parquet",
        ] {
            let first_row_id = find_entry_by_path(&entries, path)
                .data_file()
                .first_row_id();
            let first_row_id_before = find_entry_by_path(&entries_before, path)
                .data_file()
                .first_row_id();
            assert!(first_row_id.is_some(), "{path}");
            assert_eq!(first_row_id, first_row_id_before, "{path}");
        }
    }

    #[tokio::test]
    async fn test_merge_packs_from_the_last_manifest_and_only_the_first_bin_obeys_min_count() {
        let catalog = new_memory_catalog().await;
        let mut table = make_v2_minimal_table_in_catalog(&catalog).await;
        for file_number in 1..=5 {
            let path = format!("test/{file_number}.parquet");
            table = append_file(&catalog, &table, &path, 10).await;
        }
        let manifests_before = load_current_manifests(&table).await;
        let longest_manifest_length = manifests_before
            .iter()
            .map(|manifest| manifest.manifest_length)
            .max()
            .unwrap();
        let fits_two_manifests = (longest_manifest_length * 5 / 2).to_string();
        let table = set_table_properties(&catalog, &table, &[(
            TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES,
            &fits_two_manifests,
        )])
        .await;

        let table = append_file_merging_manifests(&catalog, &table, "test/6.parquet", 10).await;

        let snapshot_id = table.metadata().current_snapshot_id().unwrap();
        let manifests = load_current_manifests(&table).await;
        let entries = load_current_entries(&table).await;
        assert_eq!(manifests.len(), 4);
        assert_eq!(manifests[0].added_files_count, Some(1));
        assert_eq!(manifests[1], manifests_before[0]);
        for merged_manifest in &manifests[2..] {
            assert_eq!(merged_manifest.added_snapshot_id, snapshot_id);
            assert_eq!(merged_manifest.existing_files_count, Some(2));
        }
        assert_eq!(collect_live_paths(&entries), [
            "test/1.parquet",
            "test/2.parquet",
            "test/3.parquet",
            "test/4.parquet",
            "test/5.parquet",
            "test/6.parquet"
        ]);
    }

    #[tokio::test]
    async fn test_merge_disabled_by_table_property_lists_manifests_untouched() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = set_table_properties(&catalog, &table, &[
            (MIN_COUNT_TO_MERGE, "2"),
            (
                TableProperties::PROPERTY_COMMIT_MANIFEST_MERGE_ENABLED,
                "false",
            ),
        ])
        .await;
        let table = append_file(&catalog, &table, "test/1.parquet", 10).await;
        let table = append_file(&catalog, &table, "test/2.parquet", 10).await;
        let manifests_before = load_current_manifests(&table).await;

        let table = append_file_merging_manifests(&catalog, &table, "test/3.parquet", 10).await;

        let manifests = load_current_manifests(&table).await;
        assert_eq!(manifests.len(), 3);
        assert_eq!(manifests[..2], manifests_before);
        assert_eq!(manifests[2].added_files_count, Some(1));
    }

    #[tokio::test]
    async fn test_append_without_manifest_merge_lists_manifests_untouched() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = set_table_properties(&catalog, &table, &[(MIN_COUNT_TO_MERGE, "2")]).await;
        let table = append_file(&catalog, &table, "test/1.parquet", 10).await;
        let table = append_file(&catalog, &table, "test/2.parquet", 10).await;
        let manifests_before = load_current_manifests(&table).await;

        let table = append_file(&catalog, &table, "test/3.parquet", 10).await;

        let manifests = load_current_manifests(&table).await;
        assert_eq!(manifests.len(), 3);
        assert_eq!(manifests[..2], manifests_before);
        assert_eq!(manifests[2].added_files_count, Some(1));
    }
}
