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

use std::mem::size_of_val;
use std::sync::Arc;

use crate::encryption::EncryptionManager;
use crate::io::FileIO;
use crate::spec::{
    FieldSummary, FormatVersion, Manifest, ManifestEntry, ManifestFile, ManifestList,
    ManifestListReader, ManifestReader, PrimitiveLiteral, SchemaId, SnapshotRef, TableMetadataRef,
};
use crate::{Error, ErrorKind, Result};

const DEFAULT_CACHE_SIZE_BYTES: u64 = 32 * 1024 * 1024; // 32MB

#[derive(Clone, Debug)]
pub(crate) enum CachedItem {
    ManifestList(Arc<ManifestList>),
    Manifest(Arc<Manifest>),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum CachedObjectKey {
    ManifestList((String, FormatVersion, SchemaId)),
    // The manifest-level `first_row_id` is part of the key because the parsed
    // manifest inherits it onto its entries: the same physical manifest can be
    // referenced with different offsets across snapshots and branches, so it
    // cannot be shared under the path alone.
    Manifest((String, Option<u64>)),
}

/// Caches metadata objects deserialized from immutable files.
///
/// A cache holds no file IO of its own: a lookup reads a miss through the
/// [`FileIO`] it is given, so one cache can serve every table of a process,
/// tables whose file IO carry different credentials included. A table builds
/// a cache of its own unless given one to share
/// ([`TableBuilder::object_cache`](crate::table::TableBuilder::object_cache)).
///
/// The capacity is in bytes of parsed objects: an object weighs what its
/// parsed form holds, estimated from its entries, their paths, partition
/// values, bounds and counts.
#[derive(Clone, Debug)]
pub struct ObjectCache {
    cache: moka::future::Cache<CachedObjectKey, CachedItem>,
    cache_disabled: bool,
}

impl Default for ObjectCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectCache {
    /// Creates a new [`ObjectCache`]
    /// with the default cache size
    pub fn new() -> Self {
        Self::new_with_capacity(DEFAULT_CACHE_SIZE_BYTES)
    }

    /// Creates a new [`ObjectCache`] holding at most `cache_size_bytes` of
    /// parsed objects. A capacity of zero disables caching.
    pub fn new_with_capacity(cache_size_bytes: u64) -> Self {
        if cache_size_bytes == 0 {
            Self::with_disabled_cache()
        } else {
            Self {
                cache: moka::future::Cache::builder()
                    .weigher(|_, val: &CachedItem| weigh(val))
                    .max_capacity(cache_size_bytes)
                    .build(),
                cache_disabled: false,
            }
        }
    }

    /// Creates a new [`ObjectCache`]
    /// with caching disabled
    pub fn with_disabled_cache() -> Self {
        Self {
            cache: moka::future::Cache::new(0),
            cache_disabled: true,
        }
    }

    /// Retrieves an Arc [`Manifest`] from the cache, or reads it through
    /// `file_io` and parses it if not present
    pub async fn get_manifest(
        &self,
        manifest_file: &ManifestFile,
        file_io: &FileIO,
    ) -> Result<Arc<Manifest>> {
        if self.cache_disabled {
            return ManifestReader::new(file_io.clone())
                .read(manifest_file)
                .await
                .map(Arc::new);
        }

        let key = CachedObjectKey::Manifest((
            manifest_file.manifest_path.clone(),
            manifest_file.first_row_id,
        ));

        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(Self::fetch_and_parse_manifest(manifest_file, file_io))
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to load manifest {}", manifest_file.manifest_path),
                )
                .with_source(err)
            })?
            .into_value();

        match cache_entry {
            CachedItem::Manifest(arc_manifest) => Ok(arc_manifest),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for key '{key:?}' is not a Manifest"),
            )),
        }
    }

    /// Retrieves an Arc [`ManifestList`] from the cache, or reads it through
    /// `file_io` and parses it if not present, decrypting it with
    /// `encryption_manager` when the table is encrypted
    pub async fn get_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
        file_io: &FileIO,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> Result<Arc<ManifestList>> {
        if self.cache_disabled {
            return ManifestListReader::new(
                snapshot.clone(),
                file_io.clone(),
                table_metadata.clone(),
                encryption_manager,
            )
            .load()
            .await
            .map(Arc::new);
        }

        let key = CachedObjectKey::ManifestList((
            snapshot.manifest_list().to_string(),
            table_metadata.format_version,
            snapshot.schema_id().unwrap(),
        ));
        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(Self::fetch_and_parse_manifest_list(
                snapshot,
                table_metadata,
                file_io,
                encryption_manager,
            ))
            .await
            .map_err(|err| {
                Arc::try_unwrap(err).unwrap_or_else(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to load manifest list in cache",
                    )
                    .with_source(err)
                })
            })?
            .into_value();

        match cache_entry {
            CachedItem::ManifestList(arc_manifest_list) => Ok(arc_manifest_list),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for path '{key:?}' is not a manifest list"),
            )),
        }
    }

    async fn fetch_and_parse_manifest(
        manifest_file: &ManifestFile,
        file_io: &FileIO,
    ) -> Result<CachedItem> {
        let manifest = ManifestReader::new(file_io.clone())
            .read(manifest_file)
            .await?;

        Ok(CachedItem::Manifest(Arc::new(manifest)))
    }

    async fn fetch_and_parse_manifest_list(
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
        file_io: &FileIO,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> Result<CachedItem> {
        let manifest_list = ManifestListReader::new(
            snapshot.clone(),
            file_io.clone(),
            table_metadata.clone(),
            encryption_manager,
        )
        .load()
        .await?;

        Ok(CachedItem::ManifestList(Arc::new(manifest_list)))
    }
}

/// What a map entry costs beyond its key and value: the table's control
/// bytes and the slack of its load factor, taken as the entry over again.
const MAP_ENTRY_SLACK_FACTOR: usize = 2;

/// The weight of a cached object: the bytes its parsed form holds, estimated,
/// saturated at the weigher's range.
fn weigh(item: &CachedItem) -> u32 {
    let bytes = match item {
        CachedItem::ManifestList(list) => estimate_manifest_list_size(list),
        CachedItem::Manifest(manifest) => estimate_manifest_size(manifest),
    };
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

fn estimate_manifest_list_size(list: &ManifestList) -> usize {
    size_of::<ManifestList>()
        + list
            .entries()
            .iter()
            .map(estimate_manifest_file_size)
            .sum::<usize>()
}

fn estimate_manifest_file_size(file: &ManifestFile) -> usize {
    let summaries = file.partitions.as_deref().unwrap_or_default();
    size_of::<ManifestFile>()
        + file.manifest_path.len()
        + summaries
            .iter()
            .map(|summary| {
                size_of::<FieldSummary>()
                    + summary.lower_bound.as_ref().map_or(0, |bound| bound.len())
                    + summary.upper_bound.as_ref().map_or(0, |bound| bound.len())
            })
            .sum::<usize>()
        + file.key_metadata.as_ref().map_or(0, Vec::len)
}

fn estimate_manifest_size(manifest: &Manifest) -> usize {
    size_of::<Manifest>()
        + manifest
            .entries()
            .iter()
            .map(|entry| estimate_manifest_entry_size(entry))
            .sum::<usize>()
}

fn estimate_manifest_entry_size(entry: &ManifestEntry) -> usize {
    let data_file = entry.data_file();
    let counts = data_file.column_sizes().len()
        + data_file.value_counts().len()
        + data_file.null_value_counts().len()
        + data_file.nan_value_counts().len();
    let bounds = data_file
        .lower_bounds()
        .values()
        .chain(data_file.upper_bounds().values())
        .map(|bound| {
            size_of::<(i32, crate::spec::Datum)>() * MAP_ENTRY_SLACK_FACTOR
                + estimate_literal_heap_size(bound.literal())
        })
        .sum::<usize>();
    size_of::<ManifestEntry>()
        + data_file.file_path().len()
        + size_of_val(data_file.partition().fields())
        + counts * size_of::<(i32, u64)>() * MAP_ENTRY_SLACK_FACTOR
        + bounds
        + data_file.key_metadata().map_or(0, <[u8]>::len)
        + data_file.split_offsets().map_or(0, size_of_val)
        + data_file
            .equality_ids()
            .map_or(0, |ids| ids.len() * size_of::<i32>())
        + data_file
            .referenced_data_file()
            .map_or(0, |path| path.len())
}

fn estimate_literal_heap_size(literal: &PrimitiveLiteral) -> usize {
    match literal {
        PrimitiveLiteral::String(string) => string.len(),
        PrimitiveLiteral::Binary(bytes) => bytes.len(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use minijinja::value::Value;
    use minijinja::{AutoEscape, Environment, context};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::TableIdent;
    use crate::io::{FileIO, OutputFile};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Struct, TableMetadata,
    };
    use crate::table::Table;
    use crate::test_utils::test_runtime;

    fn render_template(template: &str, ctx: Value) -> String {
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_| AutoEscape::None);
        env.render_str(template, ctx).unwrap()
    }

    struct TableTestFixture {
        table_location: String,
        table: Table,
    }

    impl TableTestFixture {
        fn new() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io = FileIO::new_with_fs();

            let table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let metadata_json = render_template(&template_json_str, context! {
                    table_location => &table_location,
                    manifest_list_1_location => &manifest_list1_location,
                    manifest_list_2_location => &manifest_list2_location,
                    table_metadata_1_location => &table_metadata1_location,
                });
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
                .runtime(test_runtime())
                .build()
                .unwrap();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        fn next_manifest_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    self.table_location,
                    Uuid::new_v4()
                ))
                .unwrap()
        }

        async fn setup_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = self.table.metadata().default_partition_spec();

            // Write data files
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_data();
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/1.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(100))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            let data_file_manifest = writer.write_manifest_file().await.unwrap();

            // Write to manifest list
            let manifest_list_writer = self
                .table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap();
            let mut manifest_list_write = ManifestListWriter::v2(
                manifest_list_writer,
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_disabled_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::with_disabled_cache();

        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
                fixture.table.file_io(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        let manifest_file = result_manifest_list.entries().first().unwrap();
        let result_manifest = object_cache
            .get_manifest(manifest_file, fixture.table.file_io())
            .await
            .unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_default_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::new();

        // not in cache
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
                fixture.table.file_io(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        // retrieve cached version
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
                fixture.table.file_io(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        let manifest_file = result_manifest_list.entries().first().unwrap();

        // not in cache
        let result_manifest = object_cache
            .get_manifest(manifest_file, fixture.table.file_io())
            .await
            .unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );

        // retrieve cached version
        let result_manifest = object_cache
            .get_manifest(manifest_file, fixture.table.file_io())
            .await
            .unwrap();

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "1.parquet"
        );
    }

    #[tokio::test]
    async fn test_cache_shared_between_tables_serves_one_tables_load_to_another() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;
        let shared = Arc::new(ObjectCache::new());
        let first = fixture.table.clone().with_object_cache(shared.clone());
        let second = fixture.table.clone().with_object_cache(shared.clone());
        let snapshot = fixture.table.metadata().current_snapshot().unwrap();

        let loaded_by_first = first
            .object_cache()
            .get_manifest_list(snapshot, &first.metadata_ref(), first.file_io(), None)
            .await
            .unwrap();
        let served_to_second = second
            .object_cache()
            .get_manifest_list(snapshot, &second.metadata_ref(), second.file_io(), None)
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&loaded_by_first, &served_to_second));
    }

    #[tokio::test]
    async fn test_manifest_weighs_its_entries() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;
        let object_cache = ObjectCache::new();
        let snapshot = fixture.table.metadata().current_snapshot().unwrap();
        let manifest_list = object_cache
            .get_manifest_list(
                snapshot,
                &fixture.table.metadata_ref(),
                fixture.table.file_io(),
                None,
            )
            .await
            .unwrap();
        let manifest = object_cache
            .get_manifest(&manifest_list.entries()[0], fixture.table.file_io())
            .await
            .unwrap();

        let weight = weigh(&CachedItem::Manifest(manifest.clone()));

        let entry = manifest.entries()[0].data_file();
        let at_least = size_of::<Manifest>() + size_of::<ManifestEntry>() + entry.file_path().len();
        assert!(weight as usize >= at_least, "{weight} < {at_least}");
    }

    #[tokio::test]
    async fn test_get_manifest_keys_on_first_row_id() {
        use crate::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SchemaRef, Type};

        let io = FileIO::new_with_memory();
        let path = "memory:///metadata/first_row_id_manifest.avro";

        let schema: SchemaRef = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
            .into();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        let mut writer = ManifestWriterBuilder::new(
            io.new_output(path).unwrap(),
            Some(1),
            schema,
            partition_spec,
        )
        .build_v3_data();
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .content(DataContentType::Data)
                            .file_path(path.to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(100)
                            .record_count(1)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let manifest_file = writer.write_manifest_file().await.unwrap();

        // Two manifest-list references to the same physical manifest carrying
        // different offsets, as time travel or two branches would produce.
        let mut manifest_file_a = manifest_file.clone();
        manifest_file_a.first_row_id = Some(1000);
        let mut manifest_file_b = manifest_file;
        manifest_file_b.first_row_id = Some(2000);

        let object_cache = ObjectCache::new();

        let manifest_a = object_cache
            .get_manifest(&manifest_file_a, &io)
            .await
            .unwrap();
        let manifest_b = object_cache
            .get_manifest(&manifest_file_b, &io)
            .await
            .unwrap();

        // A cache keyed on path alone would serve manifest_a's inherited ids for
        // the second read; keying on first_row_id keeps them distinct.
        assert_eq!(
            manifest_a.entries()[0].data_file().first_row_id(),
            Some(1000)
        );
        assert_eq!(
            manifest_b.entries()[0].data_file().first_row_id(),
            Some(2000)
        );
    }
}
