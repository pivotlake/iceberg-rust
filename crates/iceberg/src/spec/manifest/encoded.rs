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

//! Manifest entries kept as the bytes a manifest stores them in.
//!
//! A commit that removes files, or merges manifests, writes most entries of
//! the manifests it touches again as they were. Decoding every such entry
//! into a [`ManifestEntry`] and encoding it again costs far more than the
//! entry's bytes do to copy, so an entry is encoded once, when its file is
//! first written to a manifest or first read from one, and from then on
//! carried as an [`EncodedEntry`]: the few fields a commit changes or looks
//! at, beside the bytes of everything else. The manifests this process wrote
//! or read are held as [`EncodedManifest`]s, by path, so a later commit, or a
//! reader that wants the entries of a manifest, finds them without the
//! manifest being fetched and decoded.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};

use apache_avro::schema::Schema as AvroSchema;
use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};
use bytes::Bytes;

use super::_serde::{ManifestEntryV1, ManifestEntryV2};
use super::{
    DataContentType, ManifestEntry, ManifestMetadata, ManifestStatus, manifest_schema_v1,
    manifest_schema_v2,
};
use crate::error::Result;
use crate::spec::{
    FormatVersion, INITIAL_SEQUENCE_NUMBER, ManifestContentType, ManifestFile, SchemaRef, Struct,
    StructType, Type,
};
use crate::table::Table;
use crate::{Error, ErrorKind};

/// One entry of a manifest: what a commit reads of it or changes in it, and
/// the bytes of the rest of its data file.
///
/// The bytes are those of the layout the entry was encoded for
/// ([`EntryCodec`]); an entry is only ever written by a codec of that layout.
#[derive(Clone, Debug)]
pub struct EncodedEntry {
    pub(crate) status: ManifestStatus,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) sequence_number: Option<i64>,
    pub(crate) file_sequence_number: Option<i64>,
    /// The `first_row_id` the data file states. It changes when a file is
    /// carried from the manifest that added it, where it is inherited, so it
    /// is kept out of the bytes.
    pub(crate) first_row_id: Option<i64>,
    content: DataContentType,
    file_path: Arc<str>,
    record_count: u64,
    partition: Arc<Struct>,
    /// The data file's fields before `first_row_id`: all of them, in a
    /// layout that has no `first_row_id`.
    head: Bytes,
    /// The data file's fields after `first_row_id`.
    tail: Bytes,
}

impl EncodedEntry {
    /// Whether the file is live in the manifest's snapshot.
    pub fn is_alive(&self) -> bool {
        matches!(
            self.status,
            ManifestStatus::Added | ManifestStatus::Existing
        )
    }

    /// The entry's status.
    pub fn status(&self) -> ManifestStatus {
        self.status
    }

    /// The snapshot that added the file, or removed it for a deleted entry.
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    /// The file's data sequence number.
    pub fn sequence_number(&self) -> Option<i64> {
        self.sequence_number
    }

    /// The sequence number of the commit that added the file.
    pub fn file_sequence_number(&self) -> Option<i64> {
        self.file_sequence_number
    }

    /// The id of the file's first row.
    pub fn first_row_id(&self) -> Option<i64> {
        self.first_row_id
    }

    /// The file's full path.
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// How many rows the file holds.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// The file's partition tuple.
    pub fn partition(&self) -> &Struct {
        &self.partition
    }

    /// What kind of file the entry lists.
    pub fn content_type(&self) -> DataContentType {
        self.content
    }

    /// Take from `manifest_file`, the manifest list's entry of the manifest
    /// this entry was read from, what the entry leaves to be inherited: the
    /// snapshot id and the sequence numbers.
    pub(crate) fn inherit_data(&mut self, manifest_file: &ManifestFile) {
        if self.snapshot_id.is_none() {
            self.snapshot_id = Some(manifest_file.added_snapshot_id);
        }
        let inherits_sequence_number = self.status == ManifestStatus::Added
            || manifest_file.sequence_number == INITIAL_SEQUENCE_NUMBER;
        if self.sequence_number.is_none() && inherits_sequence_number {
            self.sequence_number = Some(manifest_file.sequence_number);
        }
        if self.file_sequence_number.is_none() && inherits_sequence_number {
            self.file_sequence_number = Some(manifest_file.sequence_number);
        }
    }

    /// Assign `first_row_id` to the live entries of the data manifest
    /// `manifest_file` that state none, the way a read of the manifest does:
    /// each takes the manifest's running id, which then advances by the
    /// entry's row count. In a manifest without a `first_row_id` no entry
    /// keeps one.
    pub(crate) fn assign_first_row_ids(
        manifest_file: &ManifestFile,
        entries: &mut [EncodedEntry],
    ) -> Result<()> {
        if manifest_file.content != ManifestContentType::Data {
            return Ok(());
        }
        let Some(manifest_first_row_id) = manifest_file.first_row_id else {
            for entry in entries {
                entry.first_row_id = None;
            }
            return Ok(());
        };
        let mut next_row_id = i64::try_from(manifest_first_row_id).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid first_row_id: {manifest_first_row_id} (exceeds i64::MAX)"),
            )
        })?;
        for entry in entries {
            if !entry.is_alive() || entry.first_row_id.is_some() {
                continue;
            }
            let file_first_row_id = next_row_id;
            entry.first_row_id = Some(file_first_row_id);
            let record_count = entry.record_count;
            next_row_id = file_first_row_id
                .checked_add_unsigned(record_count)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Row ID overflow assigning first_row_id in {}. File first_row_id: {file_first_row_id}, record count: {record_count}",
                            manifest_file.manifest_path
                        ),
                    )
                })?;
        }
        Ok(())
    }

    /// About what the entry holds in memory.
    fn estimate_heap_bytes(&self) -> u64 {
        (size_of::<Self>()
            + self.file_path.len()
            + size_of::<Struct>()
            + self.head.len()
            + self.tail.len()) as u64
    }
}

/// What the bytes of an [`EncodedEntry`] depend on: whether the manifest is
/// of format version 1, whose entries and data files have other fields, and
/// the type of the partition tuple.
#[derive(Clone, Debug, PartialEq, Eq)]
struct EntryLayout {
    is_v1: bool,
    partition_type: StructType,
}

/// Encodes the entries of the manifests of one layout, and decodes them.
#[derive(Debug)]
pub struct EntryCodec {
    layout: EntryLayout,
    avro_schema: AvroSchema,
    partition_struct_type: Type,
    partition_spec_id: i32,
    schema: SchemaRef,
}

impl EntryCodec {
    /// The codec of a manifest with the metadata `metadata`.
    pub(crate) fn new(metadata: &ManifestMetadata) -> Result<Self> {
        let partition_type = metadata.partition_spec.partition_type(&metadata.schema)?;
        let is_v1 = metadata.format_version == FormatVersion::V1;
        let avro_schema = if is_v1 {
            manifest_schema_v1(&partition_type)?
        } else {
            manifest_schema_v2(&partition_type)?
        };
        Ok(Self {
            partition_struct_type: Type::Struct(partition_type.clone()),
            layout: EntryLayout {
                is_v1,
                partition_type,
            },
            avro_schema,
            partition_spec_id: metadata.partition_spec.spec_id(),
            schema: metadata.schema.clone(),
        })
    }

    /// The codec of the data manifests `table` writes now under its
    /// partition spec `partition_spec_id`; `None` when the table has no such
    /// spec, or the spec no longer binds to the table's schema.
    pub(crate) fn of_table(table: &Table, partition_spec_id: i32) -> Option<Self> {
        let metadata = table.metadata();
        let partition_spec = metadata.partition_spec_by_id(partition_spec_id)?;
        let schema = metadata.current_schema();
        Self::new(&ManifestMetadata {
            schema: schema.clone(),
            schema_id: schema.schema_id(),
            partition_spec: partition_spec.as_ref().clone(),
            format_version: metadata.format_version(),
            content: ManifestContentType::Data,
        })
        .ok()
    }

    /// The Avro schema of the entries.
    pub(crate) fn avro_schema(&self) -> &AvroSchema {
        &self.avro_schema
    }

    /// The type of the entries' partition tuples.
    pub(crate) fn partition_type(&self) -> &StructType {
        &self.layout.partition_type
    }

    /// Whether `other` encodes an entry to the same bytes.
    pub(crate) fn has_layout_of(&self, other: &EntryCodec) -> bool {
        self.layout == other.layout
    }

    /// Encode `entry`. This is the one place an entry goes through Avro's
    /// value model; what it returns is written, and written again, as bytes.
    pub(crate) fn encode(&self, entry: ManifestEntry) -> Result<EncodedEntry> {
        let ManifestEntry {
            status,
            snapshot_id,
            sequence_number,
            file_sequence_number,
            mut data_file,
        } = entry;
        let content = data_file.content;
        let file_path: Arc<str> = Arc::from(data_file.file_path.as_str());
        let record_count = data_file.record_count;
        let partition = Arc::new(data_file.partition.clone());
        let malformed = || {
            Error::new(
                ErrorKind::Unexpected,
                "A manifest entry was not encoded the way its schema lays it out",
            )
        };

        if self.layout.is_v1 {
            let plain = ManifestEntry {
                status,
                snapshot_id: Some(0),
                sequence_number: None,
                file_sequence_number: None,
                data_file,
            };
            let value = to_value(ManifestEntryV1::try_from(
                plain,
                &self.partition_struct_type,
            )?)?
            .resolve(&self.avro_schema)?;
            let datum = Bytes::from(to_avro_datum(&self.avro_schema, value)?);
            // The status, and the snapshot id 0.
            if datum.len() < 2 || datum[0] != (status as u8) << 1 || datum[1] != 0 {
                return Err(malformed());
            }
            return Ok(EncodedEntry {
                status,
                snapshot_id,
                sequence_number,
                file_sequence_number,
                first_row_id: None,
                content,
                file_path,
                record_count,
                partition,
                head: datum.slice(2..),
                tail: Bytes::new(),
            });
        }

        let first_row_id = data_file.first_row_id.take();
        let mut tail = Vec::new();
        write_optional_string(&mut tail, data_file.referenced_data_file.as_deref());
        write_optional_long(&mut tail, data_file.content_offset);
        write_optional_long(&mut tail, data_file.content_size_in_bytes);
        let plain = ManifestEntry {
            status,
            snapshot_id: None,
            sequence_number: None,
            file_sequence_number: None,
            data_file,
        };
        let value = to_value(ManifestEntryV2::try_from(
            plain,
            &self.partition_struct_type,
        )?)?
        .resolve(&self.avro_schema)?;
        let datum = Bytes::from(to_avro_datum(&self.avro_schema, value)?);
        // The status and three ids left out come first; the `first_row_id`
        // left out and the fields after it come last.
        const PREFIX_BYTES: usize = 4;
        let head_end = datum
            .len()
            .checked_sub(1 + tail.len())
            .filter(|head_end| *head_end >= PREFIX_BYTES)
            .ok_or_else(malformed)?;
        let is_laid_out = datum[..PREFIX_BYTES] == [(status as u8) << 1, 0, 0, 0]
            && datum[head_end] == 0
            && datum[head_end + 1..] == tail[..];
        if !is_laid_out {
            return Err(malformed());
        }
        Ok(EncodedEntry {
            status,
            snapshot_id,
            sequence_number,
            file_sequence_number,
            first_row_id,
            content,
            file_path,
            record_count,
            partition,
            head: datum.slice(PREFIX_BYTES..head_end),
            tail: Bytes::from(tail),
        })
    }

    /// Append the bytes a manifest holds `entry` as to `out`.
    pub(crate) fn write_entry(&self, entry: &EncodedEntry, out: &mut Vec<u8>) {
        write_long(out, entry.status as i64);
        if self.layout.is_v1 {
            write_long(out, entry.snapshot_id.unwrap_or_default());
            out.extend_from_slice(&entry.head);
            return;
        }
        write_optional_long(out, entry.snapshot_id);
        write_optional_long(out, entry.sequence_number);
        write_optional_long(out, entry.file_sequence_number);
        out.extend_from_slice(&entry.head);
        write_optional_long(out, entry.first_row_id);
        out.extend_from_slice(&entry.tail);
    }

    /// Decode `entry`, which a codec of this layout encoded.
    pub fn decode(&self, entry: &EncodedEntry) -> Result<ManifestEntry> {
        let mut datum = Vec::with_capacity(entry.head.len() + entry.tail.len() + 48);
        self.write_entry(entry, &mut datum);
        let value = from_avro_datum(&self.avro_schema, &mut datum.as_slice(), None)?;
        if self.layout.is_v1 {
            let mut decoded = from_value::<ManifestEntryV1>(&value)?.try_into(
                self.partition_spec_id,
                &self.partition_struct_type,
                &self.schema,
            )?;
            decoded.snapshot_id = entry.snapshot_id;
            decoded.sequence_number = entry.sequence_number;
            decoded.file_sequence_number = entry.file_sequence_number;
            return Ok(decoded);
        }
        from_value::<ManifestEntryV2>(&value)?.try_into(
            self.partition_spec_id,
            &self.partition_struct_type,
            &self.schema,
        )
    }
}

/// Append `value` to `out` as Avro writes an `int` or a `long`.
pub(crate) fn write_long(out: &mut Vec<u8>, value: i64) {
    let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
    while zigzag >= 0x80 {
        out.push((zigzag as u8 & 0x7F) | 0x80);
        zigzag >>= 7;
    }
    out.push(zigzag as u8);
}

/// Append `value` to `out` as Avro writes a union of `null` and `long`.
fn write_optional_long(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(2);
            write_long(out, value);
        }
    }
}

/// Append `value` to `out` as Avro writes a union of `null` and `string`.
fn write_optional_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(2);
            write_long(out, value.len() as i64);
            out.extend_from_slice(value.as_bytes());
        }
    }
}

/// The entries of one manifest as the manifest holds them, with nothing
/// inherited from the manifest list.
#[derive(Debug)]
pub struct EncodedManifest {
    metadata: ManifestMetadata,
    layout: EntryLayout,
    entries: Vec<EncodedEntry>,
}

/// How many bytes of manifests are held for the commits and reads to come.
const MANIFESTS_HELD_BYTES: u64 = 256 * 1024 * 1024;

/// The manifests held, by path, with what each counts for and when it was
/// last asked for.
#[derive(Default)]
struct HeldManifests {
    manifests: HashMap<String, HeldManifest>,
    /// The paths of the manifests by when each was last asked for.
    paths_by_use: BTreeMap<u64, String>,
    held_bytes: u64,
    uses: u64,
}

struct HeldManifest {
    manifest: Arc<EncodedManifest>,
    heap_bytes: u64,
    last_use: u64,
}

static HELD_MANIFESTS: LazyLock<Mutex<HeldManifests>> = LazyLock::new(Mutex::default);

impl EncodedManifest {
    /// The manifest at `manifest_path`, when this process wrote it or read
    /// it and still holds it.
    pub fn find(manifest_path: &str) -> Option<Arc<EncodedManifest>> {
        let mut held = HELD_MANIFESTS.lock().unwrap();
        held.uses += 1;
        let used = held.uses;
        let found = held.manifests.get_mut(manifest_path)?;
        let last_use = std::mem::replace(&mut found.last_use, used);
        let manifest = Arc::clone(&found.manifest);
        let path = held
            .paths_by_use
            .remove(&last_use)
            .expect("a held manifest is listed by its last use");
        held.paths_by_use.insert(used, path);
        Some(manifest)
    }

    /// Hold `entries`, the entries of the manifest at `manifest_path` as
    /// `codec` encoded them, for the commits and reads to come. The manifests
    /// asked for longest ago make room.
    pub(crate) fn hold(
        manifest_path: &str,
        metadata: ManifestMetadata,
        codec: &EntryCodec,
        entries: Vec<EncodedEntry>,
    ) {
        let heap_bytes = size_of::<Self>() as u64
            + manifest_path.len() as u64
            + entries
                .iter()
                .map(EncodedEntry::estimate_heap_bytes)
                .sum::<u64>();
        if heap_bytes > MANIFESTS_HELD_BYTES {
            return;
        }
        let manifest = Arc::new(EncodedManifest {
            metadata,
            layout: codec.layout.clone(),
            entries,
        });
        let mut held = HELD_MANIFESTS.lock().unwrap();
        held.uses += 1;
        let used = held.uses;
        let replaced = held
            .manifests
            .insert(manifest_path.to_string(), HeldManifest {
                manifest,
                heap_bytes,
                last_use: used,
            });
        if let Some(replaced) = replaced {
            held.paths_by_use.remove(&replaced.last_use);
            held.held_bytes -= replaced.heap_bytes;
        }
        held.paths_by_use.insert(used, manifest_path.to_string());
        held.held_bytes += heap_bytes;
        while held.held_bytes > MANIFESTS_HELD_BYTES {
            let (_, path) = held
                .paths_by_use
                .pop_first()
                .expect("manifests are held while their bytes are counted");
            let dropped = held
                .manifests
                .remove(&path)
                .expect("a manifest listed by its use is held");
            held.held_bytes -= dropped.heap_bytes;
        }
    }

    /// The metadata the manifest was written with.
    pub fn metadata(&self) -> &ManifestMetadata {
        &self.metadata
    }

    /// The entries as the manifest holds them, with nothing inherited from
    /// the manifest list.
    pub fn entries(&self) -> &[EncodedEntry] {
        &self.entries
    }

    /// Stop holding the manifests under `directory`, as when the table that
    /// keeps its metadata there is dropped. A process that has just started
    /// holds none.
    pub fn forget_under(directory: &str) {
        let directory = directory.trim_end_matches('/');
        let mut held = HELD_MANIFESTS.lock().unwrap();
        let paths: Vec<String> = held
            .manifests
            .keys()
            .filter(|path| {
                path.strip_prefix(directory)
                    .is_some_and(|rest| rest.starts_with('/'))
            })
            .cloned()
            .collect();
        for path in paths {
            let forgotten = held
                .manifests
                .remove(&path)
                .expect("the path was just listed");
            held.paths_by_use.remove(&forgotten.last_use);
            held.held_bytes -= forgotten.heap_bytes;
        }
    }

    /// The codec that decodes the manifest's entries.
    pub fn build_codec(&self) -> Result<EntryCodec> {
        EntryCodec::new(&self.metadata)
    }

    /// The entries as a read of the manifest gives them, with what
    /// `manifest_file`, the manifest list's entry of the manifest, has them
    /// inherit: the snapshot id, the sequence numbers and the row ids.
    pub fn read_entries(&self, manifest_file: &ManifestFile) -> Result<Vec<EncodedEntry>> {
        let mut entries = self.read_uncommitted_entries(manifest_file);
        EncodedEntry::assign_first_row_ids(manifest_file, &mut entries)?;
        Ok(entries)
    }

    /// The entries as a read of the manifest gives them while the commit
    /// that wrote it is in progress: `manifest_file` is in no manifest list
    /// yet, so the entries keep the row ids they state.
    pub(crate) fn read_uncommitted_entries(
        &self,
        manifest_file: &ManifestFile,
    ) -> Vec<EncodedEntry> {
        let mut entries = self.entries.clone();
        for entry in &mut entries {
            if self.layout.is_v1 {
                // A version 1 entry states its snapshot and has no sequence
                // numbers, which read as the first.
                entry.snapshot_id = Some(entry.snapshot_id.unwrap_or_default());
                entry.sequence_number = Some(INITIAL_SEQUENCE_NUMBER);
                entry.file_sequence_number = Some(INITIAL_SEQUENCE_NUMBER);
            }
            entry.inherit_data(manifest_file);
        }
        entries
    }

    /// The entries of the manifest `manifest_file` lists in `table`, in the
    /// layout of `codec`, inheriting what a read has them inherit; with
    /// `is_uncommitted`, as a read gives them while the commit that wrote
    /// the manifest is in progress. A manifest that is not held, or is held
    /// in another layout, is fetched and each entry encoded.
    pub(crate) async fn load_entries(
        table: &Table,
        manifest_file: &ManifestFile,
        codec: &EntryCodec,
        is_uncommitted: bool,
    ) -> Result<Vec<EncodedEntry>> {
        let held = Self::find(&manifest_file.manifest_path)
            .filter(|manifest| manifest.layout == codec.layout);
        if let Some(manifest) = held {
            return if is_uncommitted {
                Ok(manifest.read_uncommitted_entries(manifest_file))
            } else {
                manifest.read_entries(manifest_file)
            };
        }
        let reader = table.manifest_reader();
        let manifest = if is_uncommitted {
            reader.read_uncommitted(manifest_file).await?
        } else {
            reader.read(manifest_file).await?
        };
        let (entries, metadata) = manifest.into_parts();
        let entries = entries
            .into_iter()
            .map(|entry| codec.encode(Arc::unwrap_or_clone(entry)))
            .collect::<Result<Vec<_>>>()?;
        // What a read of a committed manifest inherits never changes, so the
        // entries are held with it; a manifest of a commit in progress is
        // held by the writer that wrote it.
        let is_of_layout = EntryCodec::new(&metadata).is_ok_and(|own| own.has_layout_of(codec));
        if !is_uncommitted && is_of_layout {
            Self::hold(
                &manifest_file.manifest_path,
                metadata,
                codec,
                entries.clone(),
            );
        }
        Ok(entries)
    }
}
