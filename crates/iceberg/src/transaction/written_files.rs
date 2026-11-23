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

//! The files the actions of one commit attempt write, kept so that the
//! attempt can delete the ones nothing references once its outcome is known.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::error::Result;
use crate::io::{FileIO, OutputFile};
use crate::spec::ManifestFile;

/// The manifests and manifest lists one action writes during one attempt of
/// [`Transaction::commit`](crate::transaction::Transaction::commit), and the
/// manifests those manifest lists name.
///
/// It is the Rust equivalent of the bookkeeping behind Java's
/// `SnapshotProducer.cleanAll` and `cleanUncommitted`. Every manifest of an
/// action is created through [`WrittenFiles::new_manifest_output`] and every
/// manifest list through [`WrittenFiles::new_manifest_list_output`], so the
/// record holds exactly the files the action wrote and none it only read or
/// carried forward.
///
/// An attempt keeps one record per action. What it deletes depends on how it
/// ends:
///
/// - It did not commit: every recorded file, see [`collect_written_paths`].
/// - It committed: the recorded manifests that no recorded manifest list
///   names, see [`collect_unreferenced_paths`]. Those are the manifests an
///   action wrote and then merged into another. Every manifest list is kept,
///   and so is every manifest that the manifest list of any action of the
///   attempt names.
/// - Its outcome is unknown: nothing.
#[derive(Default)]
pub(crate) struct WrittenFiles {
    record: Mutex<Record>,
}

#[derive(Default)]
struct Record {
    /// The manifests written, in the order they were written.
    manifest_paths: Vec<String>,
    /// The manifest lists written, in the order they were written.
    manifest_list_paths: Vec<String>,
    /// The manifests named by the manifest lists written.
    listed_manifest_paths: HashSet<String>,
}

impl WrittenFiles {
    /// Creates the output file of the manifest at `path`, recording it as
    /// written by this action. The path is recorded before anything is
    /// written, so a manifest left half-written by a failed write is recorded
    /// too.
    pub(crate) fn new_manifest_output(&self, file_io: &FileIO, path: String) -> Result<OutputFile> {
        let output_file = file_io.new_output(&path)?;
        self.record.lock().unwrap().manifest_paths.push(path);
        Ok(output_file)
    }

    /// Records the manifests at `paths` as written by this action: ones it
    /// wrote ahead of the attempt.
    pub(crate) fn record_manifests(&self, paths: &[String]) {
        self.record
            .lock()
            .unwrap()
            .manifest_paths
            .extend(paths.iter().cloned());
    }

    /// The manifests recorded as written, in the order they were written.
    pub(crate) fn collect_manifest_paths(&self) -> Vec<String> {
        self.record.lock().unwrap().manifest_paths.clone()
    }

    /// Creates the output file of the manifest list at `path`, recording it
    /// as written by this action and `listed_manifests`, the manifests the
    /// action writes into it, as named by it.
    pub(crate) fn new_manifest_list_output(
        &self,
        file_io: &FileIO,
        path: String,
        listed_manifests: &[ManifestFile],
    ) -> Result<OutputFile> {
        let output_file = file_io.new_output(&path)?;
        let mut record = self.record.lock().unwrap();
        record.manifest_list_paths.push(path);
        record.listed_manifest_paths.extend(
            listed_manifests
                .iter()
                .map(|manifest| manifest.manifest_path.clone()),
        );
        Ok(output_file)
    }
}

/// Every manifest and manifest list recorded in `written_files`, the records
/// of the actions of one attempt.
pub(crate) fn collect_written_paths(written_files: &[WrittenFiles]) -> Vec<String> {
    let mut paths = Vec::new();
    for action_files in written_files {
        let record = action_files.record.lock().unwrap();
        paths.extend(record.manifest_paths.iter().cloned());
        paths.extend(record.manifest_list_paths.iter().cloned());
    }
    paths
}

/// The manifests recorded in `written_files`, the records of the actions of
/// one attempt, that none of the manifest lists recorded in them names.
///
/// An action that wrote no manifest list has not said which of its manifests
/// are named, so none of its manifests is returned.
pub(crate) fn collect_unreferenced_paths(written_files: &[WrittenFiles]) -> Vec<String> {
    let records: Vec<_> = written_files
        .iter()
        .map(|action_files| action_files.record.lock().unwrap())
        .collect();
    let listed_manifest_paths: HashSet<&str> = records
        .iter()
        .flat_map(|record| &record.listed_manifest_paths)
        .map(String::as_str)
        .collect();

    records
        .iter()
        .filter(|record| !record.manifest_list_paths.is_empty())
        .flat_map(|record| &record.manifest_paths)
        .filter(|path| !listed_manifest_paths.contains(path.as_str()))
        .cloned()
        .collect()
}
