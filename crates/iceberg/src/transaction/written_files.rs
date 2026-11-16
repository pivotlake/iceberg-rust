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

//! The files one commit attempt writes, kept so that an attempt that did not
//! commit can delete them.

use std::sync::Mutex;

use crate::error::Result;
use crate::io::{FileIO, OutputFile};

/// The manifests and the manifest list one commit attempt writes.
///
/// It is the Rust equivalent of the bookkeeping behind Java's
/// `SnapshotProducer.cleanAll`. Every manifest and manifest list of an attempt
/// is created through [`WrittenFiles::new_output`], so the record holds
/// exactly the files the attempt wrote and none it only read or carried
/// forward.
#[derive(Default)]
pub(crate) struct WrittenFiles {
    paths: Mutex<Vec<String>>,
}

impl WrittenFiles {
    /// Creates the output file at `path`, recording it as written by this
    /// attempt. The path is recorded before anything is written, so a file
    /// left half-written by a failed write is recorded too.
    pub(crate) fn new_output(&self, file_io: &FileIO, path: String) -> Result<OutputFile> {
        let output_file = file_io.new_output(&path)?;
        self.paths.lock().unwrap().push(path);
        Ok(output_file)
    }

    /// Takes the paths recorded so far, in the order they were written.
    pub(crate) fn take_paths(&self) -> Vec<String> {
        std::mem::take(&mut self.paths.lock().unwrap())
    }
}
