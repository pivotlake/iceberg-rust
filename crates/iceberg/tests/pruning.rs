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

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::expr::{
    Bind, ExpressionEvaluator, InclusiveMetricsEvaluator, InclusiveProjection, ManifestEvaluator,
    Reference,
};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Datum, FieldSummary, Literal,
    ManifestContentType, ManifestFile, NestedField, PartitionSpec, PrimitiveType, Schema, Struct,
    Transform, Type,
};

#[test]
fn prune_manifests_and_files_without_a_table_scan() {
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
    );
    let spec = Arc::new(
        PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap(),
    );
    let filter = Reference::new("id")
        .greater_than(Datum::long(10))
        .bind(schema.clone(), true)
        .unwrap();
    let partition_schema = Arc::new(
        Schema::builder()
            .with_fields(spec.partition_type(&schema).unwrap().fields().to_vec())
            .build()
            .unwrap(),
    );
    let projected = InclusiveProjection::new(spec.clone())
        .project(&filter)
        .unwrap()
        .bind(partition_schema, true)
        .unwrap();
    let manifests = ManifestEvaluator::builder(projected.clone()).build();
    let partitions = ExpressionEvaluator::new(projected);
    let mut manifest = ManifestFile {
        manifest_path: "manifest.avro".into(),
        manifest_length: 1,
        partition_spec_id: spec.spec_id(),
        content: ManifestContentType::Data,
        sequence_number: 0,
        min_sequence_number: 0,
        added_snapshot_id: 1,
        added_files_count: Some(1),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(1),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: Some(vec![FieldSummary {
            contains_null: false,
            contains_nan: None,
            lower_bound: Some(5i64.to_le_bytes().to_vec().into()),
            upper_bound: Some(5i64.to_le_bytes().to_vec().into()),
        }]),
        key_metadata: None,
        first_row_id: None,
    };
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path("file.parquet".into())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(1)
        .record_count(1)
        .partition(Struct::from_iter([Some(Literal::long(5))]))
        .lower_bounds(HashMap::from([(1, Datum::long(5))]))
        .upper_bounds(HashMap::from([(1, Datum::long(5))]))
        .build()
        .unwrap();

    let manifest_matches = manifests.eval(&manifest).unwrap();
    let partition_matches = partitions.eval(&file).unwrap();
    let metrics_match = InclusiveMetricsEvaluator::eval(&filter, &file, false).unwrap();
    manifest.partitions = None;
    let unknown_manifest_matches = manifests.eval(&manifest).unwrap();

    assert!(!manifest_matches);
    assert!(!partition_matches);
    assert!(!metrics_match);
    assert!(unknown_manifest_matches);
}
