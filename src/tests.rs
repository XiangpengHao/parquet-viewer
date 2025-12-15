use std::sync::Arc;

use crate::{
    SESSION_CTX, storage::readers, utils::execute_query_inner,
    views::parquet_reader::ParquetUnresolved,
};
use arrow::{array::AsArray, datatypes::Int64Type, util::pretty::pretty_format_batches};
use arrow_array::{Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use bytes::Bytes;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_read_parquet() {
    // This test uses a known public Parquet file
    let ctx = SESSION_CTX.clone();
    let url = "https://raw.githubusercontent.com/tobilg/aws-edge-locations/main/data/aws-edge-locations.parquet";
    let result = readers::read_from_url(url).unwrap();
    let table = result
        .try_into_resolved(&ctx)
        .await
        .expect("Should successfully parse a valid parquet URL");

    let query = format!("select count(*) from \"{}\"", table.registered_table_name());
    let (rows, _) = execute_query_inner(&query, &ctx).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column(0).len(), 1);
    assert_eq!(
        rows[0].column(0).as_primitive::<Int64Type>().values()[0],
        107
    );
    assert_eq!(table.table_name(), "aws-edge-locations");
}

fn gen_parquet_with_empty_rows() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(vec![]))],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

async fn register_parquet_file(file_name: &str, data: Vec<u8>) -> ParquetUnresolved {
    let uuid = uuid::Uuid::new_v4();
    let object_store = Arc::new(InMemory::new());
    let object_store_url = ObjectStoreUrl::parse(format!("test://{uuid}")).unwrap();
    let payload = PutPayload::from_bytes(Bytes::from(data));
    let path = Path::parse(file_name).unwrap();
    object_store.put(&path, payload).await.unwrap();
    ParquetUnresolved::try_new(file_name.to_string(), path, object_store_url, object_store).unwrap()
}

#[wasm_bindgen_test]
async fn test_read_parquet_with_empty_rows() {
    let ctx = SESSION_CTX.clone();
    let parquet_unresolved =
        register_parquet_file("empty_rows.parquet", gen_parquet_with_empty_rows()).await;
    let table = Arc::new(parquet_unresolved.try_into_resolved(&ctx).await.unwrap());
    let query = format!("select count(*) from \"{}\"", table.registered_table_name());
    let (rows, _) = execute_query_inner(&query, &ctx).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column(0).len(), 1);
    assert_eq!(rows[0].column(0).as_primitive::<Int64Type>().values()[0], 0);
    drop(table);
}

#[wasm_bindgen_test]
async fn test_read_parquet_with_uppercase_name() {
    let ctx = SESSION_CTX.clone();
    let parquet_unresolved = register_parquet_file(
        "UPPERCASE_NAME.parquet",
        gen_parquet_with_page_stats(EnabledStatistics::Page),
    )
    .await;
    let table = Arc::new(parquet_unresolved.try_into_resolved(&ctx).await.unwrap());
    let query = format!("select count(*) from \"{}\"", table.registered_table_name());
    let (_rows, _) = execute_query_inner(&query, &ctx).await.unwrap();
    drop(table);
}

fn gen_parquet_with_nested_column() -> Vec<u8> {
    let fields = Fields::from(vec![
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Utf8, false),
    ]);
    let struct_array = StructArray::new(
        fields.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(vec![1, 2, 3])),
            Arc::new(StringArray::from_iter_values(vec!["foo", "bar", "baz"])),
        ],
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Struct(fields),
        false,
    )]));
    let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
    writer.write(&record_batch).unwrap();
    writer.close().unwrap();
    buf
}

#[wasm_bindgen_test]
async fn test_read_parquet_with_nested_column() {
    let ctx = SESSION_CTX.clone();
    let parquet_unresolved =
        register_parquet_file("nested_column.parquet", gen_parquet_with_nested_column()).await;
    let table = Arc::new(parquet_unresolved.try_into_resolved(&ctx).await.unwrap());
    let query = format!("select a.b, a.c from \"{}\"", table.registered_table_name());
    let (rows, _) = execute_query_inner(&query, &ctx).await.unwrap();
    tracing::info!("{}", pretty_format_batches(&rows).unwrap());
    assert_eq!(rows.len(), 1);
    let rows = rows[0].clone();
    assert_eq!(rows.num_rows(), 3);
    assert_eq!(rows.column(0).as_primitive::<Int64Type>().values()[0], 1);
    let string_array = rows.column(1).as_string::<i32>();
    assert_eq!(string_array.value(0), "foo");
    drop(table);
}

fn gen_parquet_with_page_stats(stats_level: EnabledStatistics) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..10_000))],
    )
    .unwrap();
    let mut buf = Vec::new();

    let props = WriterProperties::builder()
        .set_statistics_enabled(stats_level)
        .set_data_page_size_limit(100)
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}
#[wasm_bindgen_test]
async fn test_render_page_stats() {
    let ctx = SESSION_CTX.clone();
    let parquet_unresolved = register_parquet_file(
        "page_stats.parquet",
        gen_parquet_with_page_stats(EnabledStatistics::Page),
    )
    .await;
    let table = Arc::new(parquet_unresolved.try_into_resolved(&ctx).await.unwrap());
    drop(table);
}

#[wasm_bindgen_test]
async fn test_render_chunk_stats() {
    let ctx = SESSION_CTX.clone();
    let parquet_unresolved = register_parquet_file(
        "chunk_stats.parquet",
        gen_parquet_with_page_stats(EnabledStatistics::Chunk),
    )
    .await;
    let table = Arc::new(parquet_unresolved.try_into_resolved(&ctx).await.unwrap());
    drop(table);
}

#[wasm_bindgen_test]
async fn test_render_no_stats() {
    let ctx = SESSION_CTX.clone();
    let parquet_unresolved = register_parquet_file(
        "no_stats.parquet",
        gen_parquet_with_page_stats(EnabledStatistics::None),
    )
    .await;
    let table = Arc::new(parquet_unresolved.try_into_resolved(&ctx).await.unwrap());
    drop(table);
}
