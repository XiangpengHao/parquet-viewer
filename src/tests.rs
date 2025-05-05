use crate::{SESSION_CTX, utils::execute_query_inner, views::parquet_reader::read_from_url};
use arrow::{array::AsArray, datatypes::Int64Type};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_read_parquet() {
    // This test uses a known public Parquet file
    let ctx = SESSION_CTX.clone();
    let url = "https://raw.githubusercontent.com/tobilg/aws-edge-locations/main/data/aws-edge-locations.parquet";
    let result = read_from_url(url).unwrap();
    let table = result
        .try_into_resolved(&ctx)
        .await
        .expect("Should successfully parse a valid parquet URL");

    let (rows, _) = execute_query_inner("select count(*) from \"aws-edge-locations\"", &ctx)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column(0).len(), 1);
    assert_eq!(
        rows[0].column(0).as_primitive::<Int64Type>().values()[0],
        107
    );
    assert_eq!(table.table_name(), "aws-edge-locations");
}
