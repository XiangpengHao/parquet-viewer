use crate::views::parquet_reader::read_from_url;
use arrow::{array::AsArray, datatypes::Int64Type};
use datafusion::prelude::SessionContext;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_read_parquet() {
    // This test uses a known public Parquet file
    let ctx = SessionContext::new();
    let url = "https://raw.githubusercontent.com/tobilg/aws-edge-locations/main/data/aws-edge-locations.parquet";
    let result = read_from_url(url).unwrap();
    let table = result
        .try_into_resolved(&ctx)
        .await
        .expect("Should successfully parse a valid parquet URL");

    let df = ctx
        .sql("select count(*) from \"aws-edge-locations\"")
        .await
        .unwrap();
    let rows = df.collect().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column(0).len(), 1);
    assert_eq!(
        rows[0].column(0).as_primitive::<Int64Type>().values()[0],
        106
    );
    assert_eq!(table.table_name, "aws-edge-locations");
}
