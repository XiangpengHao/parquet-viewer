use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field};
use bytes::{Buf, Bytes};
use datafusion::{
    physical_plan::{ExecutionPlan, collect},
    prelude::SessionContext,
};
use leptos::logging;
use parquet::{
    arrow::ArrowWriter,
    errors::ParquetError,
    file::reader::{ChunkReader, Length},
};
use web_sys::{
    js_sys,
    wasm_bindgen::{JsCast, JsValue},
};

pub fn format_rows(rows: u64) -> String {
    let mut result = rows.to_string();
    let mut i = result.len();
    while i > 3 {
        i -= 3;
        result.insert(i, ',');
    }
    result
}

pub(crate) fn get_stored_value(key: &str) -> Option<String> {
    let window = web_sys::window()?;
    let storage = window.local_storage().unwrap()?;
    storage.get_item(key).unwrap()
}

pub(crate) fn save_to_storage(key: &str, value: &str) {
    if let Some(window) = web_sys::window()
        && let Ok(Some(storage)) = window.local_storage()
    {
        let _ = storage.set_item(key, value);
    }
}

pub fn format_arrow_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "Boolean".to_string(),
        DataType::Utf8 => "String".to_string(),
        DataType::Struct(fields) => format_struct_type(fields),
        DataType::List(child) => format!("List<{}>", format_arrow_type(child.data_type())),
        _ => data_type.to_string(),
    }
}

pub fn format_struct_type(fields: &[Arc<Field>]) -> String {
    if fields.is_empty() {
        return "Struct{}".to_string();
    }

    let field_strs: Vec<String> = fields
        .iter()
        .map(|f| format!("{}: {}", f.name(), format_arrow_type(f.data_type())))
        .collect();

    format!("Struct{{{}}}", field_strs.join(", "))
}

pub(crate) async fn execute_query_inner(
    query: &str,
    ctx: &SessionContext,
) -> Result<(Vec<RecordBatch>, Arc<dyn ExecutionPlan>)> {
    let plan = ctx.sql(query).await?;

    let (state, plan) = plan.into_parts();
    let plan = state.optimize(&plan)?;

    logging::log!("{}", &plan.display_indent());

    let physical_plan = state.create_physical_plan(&plan).await?;

    let results = collect(physical_plan.clone(), ctx.task_ctx().clone()).await?;
    Ok((results, physical_plan))
}

pub(crate) fn vscode_env() -> Option<JsValue> {
    let vscode =
        js_sys::eval("typeof acquireVsCodeApi === 'function' ? acquireVsCodeApi() : null").ok()?;
    if vscode.is_null() { None } else { Some(vscode) }
}

pub(crate) fn send_message_to_vscode(message_type: &str, vscode: &JsValue) {
    let message = js_sys::Object::new();
    js_sys::Reflect::set(&message, &"type".into(), &message_type.into()).unwrap();

    if let Ok(post_message) = js_sys::Reflect::get(vscode, &"postMessage".into())
        && post_message.is_function()
    {
        let post_message_fn = post_message.dyn_ref::<js_sys::Function>().unwrap();

        let _ = js_sys::Reflect::apply(post_message_fn, vscode, &js_sys::Array::of1(&message));

        logging::log!("Sent message to VS Code: {}", message_type);
    }
}

fn download_data(file_name: &str, data: Vec<u8>) {
    let blob =
        web_sys::Blob::new_with_u8_array_sequence(&js_sys::Array::of1(&data.into())).unwrap();
    let url = web_sys::Url::create_object_url_with_blob(&blob).unwrap();
    let a = web_sys::window()
        .unwrap()
        .document()
        .unwrap()
        .create_element("a")
        .unwrap();
    a.set_attribute("href", &url).unwrap();
    a.set_attribute("download", file_name).unwrap();
    a.dyn_ref::<web_sys::HtmlElement>().unwrap().click();
    web_sys::Url::revoke_object_url(&url).unwrap();
}

pub(crate) fn export_to_csv_inner(query_result: &[RecordBatch]) {
    let mut data = Vec::new();
    let mut writer = arrow::csv::WriterBuilder::new().build(&mut data);
    for batch in query_result {
        writer.write(batch).unwrap();
    }
    drop(writer);
    download_data("query_results.csv", data);
}

pub(crate) fn export_to_parquet_inner(query_result: &[RecordBatch]) {
    let mut buf = Vec::new();

    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::LZ4)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buf, query_result[0].schema(), Some(props))
        .expect("Failed to create parquet writer");

    // Write all record batches
    for batch in query_result {
        writer.write(batch).expect("Failed to write record batch");
    }

    writer.close().expect("Failed to close writer");

    download_data("query_results.parquet", buf);
}

pub struct ColumnChunk {
    data: Bytes,
    byte_range: (u64, u64),
}

impl ColumnChunk {
    pub fn new(data: Bytes, byte_range: (u64, u64)) -> Self {
        Self { data, byte_range }
    }
}

impl Length for ColumnChunk {
    fn len(&self) -> u64 {
        self.byte_range.1 - self.byte_range.0
    }
}

impl ChunkReader for ColumnChunk {
    type T = bytes::buf::Reader<Bytes>;
    fn get_read(&self, offset: u64) -> Result<Self::T, ParquetError> {
        let start = offset - self.byte_range.0;
        Ok(self.data.slice(start as usize..).reader())
    }

    fn get_bytes(&self, offset: u64, length: usize) -> Result<Bytes, ParquetError> {
        let start = offset - self.byte_range.0;
        Ok(self.data.slice(start as usize..(start as usize + length)))
    }
}
