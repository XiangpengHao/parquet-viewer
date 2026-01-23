use anyhow::Result;
use arrow_schema::SchemaRef;
use gloo_net::http::Request;
use serde_json::json;

use crate::{parquet_ctx::ParquetResolved, views::main_layout::DEFAULT_QUERY};

fn nl_cache(key: &str, file_name: &str) -> Option<String> {
    if key == DEFAULT_QUERY {
        return Some(format!("SELECT * FROM \"{file_name}\" LIMIT 10"));
    }
    None
}

pub(crate) async fn user_input_to_sql(input: &str, context: &ParquetResolved) -> Result<String> {
    // if the input seems to be a SQL query, replace table names with registered names
    if input.starts_with("select") || input.starts_with("SELECT") {
        let sql = input.replace(
            &format!("\"{}\"", context.table_name()),
            &format!("\"{}\"", context.registered_table_name()),
        );
        // Also handle unquoted table names
        let sql = sql.replace(
            &format!(" {} ", context.table_name()),
            &format!(" \"{}\" ", context.registered_table_name()),
        );
        let sql = sql.replace(
            &format!(" {}\n", context.table_name()),
            &format!(" \"{}\" ", context.registered_table_name()),
        );
        return Ok(sql);
    }

    // check if the input is in the cache
    let cached_sql = nl_cache(input, context.registered_table_name());
    if let Some(sql) = cached_sql {
        return Ok(sql);
    }

    // otherwise, treat it as some natural language
    let schema = context.metadata().schema();
    let file_name = context.registered_table_name();
    let schema_str = schema_to_brief_str(schema);

    tracing::info!("Generating SQL for input: {}", input);

    let sql = generate_sql(input, file_name, &schema_str).await?;
    tracing::info!("{}", sql);
    Ok(sql)
}

fn schema_to_brief_str(schema: &SchemaRef) -> String {
    let fields = schema.fields();
    let field_strs = fields
        .iter()
        .map(|field| format!("{}: {}", field.name(), field.data_type()));
    field_strs.collect::<Vec<_>>().join(", ")
}

async fn generate_sql(input: &str, file_name: &str, schema_str: &str) -> Result<String> {
    let url = "https://parquet-viewer-llm.haoxiangpeng123.workers.dev/api/llm";

    let payload = json!({
        "input": input,
        "file_name": file_name,
        "schema_str": schema_str
    });

    let response = Request::post(url)
        .header("Content-Type", "application/json")
        .json(&payload)?
        .send()
        .await?;

    if !response.ok() {
        return Err(anyhow::anyhow!(
            "Network response was not ok: {}",
            response.status()
        ));
    }

    let json_value: serde_json::Value = response.json().await?;

    json_value
        .get("response")
        .and_then(|t| t.as_str())
        .ok_or(anyhow::anyhow!("Failed to extract SQL from response"))
        .map(|s| s.trim().to_string())
}
