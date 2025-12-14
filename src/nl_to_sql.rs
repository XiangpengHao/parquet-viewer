use anyhow::Result;
use arrow_schema::SchemaRef;
use gloo_net::http::Request;
use serde_json::json;

use crate::{
    DEFAULT_QUERY, parquet_ctx::ParquetResolved, utils::get_stored_value,
    views::settings::ANTHROPIC_API_KEY,
};

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
    let api_key = get_stored_value(ANTHROPIC_API_KEY);
    let schema_str = schema_to_brief_str(schema);

    let prompt = format!(
        "Generate a SQL query to answer the following question: {input}. You should generate PostgreSQL SQL dialect, all field names and table names should be double quoted, and the output SQL should be executable, be careful about the available columns. The table name is: \"{file_name}\" (without quotes), the schema of the table is: {schema_str}.  ",
    );
    tracing::info!("{}", prompt);

    let sql = generate_sql_via_claude(&prompt, &api_key).await?;
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

async fn generate_sql_via_claude(prompt: &str, api_key: &Option<String>) -> Result<String> {
    if let Some(api_key) = api_key
        && !api_key.trim().is_empty()
    {
        tracing::info!("Using Claude API");
        send_request_to_claude(prompt, api_key).await
    } else {
        tracing::info!("No API key provided, using fallback endpoint");
        send_request_to_cloudflare(prompt).await
    }
}

async fn send_request_to_cloudflare(prompt: &str) -> Result<String> {
    let fallback_url = "https://parquet-viewer-llm.haoxiangpeng123.workers.dev/api/llm";

    let payload = json!({
        "prompt": prompt
    });

    let response = Request::post(fallback_url)
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
        .ok_or(anyhow::anyhow!(
            "Failed to extract SQL from fallback response"
        ))
        .map(|s| s.trim().to_string())
}

async fn send_request_to_claude(prompt: &str, api_key: &str) -> Result<String> {
    let url = "https://api.anthropic.com/v1/messages";

    let payload = json!({
        "model": "claude-3-haiku-20240307",
        "max_tokens": 1024,
        "messages": [{
            "role": "user",
            "content": prompt
        }],
        "system": "You are a SQL query generator. You should only respond with the generated SQL query. Do not include any explanation, JSON wrapping, or additional text."
    });

    let response = Request::post(url)
        .header("Content-Type", "application/json")
        .header("Anthropic-Version", "2023-06-01")
        .header("X-Api-Key", api_key)
        .header("Anthropic-Dangerous-Direct-Browser-Access", "true")
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
        .get("content")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("text"))
        .and_then(|t| t.as_str())
        .ok_or(anyhow::anyhow!(
            "Failed to extract SQL from Claude response"
        ))
        .map(|s| s.trim().to_string())
}
