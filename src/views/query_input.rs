use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_plan::{ExecutionPlan, collect};
use leptos::wasm_bindgen::{JsCast, JsValue};
use leptos::{logging, prelude::*};
use serde_json::json;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, Request, RequestInit, RequestMode, Response, js_sys};

use crate::utils::get_stored_value;
use crate::{ParquetResolved, SESSION_CTX, views::settings::ANTHROPIC_API_KEY};

pub(crate) async fn execute_query_inner(
    query: &str,
) -> Result<(Vec<RecordBatch>, Arc<dyn ExecutionPlan>)> {
    let ctx = SESSION_CTX.as_ref();
    let plan = ctx.sql(query).await?;

    let (state, plan) = plan.into_parts();
    let plan = state.optimize(&plan)?;

    logging::log!("{}", &plan.display_indent());

    let physical_plan = state.create_physical_plan(&plan).await?;

    let results = collect(physical_plan.clone(), ctx.task_ctx().clone()).await?;
    Ok((results, physical_plan))
}

#[component]
pub fn QueryInput(
    user_input: ReadSignal<Option<String>>,
    on_user_submit_query: impl Fn(String) + 'static + Send + Copy,
) -> impl IntoView {
    let stored_api_key = get_stored_value("claude_api_key", "");
    let (api_key, _) = signal(stored_api_key);

    Effect::new(move |_| {
        if let Some(window) = web_sys::window() {
            if let Ok(Some(storage)) = window.local_storage() {
                let _ = storage.set_item("claude_api_key", &api_key.get());
            }
        }
    });

    let (input_value, set_input_value) = signal(user_input.get_untracked());

    Effect::new(move |_| {
        set_input_value.set(user_input.get());
    });

    let key_down = move |ev: web_sys::KeyboardEvent| {
        if ev.key() == "Enter" {
            let input = input_value.get();
            if let Some(input) = input {
                on_user_submit_query(input);
            }
        }
    };

    let button_press = move |_ev: web_sys::MouseEvent| {
        let input = input_value.get();
        if let Some(input) = input {
            on_user_submit_query(input);
        }
    };

    view! {
        <div class="flex gap-2 items-center flex-col relative">
            <div class="w-full flex gap-2 items-center">
                <input
                    type="text"
                    on:input=move |ev| set_input_value(Some(event_target_value(&ev)))
                    prop:value=input_value
                    on:keydown=key_down
                    class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                />
                <div class="flex items-center gap-1">
                    <button
                        on:click=button_press
                        class="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 whitespace-nowrap"
                    >
                        "Run Query"
                    </button>
                    <div class="relative group">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            class="h-5 w-5 text-gray-500 hover:text-gray-700 cursor-help"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                        >
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                            />
                        </svg>
                        <div class="absolute bottom-full right-0 mb-2 w-64 p-2 bg-gray-800 text-white text-xs rounded shadow-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none">
                            "SQL (begin with 'SELECT') or natural language, your choice!"
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }
}

pub(crate) async fn user_input_to_sql(input: &str, table: &ParquetResolved) -> Result<String> {
    // if the input seems to be a SQL query, return it as is
    if input.starts_with("select") || input.starts_with("SELECT") {
        return Ok(input.to_string());
    }

    // otherwise, treat it as some natural language

    let schema = &table.display_info.schema;
    let file_name = &table.table_name;
    let api_key = get_stored_value(ANTHROPIC_API_KEY, "");
    let schema_str = schema_to_brief_str(schema);

    let prompt = format!(
        "Generate a SQL query to answer the following question: {}. You should generate PostgreSQL SQL dialect, all field names and table names should be double quoted, and the output SQL should be executable, be careful about the available columns. The table name is: \"{}\" (without quotes), the schema of the table is: {}.  ",
        input, file_name, schema_str
    );
    logging::log!("{}", prompt);

    let sql = generate_sql_via_claude(&prompt, &api_key).await?;
    logging::log!("{}", sql);
    Ok(sql)
}

fn schema_to_brief_str(schema: &SchemaRef) -> String {
    let fields = schema.fields();
    let field_strs = fields
        .iter()
        .map(|field| format!("{}: {}", field.name(), field.data_type()));
    field_strs.collect::<Vec<_>>().join(", ")
}

// Asynchronous function to call the Claude API
async fn generate_sql_via_claude(prompt: &str, api_key: &str) -> Result<String> {
    if api_key.trim().is_empty() {
        logging::log!("No API key provided, using fallback endpoint");
        send_request_to_fallback(prompt).await
    } else {
        logging::log!("Using Claude API");
        send_request_to_claude(prompt, api_key).await
    }
}

// Helper function to send request to fallback endpoint
async fn send_request_to_fallback(prompt: &str) -> Result<String> {
    let fallback_url = "https://parquet-viewer-llm.haoxiangpeng123.workers.dev/api/llm";

    // Create the payload for the fallback endpoint
    let payload = json!({
        "prompt": prompt
    });

    // Setup request options
    let (opts, headers) = setup_request_options("POST", RequestMode::Cors);
    headers.set("content-type", "application/json").unwrap();

    // Send request and get response
    let response = send_http_request(fallback_url, &opts, &payload).await?;

    // Parse the response
    let json_value = parse_response_to_json(response).await?;

    // Extract SQL from response
    json_value
        .get("response")
        .and_then(|t| t.as_str())
        .ok_or(anyhow::anyhow!(
            "Failed to extract SQL from fallback response"
        ))
        .map(|s| s.trim().to_string())
}

// Helper function to send request to Claude API
async fn send_request_to_claude(prompt: &str, api_key: &str) -> Result<String> {
    let url = "https://api.anthropic.com/v1/messages";

    // Create the payload for Claude API
    let payload = json!({
        "model": "claude-3-haiku-20240307",
        "max_tokens": 1024,
        "messages": [{
            "role": "user",
            "content": prompt
        }],
        "system": "You are a SQL query generator. You should only respond with the generated SQL query. Do not include any explanation, JSON wrapping, or additional text."
    });

    // Setup request options
    let (opts, headers) = setup_request_options("POST", RequestMode::Cors);
    headers.set("content-type", "application/json").unwrap();
    headers.set("anthropic-version", "2023-06-01").unwrap();
    headers.set("x-api-key", api_key).unwrap();
    headers
        .set("anthropic-dangerous-direct-browser-access", "true")
        .unwrap();

    // Send request and get response
    let response = send_http_request(url, &opts, &payload).await?;

    // Parse the response
    let json_value = parse_response_to_json(response).await?;

    // Extract SQL from Claude API response format
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

// Setup request options with method and mode
fn setup_request_options(method: &str, mode: RequestMode) -> (RequestInit, Headers) {
    let opts = RequestInit::new();
    opts.set_method(method);
    opts.set_mode(mode);

    let headers = Headers::new().unwrap();
    opts.set_headers(&headers);

    (opts, headers)
}

// Send HTTP request with given URL, options and payload
async fn send_http_request(
    url: &str,
    opts: &RequestInit,
    payload: &serde_json::Value,
) -> Result<Response> {
    // Set body
    let body = serde_json::to_string(payload)?;
    opts.set_body(&JsValue::from_str(&body));

    // Create Request
    let request = Request::new_with_str_and_init(url, opts)
        .map_err(|e| anyhow::anyhow!("Request creation failed: {:?}", e))?;

    // Send the request
    let window = web_sys::window().ok_or(anyhow::anyhow!("No global `window` exists"))?;
    let response_value = JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(|e| anyhow::anyhow!("Fetch error: {:?}", e))?;

    // Convert the response to a WebSys Response object
    let response: Response = response_value
        .dyn_into()
        .map_err(|e| anyhow::anyhow!("Response casting failed: {:?}", e))?;

    if !response.ok() {
        return Err(anyhow::anyhow!(
            "Network response was not ok: {}",
            response.status()
        ));
    }

    Ok(response)
}

// Parse HTTP response to JSON
async fn parse_response_to_json(response: Response) -> Result<serde_json::Value> {
    // Parse the JSON response
    let json = JsFuture::from(
        response
            .json()
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {:?}", e))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("JSON parsing error: {:?}", e))?;

    // Convert to serde_json Value
    let json_string = js_sys::JSON::stringify(&json)
        .map_err(|e| anyhow::anyhow!("Failed to stringify JSON: {:?}", e))?
        .as_string()
        .ok_or(anyhow::anyhow!("Failed to convert to string"))?;

    serde_json::from_str(&json_string)
        .map_err(|e| anyhow::anyhow!("Failed to parse to serde_json::Value: {:?}", e))
}
