use serde::{Deserialize, Serialize};
use serde_json::json;
use worker::*;

#[derive(Deserialize)]
struct LlmRequest {
    input: String,
    file_name: String,
    schema_str: String,
}

#[derive(Serialize)]
struct LlmResponse {
    response: String,
}

#[derive(Deserialize)]
struct OpenRouterChoice {
    message: OpenRouterMessageResponse,
}

#[derive(Deserialize)]
struct OpenRouterMessageResponse {
    content: String,
}

#[derive(Deserialize)]
struct OpenRouterResponse {
    choices: Vec<OpenRouterChoice>,
}

#[derive(Deserialize)]
struct LlmStructuredOutput {
    sql: String,
}

fn cors_headers() -> Headers {
    let headers = Headers::new();
    let _ = headers.set("Access-Control-Allow-Origin", "*");
    let _ = headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    let _ = headers.set("Access-Control-Allow-Headers", "Content-Type");
    headers
}

fn handle_options(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    Ok(Response::empty()?.with_headers(cors_headers()))
}

async fn handle_llm_request(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let api_key = ctx.secret("OPENROUTER_API_KEY")?.to_string();

    let body: LlmRequest = req.json().await?;

    let prompt = format!(
        "Generate a SQL query to answer the following question: {}. You should generate PostgreSQL SQL dialect, all field names and table names should be double quoted, and the output SQL should be executable, be careful about the available columns. The table name is: \"{}\" (without quotes), the schema of the table is: {}.",
        body.input, body.file_name, body.schema_str
    );

    let openrouter_request = json!({
        "model": "openai/gpt-oss-120b",
        "messages": [
            {
                "role": "system",
                "content": "You are a SQL query generator for a parquet file viewer. Generate SQL queries based on user requests. Return a JSON object that matches the response schema with a single sql string field. The sql value must be valid PostgreSQL and must not include code fences or extra fields. DO not use features that are not SUPPORTED by Apache DataFusion."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 1024,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "sql_response",
                "strict": true,
                "schema": {
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "The generated SQL query"
                        }
                    },
                    "required": ["sql"],
                    "additionalProperties": false
                }
            }
        }
    });

    let headers = Headers::new();
    headers.set("Content-Type", "application/json")?;
    headers.set("Authorization", &format!("Bearer {}", api_key))?;
    headers.set("HTTP-Referer", "https://parquet-viewer.xiangpeng.systems")?;
    headers.set("X-Title", "Parquet Viewer")?;

    let body_str = serde_json::to_string(&openrouter_request)?;

    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(body_str.into()));

    let openrouter_req =
        Request::new_with_init("https://openrouter.ai/api/v1/chat/completions", &init)?;

    let mut openrouter_resp = Fetch::Request(openrouter_req).send().await?;

    if openrouter_resp.status_code() != 200 {
        let error_text = openrouter_resp.text().await?;
        console_log!("OpenRouter error: {}", error_text);
        return Ok(
            Response::error(format!("OpenRouter API error: {}", error_text), 500)?
                .with_headers(cors_headers()),
        );
    }

    let openrouter_response: OpenRouterResponse = openrouter_resp.json().await?;

    let content = openrouter_response
        .choices
        .first()
        .map(|c| c.message.content.clone())
        .unwrap_or_default();

    // Parse the structured JSON response
    let sql = match serde_json::from_str::<LlmStructuredOutput>(&content) {
        Ok(structured) => structured.sql,
        Err(_) => content.trim().to_string(), // Fallback to raw content
    };

    let response = LlmResponse { response: sql };

    Ok(Response::from_json(&response)?.with_headers(cors_headers()))
}

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .options("/api/llm", handle_options)
        .post_async("/api/llm", handle_llm_request)
        .run(req, env)
        .await
}
