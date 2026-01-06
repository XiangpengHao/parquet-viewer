use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tokio_util::io::ReaderStream;
use tower_http::cors::{Any, CorsLayer};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

const VIEWER_URL: &str = "https://parquet-viewer.xiangpeng.systems";

#[derive(Parser, Debug)]
#[command(name = "parquet-viewer-cli")]
#[command(about = "Serve a local parquet file and open it in parquet-viewer")]
#[command(version)]
struct Args {
    /// Path to the parquet file to serve
    file: PathBuf,

    /// Port to serve the file on (default: random available port)
    #[arg(short, long)]
    port: Option<u16>,

    /// Don't open the browser automatically
    #[arg(long)]
    no_open: bool,

    /// Bind address (default: 0.0.0.0)
    #[arg(short, long, default_value = "0.0.0.0")]
    bind: String,
}

#[derive(Clone)]
struct AppState {
    file_path: PathBuf,
    file_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    // Validate the file exists and is a parquet file
    let file_path = args.file.canonicalize().context("File not found")?;
    if !file_path.is_file() {
        anyhow::bail!("Path is not a file: {}", file_path.display());
    }

    let file_name = file_path
        .file_name()
        .context("Could not get file name")?
        .to_string_lossy()
        .to_string();

    let state = Arc::new(AppState {
        file_path,
        file_name: file_name.clone(),
    });

    // Setup CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
        .allow_headers(Any)
        .expose_headers([
            header::CONTENT_LENGTH,
            header::CONTENT_RANGE,
            header::ACCEPT_RANGES,
        ]);

    let app = Router::new()
        .route("/{file_name}", get(serve_file).head(serve_file_head))
        .layer(cors)
        .with_state(state.clone());

    // Bind to the specified port or use a random one
    let addr: SocketAddr = format!("{}:{}", args.bind, args.port.unwrap_or(0)).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    info!("Serving {} on http://{}", file_name, local_addr);

    // Construct the viewer URL
    let file_url = format!(
        "http://localhost:{}/{}",
        local_addr.port(),
        urlencoding::encode(&file_name)
    );
    let viewer_url = format!("{}/?url={}", VIEWER_URL, urlencoding::encode(&file_url));

    info!("Opening viewer at: {}", viewer_url);

    if !args.no_open {
        if let Err(e) = open::that(&viewer_url) {
            tracing::warn!(
                "Failed to open browser: {}. Please open the URL manually.",
                e
            );
        }
    }

    println!("\nServing: {}", state.file_path.display());
    println!("Local URL: {}", file_url);
    println!("Viewer URL: {}", viewer_url);
    println!("\nPress Ctrl+C to stop the server.");

    axum::serve(listener, app).await?;

    Ok(())
}

async fn serve_file_head(
    State(state): State<Arc<AppState>>,
    Path(requested_file): Path<String>,
) -> Response {
    if urlencoding::decode(&requested_file).unwrap_or_default() != state.file_name {
        return StatusCode::NOT_FOUND.into_response();
    }

    let metadata = match tokio::fs::metadata(&state.file_path).await {
        Ok(m) => m,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(metadata.len()));
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );

    (StatusCode::OK, headers).into_response()
}

async fn serve_file(
    State(state): State<Arc<AppState>>,
    Path(requested_file): Path<String>,
    headers: HeaderMap,
) -> Response {
    if urlencoding::decode(&requested_file).unwrap_or_default() != state.file_name {
        return StatusCode::NOT_FOUND.into_response();
    }

    let mut file = match File::open(&state.file_path).await {
        Ok(f) => f,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let metadata = match file.metadata().await {
        Ok(m) => m,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let file_size = metadata.len();

    // Check for Range header
    if let Some(range_header) = headers.get(header::RANGE) {
        if let Ok(range_str) = range_header.to_str() {
            if let Some(range) = parse_range(range_str, file_size) {
                let (start, end) = range;
                let length = end - start + 1;

                if file.seek(std::io::SeekFrom::Start(start)).await.is_err() {
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }

                let limited_file = file.take(length);
                let stream = ReaderStream::new(limited_file);
                let body = Body::from_stream(stream);

                let mut response_headers = HeaderMap::new();
                response_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(length));
                response_headers.insert(
                    header::CONTENT_RANGE,
                    HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, file_size))
                        .unwrap(),
                );
                response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                response_headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                );

                return (StatusCode::PARTIAL_CONTENT, response_headers, body).into_response();
            }
        }
    }

    // No range request, serve the entire file
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(file_size));
    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );

    (StatusCode::OK, response_headers, body).into_response()
}

fn parse_range(range_header: &str, file_size: u64) -> Option<(u64, u64)> {
    let range_str = range_header.strip_prefix("bytes=")?;

    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let start: u64 = if parts[0].is_empty() {
        // Suffix range: -500 means last 500 bytes
        let suffix_length: u64 = parts[1].parse().ok()?;
        file_size.saturating_sub(suffix_length)
    } else {
        parts[0].parse().ok()?
    };

    let end: u64 = if parts[1].is_empty() {
        file_size - 1
    } else {
        parts[1].parse().ok()?
    };

    if start <= end && start < file_size {
        Some((start, end.min(file_size - 1)))
    } else {
        None
    }
}
