use anyhow::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use dioxus::prelude::*;
use object_store::path::Path;
use object_store_opendal::OpendalStore;
use opendal::{Operator, services::Http, services::S3};
use std::sync::Arc;
use url::Url;
use web_sys::js_sys;

use crate::storage::ObjectStoreCache;
use crate::utils::get_stored_value;
use crate::views::parquet_reader::ParquetUnresolved;
use crate::views::settings::S3_ACCESS_KEY_ID_KEY;
use crate::views::settings::S3_ENDPOINT_KEY;
use crate::views::settings::S3_SECRET_KEY_KEY;

/// Reads a parquet file from a URL and returns a ParquetInfo object.
/// This function parses the URL, creates an HTTP object store, and returns
/// the necessary information to read the parquet file.
pub fn read_from_url(url_str: &str) -> Result<ParquetUnresolved> {
    let url = Url::parse(url_str)?;
    let endpoint = format!(
        "{}://{}{}",
        url.scheme(),
        url.host_str().ok_or(anyhow::anyhow!("Empty host"))?,
        url.port().map_or("".to_string(), |p| format!(":{p}"))
    );
    let path = url.path().to_string();

    let table_name = path
        .split('/')
        .next_back()
        .unwrap_or("uploaded.parquet")
        .to_string();

    let builder = {
        let mut http_builder = Http::default().endpoint(&endpoint);
        let username = url.username();
        if !username.is_empty() {
            http_builder = http_builder.username(username);
        }
        if let Some(password) = url.password() {
            http_builder = http_builder.password(password);
        }
        http_builder
    };
    let op = Operator::new(builder)?;
    let op = op.finish();
    let object_store = Arc::new(ObjectStoreCache::new(OpendalStore::new(op)));
    let object_store_url = ObjectStoreUrl::parse(&endpoint)?;
    ParquetUnresolved::try_new(
        table_name.clone(),
        Path::parse(path)?,
        object_store_url,
        object_store,
    )
}

pub(crate) fn read_from_s3(
    s3_bucket: &str,
    s3_region: &str,
    s3_file_path: &str,
) -> Result<ParquetUnresolved> {
    let endpoint =
        get_stored_value(S3_ENDPOINT_KEY).unwrap_or("https://s3.amazonaws.com".to_string());
    let access_key_id = get_stored_value(S3_ACCESS_KEY_ID_KEY).unwrap_or_default();
    let secret_key = get_stored_value(S3_SECRET_KEY_KEY).unwrap_or_default();

    // Validate inputs
    if endpoint.is_empty() || s3_bucket.is_empty() || s3_file_path.is_empty() {
        return Err(anyhow::anyhow!("All fields except region are required",));
    }
    let file_name = s3_file_path
        .split('/')
        .next_back()
        .unwrap_or("uploaded.parquet")
        .to_string();

    let cfg = S3::default()
        .endpoint(&endpoint)
        .access_key_id(&access_key_id)
        .secret_access_key(&secret_key)
        .bucket(s3_bucket)
        .region(s3_region);

    let path = format!("s3://{s3_bucket}");

    let op = Operator::new(cfg)?.finish();
    let object_store = Arc::new(ObjectStoreCache::new(OpendalStore::new(op)));
    let object_store_url = ObjectStoreUrl::parse(&path)?;
    ParquetUnresolved::try_new(
        file_name.clone(),
        Path::parse(s3_file_path)?,
        object_store_url,
        object_store.clone(),
    )
}

pub(crate) fn read_from_vscode(
    obj: js_sys::Object,
    call_back: impl Fn(Result<ParquetUnresolved>) + 'static,
) {
    let url = js_sys::Reflect::get(&obj, &"url".into()).unwrap();
    let url = url.as_string().unwrap();
    let file_name = js_sys::Reflect::get(&obj, &"filename".into()).unwrap();
    let file_name = file_name.as_string().unwrap();

    spawn({
        let url = url.clone();
        let file_name = file_name.clone();
        tracing::info!("Reading from VS Code: {}, {}", url, file_name);
        async move {
            let result = async {
                let url = Url::parse(&url)?;
                let endpoint = format!(
                    "{}://{}{}",
                    url.scheme(),
                    url.host_str().ok_or(anyhow::anyhow!("Empty host"))?,
                    url.port().map_or("".to_string(), |p| format!(":{p}"))
                );
                let path = url.path().to_string();

                let builder = Http::default().endpoint(&endpoint);
                let op = Operator::new(builder)?;
                let op = op.finish();
                let object_store = Arc::new(OpendalStore::new(op));
                let object_store_url = ObjectStoreUrl::parse(&endpoint)?;
                ParquetUnresolved::try_new(
                    file_name.clone(),
                    Path::parse(path)?,
                    object_store_url,
                    object_store,
                )
            }
            .await;

            call_back(result);
        }
    });
}

#[cfg(test)]
mod tests {
    use crate::storage::readers::read_from_url;

    #[test]
    fn test_read_from_url_non_parquet() {
        let url = "not-a-url";
        let result = read_from_url(url);
        assert!(result.is_err(), "Should fail for an invalid URL");

        let url = "https://example.com/file.csv";
        let result = read_from_url(url);

        assert!(result.is_err(), "Should fail for non-parquet files");

        let url = "file:///path/to/file.parquet";
        let result = read_from_url(url);

        assert!(result.is_err(), "Should fail for URLs without a host");
    }

    #[test]
    fn test_read_from_url_valid_parquet_url() {
        // This test uses a known public Parquet file
        let url = "https://raw.githubusercontent.com/tobilg/aws-edge-locations/main/data/aws-edge-locations.parquet";
        let result = read_from_url(url);

        let result = result.expect("Should successfully parse a valid parquet URL");

        assert_eq!(result.table_name.as_str(), "aws-edge-locations",);
        assert_eq!(
            result.path_relative_to_object_store.to_string(),
            "tobilg/aws-edge-locations/main/data/aws-edge-locations.parquet",
        );
        assert_eq!(
            result.object_store_url.to_string(),
            "https://raw.githubusercontent.com/",
        );
    }
}
