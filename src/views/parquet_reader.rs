use anyhow::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use leptos::{logging, prelude::*};
use leptos_router::hooks::{query_signal, use_query_map};
use object_store::ObjectStore;
use object_store::path::Path;
use object_store_opendal::OpendalStore;
use opendal::{Operator, services::Http, services::S3};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use std::sync::Arc;
use url::Url;
use web_sys::js_sys;

use crate::object_store_cache::ObjectStoreCache;
use crate::parquet_ctx::{MetadataDisplay, ParquetResolved};
use crate::utils::{get_stored_value, save_to_storage};
use crate::views::web_file_store::WebFileObjectStore;

const S3_ENDPOINT_KEY: &str = "s3_endpoint";
const S3_ACCESS_KEY_ID_KEY: &str = "s3_access_key_id";
const S3_SECRET_KEY_KEY: &str = "s3_secret_key";
const S3_BUCKET_KEY: &str = "s3_bucket";
const S3_REGION_KEY: &str = "s3_region";
const S3_FILE_PATH_KEY: &str = "s3_file_path";

const DEFAULT_URL: &str = "https://huggingface.co/datasets/open-r1/OpenR1-Math-220k/resolve/main/data/train-00003-of-00010.parquet";

#[derive(Clone)]
pub struct TableNameWithoutExtension {
    table_name: String,
}

impl TableNameWithoutExtension {
    fn from_parquet_file(file_name_with_extension: String) -> Result<Self> {
        if !file_name_with_extension.ends_with(".parquet") {
            return Err(anyhow::anyhow!("File name must end with .parquet"));
        }
        let file_name = file_name_with_extension.split('.').next().unwrap();
        Ok(Self {
            table_name: file_name.to_string(),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.table_name
    }
}

#[derive(Clone)]
pub struct ParquetUnresolved {
    pub table_name: TableNameWithoutExtension,
    pub path_relative_to_object_store: Path,
    pub object_store_url: ObjectStoreUrl,
    pub object_store: Arc<dyn ObjectStore>,
}

impl ParquetUnresolved {
    pub(crate) fn try_new(
        file_name_with_extension: String,
        path_relative_to_object_store: Path,
        object_store_url: ObjectStoreUrl,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        logging::log!(
            "Creating ParquetUnresolved: {:?}, {:?}, {:?}",
            file_name_with_extension,
            path_relative_to_object_store,
            object_store_url,
        );
        let table_name = TableNameWithoutExtension::from_parquet_file(file_name_with_extension)?;
        Ok(Self {
            table_name,
            path_relative_to_object_store,
            object_store_url,
            object_store,
        })
    }
    /// The table path used to register_parquet in DataFusion
    pub fn table_path(&self) -> String {
        format!(
            "{}{}",
            self.object_store_url, self.path_relative_to_object_store
        )
    }

    pub async fn try_into_resolved(self, ctx: &SessionContext) -> Result<ParquetResolved> {
        // Get the actual file size from the object store
        let file_meta = self
            .object_store
            .head(&self.path_relative_to_object_store)
            .await?;
        let actual_file_size = file_meta.size;

        // Get the footer size by reading the last 8 bytes and decoding the metadata length
        let footer_size = {
            use parquet::file::FOOTER_SIZE;

            let footer_bytes = self
                .object_store
                .get_range(
                    &self.path_relative_to_object_store,
                    (actual_file_size - FOOTER_SIZE as u64)..actual_file_size,
                )
                .await?;

            // Decode the footer to get the metadata length
            let footer_tail = &footer_bytes[footer_bytes.len() - FOOTER_SIZE..];
            let metadata_len = u32::from_le_bytes([
                footer_tail[0],
                footer_tail[1],
                footer_tail[2],
                footer_tail[3],
            ]) as u64;

            metadata_len + FOOTER_SIZE as u64
        };

        let mut reader = ParquetObjectReader::new(
            self.object_store.clone(),
            self.path_relative_to_object_store.clone(),
        )
        .with_preload_column_index(true)
        .with_preload_offset_index(true);

        let metadata = reader.get_metadata(None).await?;

        let table_path = self.table_path();

        if ctx
            .runtime_env()
            .object_store(&self.object_store_url)
            .is_err()
        {
            logging::log!(
                "Object store {} not found, registering",
                self.object_store_url
            );
            ctx.register_object_store(self.object_store_url.as_ref(), self.object_store.clone());
        } else {
            logging::log!(
                "Object store {} found, using existing store",
                self.object_store_url
            );
        }
        ctx.register_parquet(self.table_name.as_str(), &table_path, Default::default())
            .await?;

        logging::log!("registered parquet table: {}", self.table_name.as_str());

        let metadata_memory_size = metadata.memory_size();
        Ok(ParquetResolved::new(
            reader,
            self.table_name.as_str().to_string(),
            self.path_relative_to_object_store,
            self.object_store_url,
            MetadataDisplay::from_metadata(
                metadata,
                metadata_memory_size as u64,
                actual_file_size,
                footer_size,
            )?,
        ))
    }
}

pub(crate) fn read_from_vscode(
    obj: js_sys::Object,
    call_back: impl Fn(Result<ParquetUnresolved>) + 'static + Send + Copy,
) {
    let url = js_sys::Reflect::get(&obj, &"url".into()).unwrap();
    let url = url.as_string().unwrap();
    let file_name = js_sys::Reflect::get(&obj, &"filename".into()).unwrap();
    let file_name = file_name.as_string().unwrap();

    leptos::task::spawn_local({
        let url = url.clone();
        let file_name = file_name.clone();
        logging::log!("Reading from VS Code: {}, {}", url, file_name);
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

#[component]
pub fn ParquetReader(
    read_call_back: impl Fn(Result<ParquetUnresolved>) + 'static + Send + Copy + Sync,
) -> impl IntoView {
    let default_tab = {
        let query = use_query_map();
        let url = query.get().get("url");
        if url.is_some() { "url" } else { "file" }
    };
    let (active_tab, set_active_tab) = signal(default_tab.to_string());

    let set_active_tab_fn = move |tab: &str| {
        if active_tab.get() != tab {
            set_active_tab.set(tab.to_string());
        }
    };

    if let Some(url) = use_query_map().get().get("url") {
        let parquet_info = read_from_url(&url);
        read_call_back(parquet_info);
    }

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-2">
            <div class="border-b border-gray-200 mb-4">
                <nav class="-mb-px flex justify-between items-center">
                    <div class="flex space-x-8">
                        <button
                            class=move || {
                                let base = "py-2 px-1 border-b-2 font-medium";
                                if active_tab.get() == "file" {
                                    return format!("{base} border-green-500 text-green-600");
                                }
                                format!(
                                    "{base} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300",
                                )
                            }
                            on:click=move |_| set_active_tab_fn("file")
                        >
                            "From file"
                        </button>
                        <button
                            class=move || {
                                let base = "py-2 px-1 border-b-2 font-medium";
                                if active_tab.get() == "url" {
                                    return format!("{base} border-green-500 text-green-600");
                                }
                                format!(
                                    "{base} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300",
                                )
                            }
                            on:click=move |_| set_active_tab_fn("url")
                        >
                            "From URL"
                        </button>
                        <button
                            class=move || {
                                let base = "py-2 px-1 border-b-2 font-medium";
                                if active_tab.get() == "s3" {
                                    return format!("{base} border-green-500 text-green-600");
                                }
                                format!(
                                    "{base} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300",
                                )
                            }
                            on:click=move |_| set_active_tab_fn("s3")
                        >
                            "From S3"
                        </button>
                    </div>
                    <div class="text-xs text-gray-400">
                        <a
                            href="https://xiangpeng.systems/fund/"
                            target="_blank"
                            class="text-blue-400 hover:text-blue-600"
                        >
                            "Funded"
                        </a>
                        " by "
                        <a
                            href="https://www.influxdata.com"
                            target="_blank"
                            class="text-blue-400 hover:text-blue-600"
                        >
                            "InfluxData"
                        </a>
                    </div>
                </nav>
            </div>
            {
                view! {
                    <Show when=move || active_tab.get() == "file">
                        <FileReader read_call_back=read_call_back />
                    </Show>
                    <Show when=move || active_tab.get() == "url">
                        <UrlReader read_call_back=read_call_back />
                    </Show>
                    <Show when=move || active_tab.get() == "s3">
                        <S3Reader read_call_back=read_call_back />
                    </Show>
                }
            }
        </div>
    }
}

#[component]
fn FileReader(
    read_call_back: impl Fn(Result<ParquetUnresolved>) + 'static + Send + Copy,
) -> impl IntoView {
    let on_file_select = move |ev: web_sys::Event| {
        let input: web_sys::HtmlInputElement = event_target(&ev);
        let files = input.files().unwrap();
        let file = files.get(0).unwrap();
        let table_name = file.name();

        leptos::task::spawn_local(async move {
            let result = async {
                let path_relative_to_object_store = Path::parse(&table_name)?;
                let uuid = uuid::Uuid::new_v4();
                let object_store = Arc::new(WebFileObjectStore::new(file));
                let object_store_url = ObjectStoreUrl::parse(format!("webfile://{uuid}"))?;

                ParquetUnresolved::try_new(
                    table_name.clone(),
                    path_relative_to_object_store,
                    object_store_url,
                    object_store,
                )
            }
            .await;

            read_call_back(result);
        });
    };

    view! {
        <div class="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center space-y-4">
            <div>
                <input type="file" accept=".parquet" on:change=on_file_select id="file-input" />
            </div>
            <div>
                <label for="file-input" class="cursor-pointer text-gray-600">
                    "Drop Parquet file or click to browse"
                </label>
            </div>
        </div>
    }
}

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

    let builder = Http::default().endpoint(&endpoint);
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

#[component]
pub fn UrlReader(
    read_call_back: impl Fn(Result<ParquetUnresolved>) + 'static + Send + Copy,
) -> impl IntoView {
    let (url_query, set_url_query) = query_signal::<String>("url");
    let default_url = {
        if let Some(url) = url_query.get() {
            url
        } else {
            DEFAULT_URL.to_string()
        }
    };

    let (url, set_url) = signal(default_url);

    let on_url_submit = move || {
        let url_str = url.get();
        set_url_query.set(Some(url_str.clone()));

        let parquet_info = read_from_url(&url_str);
        read_call_back(parquet_info);
    };

    view! {
        <div class="h-full flex items-center">
            <form
                on:submit=move |ev| {
                    ev.prevent_default();
                    on_url_submit();
                }
                class="w-full"
            >
                <div class="flex space-x-2">
                    <input
                        type="url"
                        placeholder="Enter Parquet file URL"
                        on:input=move |ev| {
                            set_url.set(event_target_value(&ev));
                        }
                        prop:value=url
                        class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                    />
                    <button
                        type="submit"
                        class="px-4 py-2 border border-green-500 text-green-500 rounded-md hover:bg-green-50"
                    >
                        "Read URL"
                    </button>
                </div>
            </form>
        </div>
    }
}

fn read_from_s3(s3_bucket: &str, s3_region: &str, s3_file_path: &str) -> Result<ParquetUnresolved> {
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

#[component]
fn S3Reader(
    read_call_back: impl Fn(Result<ParquetUnresolved>) + 'static + Send + Copy,
) -> impl IntoView {
    let (s3_bucket, set_s3_bucket) = signal(get_stored_value(S3_BUCKET_KEY).unwrap_or_default());
    let (s3_region, set_s3_region) =
        signal(get_stored_value(S3_REGION_KEY).unwrap_or("us-east-1".to_string()));
    let (s3_file_path, set_s3_file_path) =
        signal(get_stored_value(S3_FILE_PATH_KEY).unwrap_or_default());

    let on_s3_bucket_change = move |ev| {
        let value = event_target_value(&ev);
        save_to_storage(S3_BUCKET_KEY, &value);
        set_s3_bucket.set(value);
    };

    let on_s3_region_change = move |ev| {
        let value = event_target_value(&ev);
        save_to_storage(S3_REGION_KEY, &value);
        set_s3_region.set(value);
    };

    let on_s3_file_path_change = move |ev| {
        let value = event_target_value(&ev);
        save_to_storage(S3_FILE_PATH_KEY, &value);
        set_s3_file_path.set(value);
    };

    let on_s3_submit = move || {
        let parquet_info = read_from_s3(&s3_bucket.get(), &s3_region.get(), &s3_file_path.get());
        read_call_back(parquet_info);
    };

    view! {
        <div>
            <form
                on:submit=move |ev| {
                    ev.prevent_default();
                    on_s3_submit();
                }
                class="space-y-4 w-full"
            >
                <div class="flex flex-wrap gap-4">
                    <div class="flex-1 min-w-[200px] max-w-[200px]">
                        <label class="block text-sm font-medium text-gray-700 mb-1">"Bucket"</label>
                        <input
                            type="text"
                            on:input=on_s3_bucket_change
                            prop:value=s3_bucket
                            class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                        />
                    </div>
                    <div class="flex-1 min-w-[150px] max-w-[150px]">
                        <label class="block text-sm font-medium text-gray-700 mb-1">"Region"</label>
                        <input
                            type="text"
                            on:input=on_s3_region_change
                            prop:value=s3_region
                            class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                        />
                    </div>
                    <div class="flex-[2] min-w-[250px]">
                        <label class="block text-sm font-medium text-gray-700 mb-1">
                            "File Path"
                        </label>
                        <input
                            type="text"
                            on:input=on_s3_file_path_change
                            prop:value=s3_file_path
                            class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                        />
                    </div>
                    <div class="flex-1 min-w-[120px] max-w-[120px] self-end">
                        <button
                            type="submit"
                            class="w-full px-4 py-2 border border-green-500 text-green-500 rounded-md hover:border-green-600 hover:text-green-600"
                        >
                            "Read S3"
                        </button>
                    </div>
                </div>
            </form>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
