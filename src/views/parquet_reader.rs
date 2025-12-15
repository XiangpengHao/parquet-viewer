use anyhow::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use dioxus::html::HasFileData;
use dioxus::prelude::*;
use dioxus_primitives::toast::{ToastOptions, use_toast};
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use std::sync::Arc;
use url::form_urlencoded;

use crate::components::ui::{BUTTON_GHOST, BUTTON_OUTLINE, INPUT_BASE, Panel};
use crate::parquet_ctx::{MetadataSummary, ParquetResolved};
use crate::storage::WebFileObjectStore;
use crate::storage::readers;
use crate::utils::{get_stored_value, save_to_storage};

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
        let file_name = file_name_with_extension.strip_suffix(".parquet").unwrap();
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
        tracing::info!(
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
            tracing::info!(
                "Object store {} not found, registering",
                self.object_store_url
            );
            ctx.register_object_store(self.object_store_url.as_ref(), self.object_store.clone());
        } else {
            tracing::info!(
                "Object store {} found, using existing store",
                self.object_store_url
            );
        }

        let url_hash = self
            .object_store_url
            .as_str()
            .replace("://", "_")
            .replace('/', "")
            .replace('-', "_");
        let registered_table_name = format!("{}_{}", self.table_name.as_str(), url_hash); // The unique name for registration in DataFusion 
        ctx.register_parquet(
            format!("\"{}\"", registered_table_name),
            &table_path,
            Default::default(),
        )
        .await?;

        tracing::info!(
            "parquet table: {} has the registered unique name {}",
            self.table_name.as_str(),
            registered_table_name
        );

        let metadata_memory_size = metadata.memory_size();
        Ok(ParquetResolved::new(
            reader,
            self.table_name.as_str().to_string(),
            registered_table_name.clone(),
            self.path_relative_to_object_store,
            self.object_store_url,
            MetadataSummary::from_metadata(
                metadata,
                metadata_memory_size as u64,
                actual_file_size,
                footer_size,
            )?,
        ))
    }
}

#[component]
pub fn ParquetReader(read_call_back: EventHandler<Result<ParquetUnresolved>>) -> Element {
    fn query_param(key: &str) -> Option<String> {
        let window = web_sys::window()?;
        let search = window.location().search().ok()?;
        let search = search.strip_prefix('?').unwrap_or(&search);
        for (k, v) in form_urlencoded::parse(search.as_bytes()) {
            if k == key {
                return Some(v.into_owned());
            }
        }
        None
    }

    let mut active_tab = use_signal(|| {
        if query_param("url").is_some() {
            "url".to_string()
        } else {
            "file".to_string()
        }
    });

    let mut loaded_url = use_signal(|| false);
    if !loaded_url() {
        loaded_url.set(true);
        if let Some(url) = query_param("url") {
            read_call_back.call(readers::read_from_url(&url));
        }
    }

    let tab_button_class = |tab: &str| {
        let base = "py-2 px-1 border-b-2 font-medium";
        if active_tab() == tab {
            format!("{base} border-green-500 text-green-600")
        } else {
            format!(
                "{base} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
            )
        }
    };

    rsx! {
        Panel { class: Some("rounded-lg p-2".to_string()),
            div { class: "border-b border-gray-200 mb-2",
                nav { class: "-mb-px flex flex-col gap-3 md:flex-row md:items-center md:justify-between",
                    div { class: "flex flex-wrap items-center gap-4 md:gap-8",
                        button {
                            class: "{tab_button_class(\"file\")}",
                            onclick: move |_| active_tab.set("file".to_string()),
                            "From file"
                        }
                        button {
                            class: "{tab_button_class(\"url\")}",
                            onclick: move |_| active_tab.set("url".to_string()),
                            "From URL"
                        }
                        button {
                            class: "{tab_button_class(\"s3\")}",
                            onclick: move |_| active_tab.set("s3".to_string()),
                            "From S3"
                        }
                    }
                }
            }
            {
                match active_tab().as_str() {
                    "file" => rsx! {
                        FileReader { read_call_back }
                    },
                    "url" => rsx! {
                        UrlReader { read_call_back }
                    },
                    "s3" => rsx! {
                        S3Reader { read_call_back }
                    },
                    _ => rsx! {
                        FileReader { read_call_back }
                    },
                }
            }
        }
    }
}

#[component]
fn FileReader(read_call_back: EventHandler<Result<ParquetUnresolved>>) -> Element {
    let file_input_id = use_signal(|| format!("file-input-{}", uuid::Uuid::new_v4()));
    let toast_api = use_toast();
    let mut drag_depth = use_signal(|| 0i32);
    let is_dragging = move || drag_depth() > 0;
    let mut selected_file_name = use_signal(|| None::<String>);

    let read_web_file = use_callback(move |file: web_sys::File| {
        let table_name = file.name();
        if !table_name.to_ascii_lowercase().ends_with(".parquet") {
            toast_api.error(
                "Unsupported file type".to_string(),
                ToastOptions::new().description("Please select a `.parquet` file.".to_string()),
            );
            return;
        }

        selected_file_name.set(Some(table_name.clone()));

        let result = (|| {
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
        })();

        read_call_back.call(result);
    });

    let handle_file_data = use_callback(move |file_data: dioxus::html::FileData| {
        let Some(file) = file_data.inner().downcast_ref::<web_sys::File>().cloned() else {
            toast_api.error(
                "Failed to load file".to_string(),
                ToastOptions::new()
                    .description("Browser did not provide a readable file handle.".to_string()),
            );
            return;
        };
        read_web_file.call(file);
    });

    rsx! {
        div {
            class: format!(
                "rounded-lg border-2 border-dashed p-4 transition-colors {}",
                if is_dragging() {
                    "border-green-500 bg-green-50"
                } else {
                    "border-gray-300 bg-white"
                },
            ),
            ondragenter: move |ev| {
                ev.prevent_default();
                drag_depth.set(drag_depth() + 1);
            },
            ondragover: move |ev| {
                ev.prevent_default();
                ev.data_transfer().set_drop_effect("copy");
            },
            ondragleave: move |ev| {
                ev.prevent_default();
                drag_depth.set((drag_depth() - 1).max(0));
            },
            ondrop: move |ev| {
                ev.prevent_default();
                drag_depth.set(0);

                let files = ev.files();
                let file_count = files.len();
                if let Some(file_data) = files.into_iter().next() {
                    if file_count > 1 {
                        toast_api

                            .warning(
                                "Multiple files dropped".to_string(),
                                ToastOptions::new()
                                    .description("Using the first file only.".to_string()),
                            );
                    }
                    handle_file_data.call(file_data);
                    return;
                }
                if let Some(text) = ev
                    .data_transfer()
                    .get_data("text/uri-list")
                    .or_else(|| ev.data_transfer().get_as_text())
                {
                    let candidate = text
                        .lines()
                        .map(str::trim)
                        .find(|line| !line.is_empty() && !line.starts_with('#'));
                    if let Some(url) = candidate {
                        let looks_like_parquet_url = url.contains(".parquet");
                        if looks_like_parquet_url {
                            read_call_back.call(readers::read_from_url(url));
                        } else {
                            toast_api
                                .error(
                                    "Dropped text is not a Parquet URL".to_string(),
                                    ToastOptions::new()
                                        .description(
                                            "Drop a `.parquet` file, or a URL ending in `.parquet`."
                                                .to_string(),
                                        ),
                                );
                        }
                        return;
                    }
                }
                toast_api
                    .error(
                        "Nothing to import".to_string(),
                        ToastOptions::new()
                            .description("Drop a `.parquet` file here.".to_string()),
                    );
            },

            input {
                id: "{file_input_id()}",
                r#type: "file",
                accept: ".parquet",
                class: "hidden",
                onchange: move |ev| {
                    let files = ev.files();
                    let Some(file_data) = files.into_iter().next() else {
                        return;
                    };
                    handle_file_data.call(file_data);
                },
            }

            div { class: "flex flex-col items-center gap-1 text-center",
                div { class: "space-y-0.5",
                    p { class: "text-sm font-medium text-gray-900", "Drop a Parquet file here" }
                }

                label {
                    r#for: "{file_input_id()}",
                    class: "btn btn-outline btn-sm",
                    "Choose file"
                }

                if let Some(name) = selected_file_name() {
                    p { class: "text-xs text-gray-500 mt-1",
                        "Selected: "
                        span { class: "font-mono", "{name}" }
                    }
                }
            }
        }
    }
}

#[component]
pub fn UrlReader(read_call_back: EventHandler<Result<ParquetUnresolved>>) -> Element {
    let mut url = use_signal(|| DEFAULT_URL.to_string());

    rsx! {
        div { class: "h-full flex items-center",
            form {
                class: "w-full",
                onsubmit: move |ev| {
                    ev.prevent_default();
                    read_call_back.call(readers::read_from_url(&url()));
                },
                div { class: "flex flex-col gap-2 sm:flex-row sm:items-center",
                    input {
                        r#type: "url",
                        placeholder: "Enter Parquet file URL",
                        value: "{url()}",
                        class: "flex-1 {INPUT_BASE}",
                        oninput: move |ev| url.set(ev.value()),
                    }
                    button { r#type: "submit", class: "{BUTTON_GHOST}", "Read URL" }
                }
            }
        }
    }
}

#[component]
fn S3Reader(read_call_back: EventHandler<Result<ParquetUnresolved>>) -> Element {
    let mut s3_bucket = use_signal(|| get_stored_value(S3_BUCKET_KEY).unwrap_or_default());
    let mut s3_region =
        use_signal(|| get_stored_value(S3_REGION_KEY).unwrap_or("us-east-1".to_string()));
    let mut s3_file_path = use_signal(|| get_stored_value(S3_FILE_PATH_KEY).unwrap_or_default());

    rsx! {
        div {
            form {
                class: "space-y-3 w-full",
                onsubmit: move |ev| {
                    ev.prevent_default();
                    read_call_back
                        .call(readers::read_from_s3(&s3_bucket(), &s3_region(), &s3_file_path()));
                },
                div { class: "grid grid-cols-1 gap-4 sm:grid-cols-2",
                    div {
                        label { class: "block text-sm font-medium text-gray-700 mb-1",
                            "Bucket"
                        }
                        input {
                            r#type: "text",
                            class: "w-full {INPUT_BASE}",
                            value: "{s3_bucket()}",
                            oninput: move |ev| {
                                let value = ev.value();
                                save_to_storage(S3_BUCKET_KEY, &value);
                                s3_bucket.set(value);
                            },
                        }
                    }
                    div {
                        label { class: "block text-sm font-medium text-gray-700 mb-1",
                            "Region"
                        }
                        input {
                            r#type: "text",
                            class: "w-full {INPUT_BASE}",
                            value: "{s3_region()}",
                            oninput: move |ev| {
                                let value = ev.value();
                                save_to_storage(S3_REGION_KEY, &value);
                                s3_region.set(value);
                            },
                        }
                    }
                    div { class: "sm:col-span-2",
                        label { class: "block text-sm font-medium text-gray-700 mb-1",
                            "File Path"
                        }
                        input {
                            r#type: "text",
                            class: "w-full {INPUT_BASE}",
                            value: "{s3_file_path()}",
                            oninput: move |ev| {
                                let value = ev.value();
                                save_to_storage(S3_FILE_PATH_KEY, &value);
                                s3_file_path.set(value);
                            },
                        }
                    }
                }
                div { class: "flex justify-end",
                    button {
                        r#type: "submit",
                        class: "{BUTTON_OUTLINE} w-full sm:w-auto text-center",
                        "Read S3"
                    }
                }
            }
        }
    }
}
