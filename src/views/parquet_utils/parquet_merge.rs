use arrow_schema::SchemaRef;
use bytes::Bytes;
use dioxus::html::HasFileData;
use dioxus::prelude::*;
use dioxus_primitives::toast::{ToastOptions, use_toast};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::js_sys;

/// Information about a loaded parquet file for merging
#[derive(Clone)]
struct ParquetFileInfo {
    name: String,
    schema: SchemaRef,
    data: Bytes,
    row_count: usize,
    compression: Compression,
}

/// State for the merge operation
#[derive(Clone, Default)]
struct MergeState {
    files: Vec<ParquetFileInfo>,
    is_merging: bool,
    error: Option<String>,
}

impl MergeState {
    fn schemas_match(&self) -> bool {
        if self.files.len() < 2 {
            return true;
        }
        let first_schema = &self.files[0].schema;
        self.files.iter().skip(1).all(|f| f.schema == *first_schema)
    }

    fn total_rows(&self) -> usize {
        self.files.iter().map(|f| f.row_count).sum()
    }

    fn detected_compression(&self) -> Option<Compression> {
        self.files.first().map(|f| f.compression)
    }
}

#[component]
pub fn ParquetMerge() -> Element {
    let toast_api = use_toast();
    let mut state = use_signal(MergeState::default);
    let mut drag_depth = use_signal(|| 0i32);
    let is_dragging = move || drag_depth() > 0;
    let file_input_id = use_signal(|| format!("merge-file-input-{}", uuid::Uuid::new_v4()));

    let add_file = use_callback(move |file_info: ParquetFileInfo| {
        let mut current = state();
        // Check schema compatibility before adding
        if !current.files.is_empty() && current.files[0].schema != file_info.schema {
            state.set(MergeState {
                error: Some(format!(
                    "Schema mismatch: '{}' has a different schema than the first file",
                    file_info.name
                )),
                ..current
            });
            return;
        }
        current.files.push(file_info);
        current.error = None;
        state.set(current);
    });

    let read_web_file = use_callback(move |file: web_sys::File| {
        let file_name = file.name();
        if !file_name.to_ascii_lowercase().ends_with(".parquet") {
            toast_api.error(
                "Unsupported file type".to_string(),
                ToastOptions::new().description("Please select `.parquet` files only.".to_string()),
            );
            return;
        }

        spawn(async move {
            match read_parquet_file_info(file).await {
                Ok(info) => {
                    add_file.call(info);
                }
                Err(e) => {
                    toast_api.error(
                        "Failed to read file".to_string(),
                        ToastOptions::new().description(format!("{}", e)),
                    );
                }
            }
        });
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

    let mut remove_file = move |index: usize| {
        let mut current = state();
        current.files.remove(index);
        current.error = None;
        // Re-validate schemas after removal
        if !current.schemas_match() {
            current.error = Some("Schema mismatch between remaining files".to_string());
        }
        state.set(current);
    };

    let clear_all = move |_| {
        state.set(MergeState::default());
    };

    let do_merge = move |_| {
        let current = state();
        if current.files.len() < 2 {
            toast_api.warning(
                "Not enough files".to_string(),
                ToastOptions::new().description("Add at least 2 files to merge.".to_string()),
            );
            return;
        }

        if !current.schemas_match() {
            toast_api.error(
                "Schema mismatch".to_string(),
                ToastOptions::new()
                    .description("All files must have the same schema to merge.".to_string()),
            );
            return;
        }

        state.set(MergeState {
            is_merging: true,
            ..current.clone()
        });

        spawn(async move {
            match merge_parquet_files(&current.files).await {
                Ok(merged_data) => {
                    download_data("merged.parquet", merged_data);
                    toast_api.success(
                        "Merge complete".to_string(),
                        ToastOptions::new()
                            .description("Your merged file is downloading.".to_string()),
                    );
                    state.set(MergeState {
                        is_merging: false,
                        ..state()
                    });
                }
                Err(e) => {
                    toast_api.error(
                        "Merge failed".to_string(),
                        ToastOptions::new().description(format!("{}", e)),
                    );
                    state.set(MergeState {
                        is_merging: false,
                        error: Some(format!("{}", e)),
                        ..state()
                    });
                }
            }
        });
    };

    let current_state = state();
    let has_files = !current_state.files.is_empty();
    let can_merge = current_state.files.len() >= 2 && current_state.schemas_match();

    rsx! {
        div { class: "space-y-4",
            // Drop zone
            div {
                class: format!(
                    "drop-zone p-8 {}",
                    if is_dragging() { "dragging" } else { "" },
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
                    for file_data in files.into_iter() {
                        handle_file_data.call(file_data);
                    }
                },

                input {
                    id: "{file_input_id()}",
                    r#type: "file",
                    accept: ".parquet",
                    multiple: true,
                    class: "hidden",
                    onchange: move |ev| {
                        let files = ev.files();
                        for file_data in files.into_iter() {
                            handle_file_data.call(file_data);
                        }
                    },
                }

                div { class: "flex flex-col items-center gap-3 text-center",
                    // Upload icon
                    svg {
                        xmlns: "http://www.w3.org/2000/svg",
                        class: "w-8 h-8 text-tertiary",
                        fill: "none",
                        view_box: "0 0 24 24",
                        stroke: "currentColor",
                        stroke_width: "1.5",
                        path {
                            stroke_linecap: "round",
                            stroke_linejoin: "round",
                            d: "M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5",
                        }
                    }

                    div {
                        p { class: "text-primary text-sm font-medium", "Drop Parquet files here" }
                        p { class: "text-tertiary text-xs mt-0.5", "or click to browse" }
                    }

                    label {
                        r#for: "{file_input_id()}",
                        class: "btn-soft text-sm px-4 py-1.5 cursor-pointer",
                        "Choose files"
                    }
                }
            }

            // Error message
            if let Some(error) = &current_state.error {
                div { class: "panel-soft p-3 border-l-2 border-red-400 flex items-start gap-2",
                    svg {
                        xmlns: "http://www.w3.org/2000/svg",
                        class: "w-4 h-4 text-red-500 shrink-0 mt-0.5",
                        fill: "none",
                        view_box: "0 0 24 24",
                        stroke: "currentColor",
                        stroke_width: "1.5",
                        path {
                            stroke_linecap: "round",
                            stroke_linejoin: "round",
                            d: "M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z",
                        }
                    }
                    span { class: "text-sm text-red-600 dark:text-red-400", "{error}" }
                }
            }

            // File list
            if has_files {
                div { class: "space-y-3",
                    div { class: "flex items-center justify-between",
                        span { class: "text-primary text-sm font-medium",
                            "Files ({current_state.files.len()})"
                        }
                        button {
                            class: "text-tertiary text-xs hover:text-primary cursor-pointer",
                            onclick: clear_all,
                            "Clear all"
                        }
                    }

                    // File entries
                    div { class: "space-y-1",
                        for (index, file) in current_state.files.iter().enumerate() {
                            div {
                                key: "{index}-{file.name}",
                                class: "file-item flex items-center justify-between",
                                div { class: "flex items-center gap-3 min-w-0",
                                    // File icon
                                    svg {
                                        xmlns: "http://www.w3.org/2000/svg",
                                        class: "w-4 h-4 text-tertiary shrink-0",
                                        fill: "none",
                                        view_box: "0 0 24 24",
                                        stroke: "currentColor",
                                        stroke_width: "1.5",
                                        path {
                                            stroke_linecap: "round",
                                            stroke_linejoin: "round",
                                            d: "M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z",
                                        }
                                    }
                                    div { class: "min-w-0",
                                        p { class: "text-primary text-sm truncate", "{file.name}" }
                                        p { class: "text-tertiary text-xs",
                                            "{format_rows(file.row_count)} rows"
                                        }
                                    }
                                }
                                button {
                                    class: "text-tertiary hover:text-primary p-1 cursor-pointer",
                                    onclick: move |_| remove_file(index),
                                    title: "Remove",
                                    svg {
                                        xmlns: "http://www.w3.org/2000/svg",
                                        class: "w-4 h-4",
                                        fill: "none",
                                        view_box: "0 0 24 24",
                                        stroke: "currentColor",
                                        stroke_width: "1.5",
                                        path {
                                            stroke_linecap: "round",
                                            stroke_linejoin: "round",
                                            d: "M6 18L18 6M6 6l12 12",
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Summary
                    if current_state.files.len() >= 2 {
                        div { class: "divider-soft my-3" }
                        div { class: "flex items-center justify-between text-sm",
                            span { class: "text-tertiary", "Total rows" }
                            span { class: "text-primary font-medium",
                                "{format_rows(current_state.total_rows())}"
                            }
                        }
                        if let Some(compression) = current_state.detected_compression() {
                            div { class: "flex items-center justify-between text-sm mt-1",
                                span { class: "text-tertiary", "Compression" }
                                span { class: "text-primary",
                                    "{format_compression(compression)}"
                                }
                            }
                        }
                    }
                }
            }

            // Merge button
            if has_files {
                div { class: "pt-2",
                    button {
                        class: if can_merge && !current_state.is_merging {
                            "btn-primary-soft w-full py-2 text-sm font-medium cursor-pointer"
                        } else {
                            "btn-soft w-full py-2 text-sm font-medium opacity-50 cursor-not-allowed"
                        },
                        disabled: !can_merge || current_state.is_merging,
                        onclick: do_merge,
                        if current_state.is_merging {
                            span { class: "flex items-center justify-center gap-2",
                                svg {
                                    class: "animate-spin w-4 h-4",
                                    xmlns: "http://www.w3.org/2000/svg",
                                    fill: "none",
                                    view_box: "0 0 24 24",
                                    circle {
                                        class: "opacity-25",
                                        cx: "12",
                                        cy: "12",
                                        r: "10",
                                        stroke: "currentColor",
                                        stroke_width: "4",
                                    }
                                    path {
                                        class: "opacity-75",
                                        fill: "currentColor",
                                        d: "M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z",
                                    }
                                }
                                "Merging..."
                            }
                        } else {
                            "Merge & Download"
                        }
                    }
                }
            }
        }
    }
}

fn format_rows(count: usize) -> String {
    let mut result = count.to_string();
    let mut i = result.len();
    while i > 3 {
        i -= 3;
        result.insert(i, ',');
    }
    result
}

fn format_compression(compression: Compression) -> &'static str {
    match compression {
        Compression::UNCOMPRESSED => "Uncompressed",
        Compression::SNAPPY => "Snappy",
        Compression::GZIP(_) => "GZIP",
        Compression::LZO => "LZO",
        Compression::BROTLI(_) => "Brotli",
        Compression::LZ4 => "LZ4",
        Compression::ZSTD(_) => "ZSTD",
        Compression::LZ4_RAW => "LZ4 Raw",
    }
}

async fn read_parquet_file_info(file: web_sys::File) -> anyhow::Result<ParquetFileInfo> {
    let name = file.name();

    // Read file as ArrayBuffer
    let array_buffer = JsFuture::from(file.array_buffer())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read file: {:?}", e))?;

    let uint8_array = js_sys::Uint8Array::new(&array_buffer);
    let data = Bytes::from(uint8_array.to_vec());

    // Parse parquet metadata
    let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())?;
    let metadata = builder.metadata();

    let schema = builder.schema().clone();
    let row_count: usize = metadata
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .sum();

    // Get compression from the first column of the first row group
    let compression = metadata
        .row_groups()
        .first()
        .and_then(|rg| rg.columns().first())
        .map(|col| col.compression())
        .unwrap_or(Compression::UNCOMPRESSED);

    Ok(ParquetFileInfo {
        name,
        schema,
        data,
        row_count,
        compression,
    })
}

async fn merge_parquet_files(files: &[ParquetFileInfo]) -> anyhow::Result<Vec<u8>> {
    if files.is_empty() {
        return Err(anyhow::anyhow!("No files to merge"));
    }

    let schema = files[0].schema.clone();
    let compression = files[0].compression;

    // Create writer with matching compression
    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;

    // Read and write each file's record batches
    for file in files {
        let builder = ParquetRecordBatchReaderBuilder::try_new(file.data.clone())?;
        let reader = builder.build()?;

        for batch_result in reader {
            let batch = batch_result?;
            writer.write(&batch)?;
        }
    }

    writer.close()?;

    Ok(buf)
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
