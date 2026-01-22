use arrow_schema::SchemaRef;
use bytes::Bytes;
use dioxus::html::HasFileData;
use dioxus::prelude::*;
use dioxus_primitives::toast::{ToastOptions, use_toast};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::{
    DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT, DEFAULT_PAGE_SIZE, EnabledStatistics, WriterProperties,
};
use parquet::schema::types::ColumnPath;
use std::collections::HashMap;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::js_sys;

const DEFAULT_ROW_GROUP_SIZE: usize = 256 * 1024;

/// Information about a loaded parquet file for rewriting
#[derive(Clone)]
struct ParquetFileInfo {
    name: String,
    schema: SchemaRef,
    data: Bytes,
    row_count: usize,
    compression: Compression,
    size_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum CompressionChoice {
    Zstd,
    Snappy,
    Gzip,
    Brotli,
    Lz4,
    #[default]
    Lz4Raw,
    Uncompressed,
}

impl CompressionChoice {
    fn all() -> &'static [CompressionChoice] {
        &[
            CompressionChoice::Lz4Raw,
            CompressionChoice::Zstd,
            CompressionChoice::Snappy,
            CompressionChoice::Gzip,
            CompressionChoice::Brotli,
            CompressionChoice::Lz4,
            CompressionChoice::Uncompressed,
        ]
    }

    fn value(&self) -> &'static str {
        match self {
            CompressionChoice::Zstd => "zstd",
            CompressionChoice::Snappy => "snappy",
            CompressionChoice::Gzip => "gzip",
            CompressionChoice::Brotli => "brotli",
            CompressionChoice::Lz4 => "lz4",
            CompressionChoice::Lz4Raw => "lz4_raw",
            CompressionChoice::Uncompressed => "uncompressed",
        }
    }

    fn label(&self) -> &'static str {
        match self {
            CompressionChoice::Zstd => "ZSTD",
            CompressionChoice::Snappy => "Snappy",
            CompressionChoice::Gzip => "Gzip",
            CompressionChoice::Brotli => "Brotli",
            CompressionChoice::Lz4 => "LZ4 (legacy)",
            CompressionChoice::Lz4Raw => "LZ4 Raw (default)",
            CompressionChoice::Uncompressed => "Uncompressed",
        }
    }

    fn from_value(value: &str) -> Option<Self> {
        match value {
            "zstd" => Some(CompressionChoice::Zstd),
            "snappy" => Some(CompressionChoice::Snappy),
            "gzip" => Some(CompressionChoice::Gzip),
            "brotli" => Some(CompressionChoice::Brotli),
            "lz4" => Some(CompressionChoice::Lz4),
            "lz4_raw" => Some(CompressionChoice::Lz4Raw),
            "uncompressed" => Some(CompressionChoice::Uncompressed),
            _ => None,
        }
    }

    fn to_parquet(self) -> Compression {
        match self {
            CompressionChoice::Zstd => Compression::ZSTD(Default::default()),
            CompressionChoice::Snappy => Compression::SNAPPY,
            CompressionChoice::Gzip => Compression::GZIP(Default::default()),
            CompressionChoice::Brotli => Compression::BROTLI(Default::default()),
            CompressionChoice::Lz4 => Compression::LZ4,
            CompressionChoice::Lz4Raw => Compression::LZ4_RAW,
            CompressionChoice::Uncompressed => Compression::UNCOMPRESSED,
        }
    }
}

#[derive(Clone)]
struct RewriteSettings {
    compression: CompressionChoice,
    data_page_size: usize,
    dictionary_page_size: usize,
    row_group_size: usize,
    page_index_enabled: bool,
    bloom_filter_enabled: bool,
    per_column_compression: bool,
    column_compressions: HashMap<String, CompressionChoice>,
}

impl Default for RewriteSettings {
    fn default() -> Self {
        Self {
            compression: CompressionChoice::default(),
            data_page_size: DEFAULT_PAGE_SIZE,
            dictionary_page_size: DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            page_index_enabled: true,
            bloom_filter_enabled: false,
            per_column_compression: false,
            column_compressions: HashMap::new(),
        }
    }
}

/// State for the rewrite operation
#[derive(Clone, Default)]
struct RewriteState {
    files: Vec<ParquetFileInfo>,
    is_rewriting: bool,
    error: Option<String>,
}

impl RewriteState {
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
}

#[component]
pub fn ParquetRewriterTool() -> Element {
    let toast_api = use_toast();
    let mut state = use_signal(RewriteState::default);
    let mut settings = use_signal(RewriteSettings::default);
    let mut drag_depth = use_signal(|| 0i32);
    let is_dragging = move || drag_depth() > 0;
    let file_input_id = use_signal(|| format!("rewrite-file-input-{}", uuid::Uuid::new_v4()));

    let add_file = use_callback(move |file_info: ParquetFileInfo| {
        let mut current = state();
        if !current.files.is_empty() && current.files[0].schema != file_info.schema {
            state.set(RewriteState {
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
        if !current.schemas_match() {
            current.error = Some("Schema mismatch between remaining files".to_string());
        }
        state.set(current);
    };

    let clear_all = move |_| {
        state.set(RewriteState::default());
    };

    let reset_settings = move |_| {
        settings.set(RewriteSettings::default());
    };

    let update_page_size = move |ev: Event<FormData>| {
        if let Ok(value) = ev.value().parse::<usize>()
            && value > 0
        {
            settings.with_mut(|current| current.data_page_size = value);
        }
    };

    let update_row_group_size = move |ev: Event<FormData>| {
        if let Ok(value) = ev.value().parse::<usize>()
            && value > 0
        {
            settings.with_mut(|current| current.row_group_size = value);
        }
    };

    let update_dictionary_page_size = move |ev: Event<FormData>| {
        if let Ok(value) = ev.value().parse::<usize>()
            && value > 0
        {
            settings.with_mut(|current| current.dictionary_page_size = value);
        }
    };

    let update_compression = move |ev: Event<FormData>| {
        if let Some(choice) = CompressionChoice::from_value(&ev.value()) {
            settings.with_mut(|current| current.compression = choice);
        }
    };

    let toggle_page_index = move |ev: Event<FormData>| {
        let enabled = ev.checked();
        settings.with_mut(|current| current.page_index_enabled = enabled);
    };

    let toggle_bloom_filter = move |ev: Event<FormData>| {
        let enabled = ev.checked();
        settings.with_mut(|current| current.bloom_filter_enabled = enabled);
    };

    let toggle_per_column_compression = move |ev: Event<FormData>| {
        let enabled = ev.checked();
        settings.with_mut(|current| current.per_column_compression = enabled);
    };

    let do_rewrite = move |_| {
        let current = state();
        if current.files.is_empty() {
            toast_api.warning(
                "No files".to_string(),
                ToastOptions::new().description("Add at least 1 Parquet file.".to_string()),
            );
            return;
        }

        if !current.schemas_match() {
            toast_api.error(
                "Schema mismatch".to_string(),
                ToastOptions::new().description(
                    "All files must have the same schema to rewrite into a single file."
                        .to_string(),
                ),
            );
            return;
        }

        state.set(RewriteState {
            is_rewriting: true,
            ..current.clone()
        });

        let active_settings = settings();

        spawn(async move {
            match rewrite_parquet_files(&current.files, &active_settings).await {
                Ok(rewritten_data) => {
                    download_data("rewritten.parquet", rewritten_data);
                    toast_api.success(
                        "Rewrite complete".to_string(),
                        ToastOptions::new()
                            .description("Your rewritten file is downloading.".to_string()),
                    );
                    state.set(RewriteState {
                        is_rewriting: false,
                        ..state()
                    });
                }
                Err(e) => {
                    toast_api.error(
                        "Rewrite failed".to_string(),
                        ToastOptions::new().description(format!("{}", e)),
                    );
                    state.set(RewriteState {
                        is_rewriting: false,
                        error: Some(format!("{}", e)),
                        ..state()
                    });
                }
            }
        });
    };

    let current_state = state();
    let current_settings = settings();
    let has_files = !current_state.files.is_empty();
    let can_rewrite = has_files && current_state.schemas_match();
    let column_names: Vec<String> = current_state
        .files
        .first()
        .map(|file| {
            file.schema
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect()
        })
        .unwrap_or_default();
    let column_rows: Vec<(String, String)> = column_names
        .iter()
        .map(|name| {
            let override_value = current_settings
                .column_compressions
                .get(name)
                .map(|choice| choice.value().to_string())
                .unwrap_or_else(|| "default".to_string());
            (name.clone(), override_value)
        })
        .collect();

    rsx! {
        div { class: "space-y-6 select-text",
            div { class: "space-y-1",
                h1 { class: "text-primary text-xl font-semibold tracking-tight select-text",
                    "Parquet Rewriter"
                }
                p { class: "text-tertiary text-sm select-text",
                    "Upload one or more Parquet files, tune writer settings, and download a single rewritten output."
                }
            }

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
                    span { class: "text-sm text-red-600 dark:text-red-400 select-text",
                        "{error}"
                    }
                }
            }

            div { class: "grid gap-4 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]",
                div { class: "panel-soft p-4 space-y-4",
                    div { class: "flex items-center justify-between",
                        div { class: "space-y-0.5",
                            h2 { class: "text-primary text-sm font-semibold select-text",
                                "Source files"
                            }
                            p { class: "text-tertiary text-xs select-text",
                                "Files must share the same schema to combine into one output."
                            }
                        }
                        if has_files {
                            button {
                                class: "btn-soft text-xs select-text",
                                onclick: clear_all,
                                "Clear all"
                            }
                        }
                    }

                    div {
                        class: format!("drop-zone p-6 {}", if is_dragging() { "dragging" } else { "" }),
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

                        div { class: "flex flex-col items-center gap-2 text-center",
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
                                p { class: "text-primary text-sm font-medium select-text",
                                    "Drop Parquet files here"
                                }
                                p { class: "text-tertiary text-xs mt-0.5 select-text",
                                    "or click to browse"
                                }
                            }

                            label {
                                r#for: "{file_input_id()}",
                                class: "btn-soft text-xs px-3 py-1.5 cursor-pointer select-text",
                                "Choose files"
                            }
                        }
                    }

                    if has_files {
                        div { class: "space-y-2",
                            div { class: "flex items-center justify-between",
                                span { class: "text-primary text-xs font-medium select-text",
                                    "Files ({current_state.files.len()})"
                                }
                                if !current_state.schemas_match() {
                                    span { class: "text-red-500 text-xs select-text",
                                        "Schema mismatch"
                                    }
                                }
                            }

                            div { class: "space-y-1",
                                for (index , file) in current_state.files.iter().enumerate() {
                                    div {
                                        key: "{index}-{file.name}",
                                        class: "file-item flex items-center justify-between gap-3",
                                        div { class: "min-w-0",
                                            p { class: "text-primary text-sm truncate select-text",
                                                "{file.name}"
                                            }
                                            div { class: "flex flex-wrap items-center gap-2 text-tertiary text-xs select-text",
                                                span { "{format_rows(file.row_count)} rows" }
                                                span { "•" }
                                                span { "{format_compression(file.compression)}" }
                                                span { "•" }
                                                span { "{format_bytes_short(file.size_bytes)}" }
                                            }
                                        }
                                        button {
                                            class: "text-tertiary hover:text-primary p-1 cursor-pointer select-text",
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

                            div { class: "divider-soft" }
                            div { class: "flex items-center justify-between text-xs",
                                span { class: "text-tertiary select-text", "Total rows" }
                                span { class: "text-primary font-medium select-text",
                                    "{format_rows(current_state.total_rows())}"
                                }
                            }
                        }
                    } else {
                        div { class: "text-tertiary text-xs select-text",
                            "No files yet. Upload a Parquet file to begin."
                        }
                    }
                }

                div { class: "panel-soft p-4 space-y-4",
                    div { class: "flex items-center justify-between",
                        div { class: "space-y-0.5",
                            h2 { class: "text-primary text-sm font-semibold select-text",
                                "Writer settings"
                            }
                            p { class: "text-tertiary text-xs select-text",
                                "Defaults: LZ4 Raw compression, 256k row groups, parquet page sizes."
                            }
                        }
                        button {
                            class: "btn-soft text-xs select-text",
                            onclick: reset_settings,
                            "Reset"
                        }
                    }

                    div { class: "space-y-3",
                        div { class: "grid gap-3 sm:grid-cols-2",
                            div { class: "space-y-1",
                                label { class: "text-xs text-tertiary select-text", "Compression" }
                                select {
                                    class: "select select-bordered select-sm w-full select-text",
                                    value: "{current_settings.compression.value()}",
                                    onchange: update_compression,
                                    for option in CompressionChoice::all() {
                                        option { value: "{option.value()}", "{option.label()}" }
                                    }
                                }
                            }

                            div { class: "space-y-1",
                                label { class: "text-xs text-tertiary select-text",
                                    "Row group size (rows)"
                                }
                                input {
                                    class: "input input-bordered input-sm w-full select-text",
                                    r#type: "number",
                                    min: "1",
                                    value: "{current_settings.row_group_size}",
                                    oninput: update_row_group_size,
                                }
                                p { class: "text-[11px] text-tertiary select-text",
                                    "{format_rows(current_settings.row_group_size)} rows per group"
                                }
                            }

                            div { class: "space-y-1",
                                label { class: "text-xs text-tertiary select-text",
                                    "Data page size (bytes)"
                                }
                                input {
                                    class: "input input-bordered input-sm w-full select-text",
                                    r#type: "number",
                                    min: "1",
                                    value: "{current_settings.data_page_size}",
                                    oninput: update_page_size,
                                }
                                p { class: "text-[11px] text-tertiary select-text",
                                    "{format_bytes_short(current_settings.data_page_size as u64)} per page"
                                }
                            }

                            div { class: "space-y-1",
                                label { class: "text-xs text-tertiary select-text",
                                    "Dictionary page size (bytes)"
                                }
                                input {
                                    class: "input input-bordered input-sm w-full select-text",
                                    r#type: "number",
                                    min: "1",
                                    value: "{current_settings.dictionary_page_size}",
                                    oninput: update_dictionary_page_size,
                                }
                                p { class: "text-[11px] text-tertiary select-text",
                                    "{format_bytes_short(current_settings.dictionary_page_size as u64)} per dictionary page"
                                }
                            }
                        }

                        div { class: "flex items-start justify-between gap-3",
                            div { class: "space-y-0.5",
                                label { class: "text-xs text-tertiary select-text", "Page index" }
                                p { class: "text-[11px] text-tertiary select-text",
                                    "Include column + offset indexes for page-level filtering."
                                }
                            }
                            input {
                                r#type: "checkbox",
                                class: "toggle toggle-sm",
                                checked: current_settings.page_index_enabled,
                                onchange: toggle_page_index,
                            }
                        }

                        div { class: "flex items-start justify-between gap-3",
                            div { class: "space-y-0.5",
                                label { class: "text-xs text-tertiary select-text", "Bloom filter" }
                                p { class: "text-[11px] text-tertiary select-text",
                                    "Off by default. Enables bloom filters for all columns."
                                }
                            }
                            input {
                                r#type: "checkbox",
                                class: "toggle toggle-sm",
                                checked: current_settings.bloom_filter_enabled,
                                onchange: toggle_bloom_filter,
                            }
                        }

                        div { class: "divider-soft" }

                        div { class: "space-y-2",
                            div { class: "flex items-start justify-between gap-3",
                                div { class: "space-y-0.5",
                                    label { class: "text-xs text-tertiary select-text",
                                        "Per-column compression"
                                    }
                                    p { class: "text-[11px] text-tertiary select-text",
                                        "Off by default. When on, overrides can be set per column."
                                    }
                                }
                                input {
                                    r#type: "checkbox",
                                    class: "toggle toggle-sm",
                                    checked: current_settings.per_column_compression,
                                    onchange: toggle_per_column_compression,
                                }
                            }

                            if current_settings.per_column_compression {
                                if column_names.is_empty() {
                                    div { class: "text-[11px] text-tertiary select-text",
                                        "Add at least one file to configure per-column compression."
                                    }
                                } else {
                                    div { class: "space-y-2 max-h-56 overflow-auto pr-1",
                                        for (column_name , override_value) in column_rows {
                                            div {
                                                key: "{column_name}",
                                                class: "flex items-center justify-between gap-3",
                                                span { class: "text-xs text-primary truncate select-text",
                                                    "{column_name}"
                                                }
                                                select {
                                                    class: "select select-bordered select-xs w-40 select-text",
                                                    value: "{override_value}",
                                                    onchange: {
                                                        let column_for_update = column_name.clone();
                                                        move |ev| {
                                                            let value = ev.value();
                                                            settings
                                                                .with_mut(|current| {
                                                                    if value == "default" {
                                                                        current.column_compressions.remove(&column_for_update);
                                                                    } else if let Some(choice) = CompressionChoice::from_value(&value) {
                                                                        current
                                                                            .column_compressions
                                                                            .insert(column_for_update.clone(), choice);
                                                                    }
                                                                });
                                                        }
                                                    },
                                                    option { value: "default", "Use default" }
                                                    for option in CompressionChoice::all() {
                                                        option { value: "{option.value()}",
                                                            "{option.label()}"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    div { class: "divider-soft" }

                    div { class: "space-y-2",
                        div { class: "flex items-center justify-between text-xs",
                            span { class: "text-tertiary select-text", "Output" }
                            span { class: "text-primary select-text", "rewritten.parquet" }
                        }
                        if has_files {
                            div { class: "flex items-center justify-between text-xs",
                                span { class: "text-tertiary select-text", "Files" }
                                span { class: "text-primary select-text", "{current_state.files.len()}" }
                            }
                        }
                        if has_files {
                            div { class: "flex items-center justify-between text-xs",
                                span { class: "text-tertiary select-text", "Total rows" }
                                span { class: "text-primary select-text",
                                    "{format_rows(current_state.total_rows())}"
                                }
                            }
                        }
                    }

                    button {
                        class: if can_rewrite && !current_state.is_rewriting { "btn-primary-soft w-full py-2 text-sm font-medium cursor-pointer select-text" } else { "btn-soft w-full py-2 text-sm font-medium opacity-50 cursor-not-allowed select-text" },
                        disabled: !can_rewrite || current_state.is_rewriting,
                        onclick: do_rewrite,
                        if current_state.is_rewriting {
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
                                "Rewriting..."
                            }
                        } else {
                            "Rewrite & Download"
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

fn format_bytes_short(bytes: u64) -> String {
    let value = bytes as f64;
    let kb = 1024.0;
    let mb = kb * 1024.0;
    let gb = mb * 1024.0;
    if value >= gb {
        format!("{:.1} GB", value / gb)
    } else if value >= mb {
        format!("{:.1} MB", value / mb)
    } else if value >= kb {
        format!("{:.1} KB", value / kb)
    } else {
        format!("{} B", bytes)
    }
}

fn format_compression(compression: Compression) -> &'static str {
    match compression {
        Compression::UNCOMPRESSED => "Uncompressed",
        Compression::SNAPPY => "Snappy",
        Compression::GZIP(_) => "Gzip",
        Compression::LZO => "LZO",
        Compression::BROTLI(_) => "Brotli",
        Compression::LZ4 => "LZ4",
        Compression::ZSTD(_) => "ZSTD",
        Compression::LZ4_RAW => "LZ4 Raw",
    }
}

async fn read_parquet_file_info(file: web_sys::File) -> anyhow::Result<ParquetFileInfo> {
    let name = file.name();
    let size_bytes = file.size() as u64;

    let array_buffer = JsFuture::from(file.array_buffer())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read file: {:?}", e))?;

    let uint8_array = js_sys::Uint8Array::new(&array_buffer);
    let data = Bytes::from(uint8_array.to_vec());

    let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())?;
    let metadata = builder.metadata();

    let schema = builder.schema().clone();
    let row_count: usize = metadata
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .sum();

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
        size_bytes,
    })
}

async fn rewrite_parquet_files(
    files: &[ParquetFileInfo],
    settings: &RewriteSettings,
) -> anyhow::Result<Vec<u8>> {
    if files.is_empty() {
        return Err(anyhow::anyhow!("No files to rewrite"));
    }

    let schema = files[0].schema.clone();

    let mut buf = Vec::new();
    let mut builder = WriterProperties::builder()
        .set_compression(settings.compression.to_parquet())
        .set_data_page_size_limit(settings.data_page_size)
        .set_dictionary_page_size_limit(settings.dictionary_page_size)
        .set_max_row_group_size(settings.row_group_size);

    builder = builder.set_bloom_filter_enabled(settings.bloom_filter_enabled);

    if settings.page_index_enabled {
        builder = builder
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_offset_index_disabled(false);
    } else {
        builder = builder
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_offset_index_disabled(true);
    }

    if settings.per_column_compression {
        for (column, compression) in settings.column_compressions.iter() {
            builder = builder.set_column_compression(
                ColumnPath::from(column.as_str()),
                compression.to_parquet(),
            );
        }
    }

    let props = builder.build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;

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
