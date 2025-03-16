use anyhow::Result;
use arrow::datatypes::SchemaRef;
use datafusion::{
    execution::object_store::ObjectStoreUrl,
    prelude::{SessionConfig, SessionContext},
};
use leptos::prelude::*;
use leptos_router::components::Router;
use object_store::path::Path;
use parquet::{
    arrow::{async_reader::ParquetObjectReader, parquet_to_arrow_schema},
    file::metadata::ParquetMetaData,
};
use std::{sync::Arc, sync::LazyLock};

mod object_store_cache;
#[cfg(test)]
mod tests;
mod utils;
mod views;

use views::metadata::MetadataSection;
use views::parquet_reader::{ParquetReader, ParquetUnresolved};
use views::query_input::{QueryInput, execute_query_inner};
use views::query_results::{QueryResult, QueryResultView};
use views::schema::SchemaSection;
use views::settings::Settings;

pub(crate) static SESSION_CTX: LazyLock<Arc<SessionContext>> = LazyLock::new(|| {
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();
    config.options_mut().execution.parquet.pushdown_filters = true;
    Arc::new(SessionContext::new_with_config(config))
});

const DEFAULT_URL: &str = "https://parquet-viewer.xiangpeng.systems/?url=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Fopen-r1%2FOpenR1-Math-220k%2Fresolve%2Fmain%2Fdata%2Ftrain-00003-of-00010.parquet";
const DEFAULT_QUERY: &str = "show first 10 rows";

#[derive(Debug, Clone, PartialEq)]
struct MetadataDisplay {
    file_size: u64,
    uncompressed_size: u64,
    compression_ratio: f64,
    row_group_count: u64,
    row_count: u64,
    columns: u64,
    has_row_group_stats: bool,
    has_column_index: bool,
    has_page_index: bool,
    has_bloom_filter: bool,
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
    metadata_len: u64,
}

impl MetadataDisplay {
    fn from_metadata(metadata: Arc<ParquetMetaData>, metadata_len: u64) -> Result<Self> {
        let compressed_size = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.compressed_size())
            .sum::<i64>() as u64;
        let uncompressed_size = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.total_byte_size())
            .sum::<i64>() as u64;

        let schema = parquet_to_arrow_schema(
            metadata.file_metadata().schema_descr(),
            metadata.file_metadata().key_value_metadata(),
        )?;
        let first_row_group = metadata.row_groups().first();
        let first_column = first_row_group.and_then(|rg| rg.columns().first());

        let has_column_index = metadata
            .column_index()
            .and_then(|ci| ci.first().map(|c| !c.is_empty()))
            .unwrap_or(false);
        let has_page_index = metadata
            .offset_index()
            .and_then(|ci| ci.first().map(|c| !c.is_empty()))
            .unwrap_or(false);

        Ok(Self {
            file_size: compressed_size,
            uncompressed_size,
            compression_ratio: compressed_size as f64 / uncompressed_size as f64,
            row_group_count: metadata.num_row_groups() as u64,
            row_count: metadata.file_metadata().num_rows() as u64,
            columns: schema.fields.len() as u64,
            has_row_group_stats: first_column
                .map(|c| c.statistics().is_some())
                .unwrap_or(false),
            has_column_index,
            has_page_index,
            has_bloom_filter: first_column
                .map(|c| c.bloom_filter_offset().is_some())
                .unwrap_or(false),
            schema: Arc::new(schema),
            metadata,
            metadata_len,
        })
    }
}

impl std::fmt::Display for MetadataDisplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "File Size: {} MB\nRow Groups: {}\nTotal Rows: {}\nColumns: {}\nFeatures: {}{}{}{}",
            self.file_size as f64 / 1_048_576.0, // Convert bytes to MB
            self.row_group_count,
            self.row_count,
            self.columns,
            if self.has_row_group_stats {
                "✓ Statistics "
            } else {
                "✗ Statistics "
            },
            if self.has_column_index {
                "✓ Column Index "
            } else {
                "✗ Column Index "
            },
            if self.has_page_index {
                "✓ Page Index "
            } else {
                "✗ Page Index "
            },
            if self.has_bloom_filter {
                "✓ Bloom Filter"
            } else {
                "✗ Bloom Filter"
            },
        )
    }
}

#[derive(Debug, Clone)]
struct ParquetResolved {
    reader: ParquetObjectReader,
    table_name: String,
    path: Path,
    object_store_url: ObjectStoreUrl,
    display_info: MetadataDisplay,
}

impl PartialEq for ParquetResolved {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.path == other.path
            && self.object_store_url == other.object_store_url
    }
}

#[component]
fn App() -> impl IntoView {
    let (error_message, set_error_message) = signal(Option::<String>::None);
    let (parquet_table, set_parquet_table) = signal(None::<Arc<ParquetResolved>>);
    let (user_input, set_user_input) = signal(Option::<String>::None);

    let (query_results, set_query_results) = signal(Vec::<QueryResult>::new());

    let (show_settings, set_show_settings) = signal(false);

    let toggle_display = move |id: usize| {
        set_query_results.update(|r| {
            r.iter_mut()
                .find(|r| r.id() == id)
                .unwrap()
                .toggle_display();
        });
    };

    let on_user_submit_query_call_back = move |query: String| {
        set_user_input.set(Some(query.clone()));
        let Some(table) = parquet_table.get() else {
            return;
        };

        set_query_results.update(|v| {
            let id = v.len();
            v.push(QueryResult::new(id, query, table));
        });
    };

    let on_parquet_read_call_back =
        move |parquet_info: Result<ParquetUnresolved>| match parquet_info {
            Ok(parquet_info) => {
                leptos::task::spawn_local(async move {
                    match parquet_info.try_into_resolved(SESSION_CTX.as_ref()).await {
                        Ok(table) => {
                            set_parquet_table.set(Some(Arc::new(table)));
                            on_user_submit_query_call_back(DEFAULT_QUERY.to_string());
                        }
                        Err(e) => {
                            set_error_message.set(Some(format!("{:#?}", e)));
                        }
                    }
                });
            }
            Err(e) => set_error_message.set(Some(format!("{:#?}", e))),
        };

    view! {
        <div class="container mx-auto px-4 py-8 max-w-6xl">
            <h1 class="text-3xl font-bold mb-8 flex items-center justify-between">
                <span>"Parquet Viewer"</span>
                <div class="flex items-center gap-4">
                    <button
                        on:click=move |_| set_show_settings.set(true)
                        class="text-gray-600 hover:text-gray-800"
                        title="Settings"
                    >
                        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"
                            ></path>
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"
                            ></path>
                        </svg>
                    </button>
                    <a
                        href="https://github.com/XiangpengHao/parquet-viewer"
                        target="_blank"
                        class="text-gray-600 hover:text-gray-800"
                    >
                        <svg height="24" width="24" viewBox="0 0 16 16">
                            <path
                                fill="currentColor"
                                d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"
                            ></path>
                        </svg>
                    </a>
                </div>
            </h1>
            <div class="space-y-6">
                <ParquetReader read_call_back=on_parquet_read_call_back />

                {move || {
                    error_message
                        .get()
                        .map(|msg| {
                            view! {
                                <div class="bg-red-50 border-l-4 border-red-500 p-4 my-4">
                                    <pre class="text-red-700 whitespace-pre-wrap break-words">
                                        {msg}
                                    </pre>
                                </div>
                            }
                        })
                }}

                <div class="border-t border-gray-300 my-4"></div>

                <div class="mt-4">
                    {move || {
                        parquet_table
                            .get()
                            .map(|table| {
                                if table.display_info.row_group_count > 0 {
                                    view! {
                                        <QueryInput
                                            user_input=user_input
                                            on_user_submit_query=on_user_submit_query_call_back
                                        />
                                    }
                                        .into_any()
                                } else {
                                    ().into_any()
                                }
                            })
                    }}
                </div>

                <div class="space-y-4">
                    <For
                        each=move || query_results.get().into_iter().filter(|r| r.display()).rev()
                        key=|result| result.id()
                        children=move |result| {
                            view! {
                                <div class="transform transition-all duration-300 ease-out animate-slide-in">
                                    <QueryResultView result=result toggle_display=toggle_display />
                                </div>
                            }
                        }
                    />
                </div>

                <div class="border-t border-gray-300 my-4"></div>

                <div class="mt-8">
                    {move || {
                        let table = parquet_table.get();
                        match table {
                            Some(table) => {
                                view! {
                                    <div class="space-y-6">
                                        <div class="w-full">
                                            <MetadataSection parquet_reader=table.clone() />
                                        </div>
                                        <div class="w-full">
                                            <SchemaSection parquet_reader=table.clone() />
                                        </div>
                                    </div>
                                }
                                    .into_any()
                            }
                            None => {
                                view! {
                                    <div class="text-center text-gray-500 py-8">
                                        "No file selected, try "
                                        <a class="text-blue-500" href=DEFAULT_URL target="_blank">
                                            an example?
                                        </a>
                                    </div>
                                }
                                    .into_any()
                            }
                        }
                    }}
                </div>

            </div>
            <Settings show=show_settings set_show=set_show_settings />
        </div>
    }
}

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(|| {
        view! {
            <Router>
                <App />
            </Router>
        }
    })
}
