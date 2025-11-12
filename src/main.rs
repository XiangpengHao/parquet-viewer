use anyhow::Result;
use datafusion::prelude::{SessionConfig, SessionContext};
use leptos::{logging, prelude::*};
use leptos_router::components::Router;
use parquet_ctx::ParquetResolved;
use std::{sync::Arc, sync::LazyLock};
use utils::{send_message_to_vscode, vscode_env};
use web_sys::js_sys;

mod components;
mod nl_to_sql;
mod object_store_cache;
mod parquet_ctx;
#[cfg(test)]
mod tests;
mod utils;
mod views;
use views::metadata::MetadataView;
use views::parquet_reader::{ParquetReader, ParquetUnresolved, read_from_vscode};
use views::query_input::QueryInput;
use views::query_results::{QueryResult, QueryResultView};
use views::schema::SchemaSection;
use views::settings::Settings;

pub(crate) static SESSION_CTX: LazyLock<Arc<SessionContext>> = LazyLock::new(|| {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();
    config.options_mut().execution.parquet.pushdown_filters = true;
    Arc::new(SessionContext::new_with_config(config))
});

const DEFAULT_URL: &str = "https://parquet-viewer.xiangpeng.systems/?url=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Fopen-r1%2FOpenR1-Math-220k%2Fresolve%2Fmain%2Fdata%2Ftrain-00003-of-00010.parquet";
const DEFAULT_QUERY: &str = "show first 10 rows";

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
                            set_error_message.set(Some(format!("{e:#?}")));
                        }
                    }
                });
            }
            Err(e) => set_error_message.set(Some(format!("{e:#?}"))),
        };

    let vscode_env = vscode_env();
    let is_in_vscode = vscode_env.is_some();
    if let Some(vscode) = vscode_env {
        send_message_to_vscode("ready", &vscode);

        window_event_listener(leptos::ev::message, move |event: web_sys::MessageEvent| {
            let data = event.data();
            if !data.is_object() {
                return;
            }
            let obj = js_sys::Object::from(data);
            if let Ok(type_val) = js_sys::Reflect::get(&obj, &"type".into())
                && let Some(type_str) = type_val.as_string()
            {
                match type_str.as_str() {
                    "parquetServerReady" => {
                        read_from_vscode(obj, on_parquet_read_call_back);
                    }
                    _ => logging::log!("Unknown message type: {}", type_str),
                }
            }
        });
    }

    view! {
        <div class="container mx-auto px-4 py-4 text-xs">
            <h1 class="text-2xl font-bold mb-2 flex items-center justify-between">
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
            <div class="space-y-3">
                {move || {
                    if is_in_vscode {
                        ().into_any()
                    } else {
                        view! { <ParquetReader read_call_back=on_parquet_read_call_back /> }
                            .into_any()
                    }
                }}
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
                <div class="mt-2">
                    {move || {
                        parquet_table
                            .get()
                            .map(|table| {
                                if table.metadata().row_group_count > 0 {
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
                <div class="space-y-2">
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
                <div class="mt-4">
                    {move || {
                        let table = parquet_table.get();
                        match table {
                            Some(table) => {
                                view! {
                                    <div class="space-y-6">
                                        <div class="w-full">
                                            <MetadataView parquet_reader=table.clone() />
                                        </div>
                                        <div class="w-full">
                                            <SchemaSection parquet_reader=table.clone() />
                                        </div>
                                    </div>
                                }
                                    .into_any()
                            }
                            None => {
                                if is_in_vscode {
                                    ().into_any()
                                } else {
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
