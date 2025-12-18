use std::sync::Arc;

use anyhow::Result;
use dioxus::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen_futures::spawn_local;
use web_sys::js_sys;

use crate::components::{QueryInput, Theme, use_theme};
use crate::parquet_ctx::ParquetResolved;
use crate::storage::readers;
use crate::utils::{send_message_to_vscode, vscode_env};
use crate::{Route, SESSION_CTX};

use super::metadata::MetadataView;
use super::parquet_reader::{ParquetReader, ParquetUnresolved};
use super::query_results::QueryResultView;
use super::schema::SchemaSection;

const DEFAULT_URL: &str = "https://parquet-viewer.xiangpeng.systems/?url=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Fopen-r1%2FOpenR1-Math-220k%2Fresolve%2Fmain%2Fdata%2Ftrain-00003-of-00010.parquet";
pub(crate) const DEFAULT_QUERY: &str = "show first 10 rows";

#[derive(Clone)]
struct QueryResultEntry {
    id: usize,
    query: String,
    display: bool,
    table: Arc<ParquetResolved>,
}

#[component]
pub(crate) fn MainLayout() -> Element {
    let error_message = use_signal(|| None::<String>);
    let parquet_resolved = use_signal(|| None::<Arc<ParquetResolved>>);
    let query_input = use_signal(|| DEFAULT_QUERY.to_string());
    let query_results = use_signal(Vec::<QueryResultEntry>::new);

    // Theme management
    let (theme, toggle_theme) = use_theme();

    let on_hide = {
        move |id: usize| {
            let mut query_results = query_results;
            let mut next = query_results();
            if let Some(entry) = next.iter_mut().find(|e| e.id == id) {
                entry.display = false;
            }
            query_results.set(next);
        }
    };

    let on_submit_query = {
        move |query: String| {
            let mut query_input = query_input;
            let mut query_results = query_results;
            let parquet_table = parquet_resolved;

            query_input.set(query.clone());
            let Some(table) = parquet_table().as_ref().cloned() else {
                return;
            };
            let mut next = query_results();
            let id = next.len();
            next.push(QueryResultEntry {
                id,
                query,
                display: true,
                table,
            });
            query_results.set(next);
        }
    };

    let on_parquet_read = {
        move |parquet_info: Result<ParquetUnresolved>| match parquet_info {
            Ok(parquet_info) => {
                let mut error_message = error_message;
                let mut parquet_table = parquet_resolved;
                let mut query_results = query_results;
                let mut query_input = query_input;
                spawn_local({
                    async move {
                        match parquet_info.try_into_resolved(SESSION_CTX.as_ref()).await {
                            Ok(table) => {
                                let table = Arc::new(table);
                                parquet_table.set(Some(table.clone()));
                                query_results.set(vec![]);
                                query_input.set(DEFAULT_QUERY.to_string());

                                let next = vec![QueryResultEntry {
                                    id: 0,
                                    query: DEFAULT_QUERY.to_string(),
                                    display: true,
                                    table,
                                }];
                                query_results.set(next);
                            }
                            Err(e) => error_message.set(Some(format!("{e:#?}"))),
                        }
                    }
                });
            }
            Err(e) => {
                let mut error_message = error_message;
                error_message.set(Some(format!("{e:#?}")))
            }
        }
    };

    // Get the URL parameter from the route
    let route = use_route::<Route>();
    let url_param = match route {
        Route::Index { url } => url,
        _ => None,
    };

    let vscode = vscode_env();
    let is_in_vscode = vscode.is_some();
    let mut vscode_initialized = use_signal(|| false);
    if let Some(vscode) = vscode
        && !vscode_initialized()
    {
        vscode_initialized.set(true);
        send_message_to_vscode("ready", &vscode);

        let handler: Closure<dyn FnMut(web_sys::MessageEvent)> =
            Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
                let data = event.data();
                if !data.is_object() {
                    return;
                }
                let obj = js_sys::Object::from(data);
                if let Ok(type_val) = js_sys::Reflect::get(&obj, &"type".into())
                    && let Some(type_str) = type_val.as_string()
                    && type_str.as_str() == "parquetServerReady"
                {
                    readers::read_from_vscode(obj, on_parquet_read);
                }
            }));

        if let Some(window) = web_sys::window() {
            let _ = window
                .add_event_listener_with_callback("message", handler.as_ref().unchecked_ref());
        }
        handler.forget();
    }

    rsx! {
        div { class: "container mx-auto px-4 py-4 text-xs",
            h1 { class: "text-2xl font-bold mb-2 flex items-center justify-between",
                span { "Parquet Viewer" }
                div { class: "flex items-center gap-2",
                    // Theme toggle button
                    button {
                        class: "btn btn-ghost btn-sm swap swap-rotate",
                        onclick: move |_| toggle_theme.call(()),
                        title: if theme() == Theme::Light { "Switch to dark mode" } else { "Switch to light mode" },
                        aria_label: "Toggle theme",
                        // Sun icon (shown in dark mode)
                        if theme() == Theme::Dark {
                            svg {
                                xmlns: "http://www.w3.org/2000/svg",
                                class: "h-5 w-5",
                                fill: "none",
                                view_box: "0 0 24 24",
                                stroke: "currentColor",
                                path {
                                    stroke_linecap: "round",
                                    stroke_linejoin: "round",
                                    stroke_width: "2",
                                    d: "M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z",
                                }
                            }
                        } else {
                            // Moon icon (shown in light mode)
                            svg {
                                xmlns: "http://www.w3.org/2000/svg",
                                class: "h-5 w-5",
                                fill: "none",
                                view_box: "0 0 24 24",
                                stroke: "currentColor",
                                path {
                                    stroke_linecap: "round",
                                    stroke_linejoin: "round",
                                    stroke_width: "2",
                                    d: "M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z",
                                }
                            }
                        }
                    }
                    Link {
                        to: Route::SettingsRoute {},
                        class: "btn btn-ghost btn-sm",
                        title: "Settings",
                        aria_label: "Settings",
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            class: "h-5 w-5",
                            fill: "none",
                            view_box: "0 0 24 24",
                            stroke: "currentColor",
                            path {
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                stroke_width: "2",
                                d: "M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z",
                            }
                            path {
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                stroke_width: "2",
                                d: "M15 12a3 3 0 11-6 0 3 3 0 016 0z",
                            }
                        }
                    }
                }
            }

            div { class: "space-y-3",
                if !is_in_vscode {
                    ParquetReader { read_call_back: on_parquet_read, initial_url: url_param }
                }

                if let Some(msg) = error_message() {
                    div { class: "alert alert-error my-4",
                        pre { class: "whitespace-pre-wrap break-words", "{msg}" }
                    }
                }

                if let Some(table) = parquet_resolved() {
                    if table.metadata().row_group_count > 0 {
                        QueryInput {
                            value: query_input(),
                            on_value_change: move |v| {
                                let mut query_input = query_input;
                                query_input.set(v);
                            },
                            on_user_submit_query: on_submit_query,
                        }
                    }
                }

                div { class: "space-y-2",
                    for entry in query_results().iter().rev().filter(|r| r.display) {
                        div {
                            key: "{entry.id}",
                            class: "transform transition-all duration-300 ease-out animate-slide-in",
                            QueryResultView {
                                id: entry.id,
                                query: entry.query.clone(),
                                parquet_table: entry.table.clone(),
                                on_hide,
                            }
                        }
                    }
                }

                if let Some(table) = parquet_resolved() {
                    div { class: "space-y-6 mt-4",
                        MetadataView { parquet_reader: table.clone() }
                        SchemaSection { parquet_reader: table.clone() }
                    }
                } else if !is_in_vscode {
                    div { class: "text-center opacity-60 py-8",
                        "No file selected, try "
                        a {
                            class: "link link-primary",
                            href: "{DEFAULT_URL}",
                            target: "_blank",
                            "an example?"
                        }
                    }
                }
            }

            Outlet::<Route> {}
        }
    }
}
