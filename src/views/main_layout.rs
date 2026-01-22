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
use super::settings::Settings;

const DEFAULT_URL: &str = "https://parquet-viewer.xiangpeng.systems/?url=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Fopen-r1%2FOpenR1-Math-220k%2Fresolve%2Fmain%2Fdata%2Ftrain-00003-of-00010.parquet";
pub(crate) const DEFAULT_QUERY: &str = "show first 10 rows";

fn format_rows(count: u64) -> String {
    let mut result = count.to_string();
    let mut i = result.len();
    while i > 3 {
        i -= 3;
        result.insert(i, ',');
    }
    result
}

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
    let loaded_files = use_signal(Vec::<Arc<ParquetResolved>>::new);
    let query_input = use_signal(|| DEFAULT_QUERY.to_string());
    let query_results = use_signal(Vec::<QueryResultEntry>::new);

    // Theme management
    let (theme, toggle_theme) = use_theme();

    // Settings modal state
    let mut show_settings = use_signal(|| false);

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
            let files = loaded_files();

            query_input.set(query.clone());
            // Use the most recently loaded file for queries
            let Some(table) = files.last().cloned() else {
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
                let mut loaded_files = loaded_files;
                let mut query_results = query_results;
                let mut query_input = query_input;
                spawn_local({
                    async move {
                        match parquet_info.try_into_resolved(SESSION_CTX.as_ref()).await {
                            Ok(table) => {
                                let table = Arc::new(table);
                                // Add to list of loaded files
                                let mut files = loaded_files();
                                files.push(table.clone());
                                loaded_files.set(files);

                                query_input.set(DEFAULT_QUERY.to_string());

                                // Add default query for the new file
                                let mut results = query_results();
                                let id = results.len();
                                results.push(QueryResultEntry {
                                    id,
                                    query: DEFAULT_QUERY.to_string(),
                                    display: true,
                                    table,
                                });
                                query_results.set(results);
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
    let url_param = match &route {
        Route::Index { url } => url.clone(),
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

    // Determine which view is active based on route
    let is_viewer = matches!(route, Route::Index { .. });
    let is_utils = matches!(route, Route::UtilsRoute {});

    rsx! {
        div { class: "flex h-screen overflow-hidden",
            // Slim sidebar - fixed position
            if !is_in_vscode {
                aside { class: "sidebar flex flex-col items-center py-3 gap-1 shrink-0 h-screen",
                    // Viewer icon
                    Link {
                        to: Route::Index { url: None },
                        class: if is_viewer { "sidebar-icon active" } else { "sidebar-icon" },
                        title: "Viewer",
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            class: "w-[18px] h-[18px]",
                            fill: "none",
                            view_box: "0 0 24 24",
                            stroke: "currentColor",
                            stroke_width: "1.5",
                            path {
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                d: "M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0112 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125M3.375 8.25c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m17.25-3.75h-7.5c-.621 0-1.125.504-1.125 1.125m8.625-1.125c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125M12 10.875v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125M13.125 12h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125M20.625 12c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5M12 14.625v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 14.625c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125m0 0v1.5c0 .621-.504 1.125-1.125 1.125",
                            }
                        }
                    }

                    // Utils icon
                    Link {
                        to: Route::UtilsRoute {},
                        class: if is_utils { "sidebar-icon active" } else { "sidebar-icon" },
                        title: "Utils",
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            class: "w-[18px] h-[18px]",
                            fill: "none",
                            view_box: "0 0 24 24",
                            stroke: "currentColor",
                            stroke_width: "1.5",
                            path {
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                d: "M11.42 15.17L17.25 21A2.652 2.652 0 0021 17.25l-5.877-5.877M11.42 15.17l2.496-3.03c.317-.384.74-.626 1.208-.766M11.42 15.17l-4.655 5.653a2.548 2.548 0 11-3.586-3.586l6.837-5.63m5.108-.233c.55-.164 1.163-.188 1.743-.14a4.5 4.5 0 004.486-6.336l-3.276 3.277a3.004 3.004 0 01-2.25-2.25l3.276-3.276a4.5 4.5 0 00-6.336 4.486c.091 1.076-.071 2.264-.904 2.95l-.102.085m-1.745 1.437L5.909 7.5H4.5L2.25 3.75l1.5-1.5L7.5 4.5v1.409l4.26 4.26m-1.745 1.437l1.745-1.437m6.615 8.206L15.75 15.75M4.867 19.125h.008v.008h-.008v-.008z",
                            }
                        }
                    }

                    // Spacer
                    div { class: "flex-1" }

                    // Theme toggle
                    button {
                        class: "sidebar-icon",
                        onclick: move |_| toggle_theme.call(()),
                        title: if theme() == Theme::Light { "Dark mode" } else { "Light mode" },
                        if theme() == Theme::Dark {
                            svg {
                                xmlns: "http://www.w3.org/2000/svg",
                                class: "w-[18px] h-[18px]",
                                fill: "none",
                                view_box: "0 0 24 24",
                                stroke: "currentColor",
                                stroke_width: "1.5",
                                path {
                                    stroke_linecap: "round",
                                    stroke_linejoin: "round",
                                    d: "M12 3v2.25m6.364.386l-1.591 1.591M21 12h-2.25m-.386 6.364l-1.591-1.591M12 18.75V21m-4.773-4.227l-1.591 1.591M5.25 12H3m4.227-4.773L5.636 5.636M15.75 12a3.75 3.75 0 11-7.5 0 3.75 3.75 0 017.5 0z",
                                }
                            }
                        } else {
                            svg {
                                xmlns: "http://www.w3.org/2000/svg",
                                class: "w-[18px] h-[18px]",
                                fill: "none",
                                view_box: "0 0 24 24",
                                stroke: "currentColor",
                                stroke_width: "1.5",
                                path {
                                    stroke_linecap: "round",
                                    stroke_linejoin: "round",
                                    d: "M21.752 15.002A9.718 9.718 0 0118 15.75c-5.385 0-9.75-4.365-9.75-9.75 0-1.33.266-2.597.748-3.752A9.753 9.753 0 003 11.25C3 16.635 7.365 21 12.75 21a9.753 9.753 0 009.002-5.998z",
                                }
                            }
                        }
                    }

                    // Settings
                    button {
                        class: "sidebar-icon",
                        title: "Settings",
                        onclick: move |_| show_settings.set(true),
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            class: "w-[18px] h-[18px]",
                            fill: "none",
                            view_box: "0 0 24 24",
                            stroke: "currentColor",
                            stroke_width: "1.5",
                            path {
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                d: "M9.594 3.94c.09-.542.56-.94 1.11-.94h2.593c.55 0 1.02.398 1.11.94l.213 1.281c.063.374.313.686.645.87.074.04.147.083.22.127.324.196.72.257 1.075.124l1.217-.456a1.125 1.125 0 011.37.49l1.296 2.247a1.125 1.125 0 01-.26 1.431l-1.003.827c-.293.24-.438.613-.431.992a6.759 6.759 0 010 .255c-.007.378.138.75.43.99l1.005.828c.424.35.534.954.26 1.43l-1.298 2.247a1.125 1.125 0 01-1.369.491l-1.217-.456c-.355-.133-.75-.072-1.076.124a6.57 6.57 0 01-.22.128c-.331.183-.581.495-.644.869l-.213 1.28c-.09.543-.56.941-1.11.941h-2.594c-.55 0-1.02-.398-1.11-.94l-.213-1.281c-.062-.374-.312-.686-.644-.87a6.52 6.52 0 01-.22-.127c-.325-.196-.72-.257-1.076-.124l-1.217.456a1.125 1.125 0 01-1.369-.49l-1.297-2.247a1.125 1.125 0 01.26-1.431l1.004-.827c.292-.24.437-.613.43-.992a6.932 6.932 0 010-.255c.007-.378-.138-.75-.43-.99l-1.004-.828a1.125 1.125 0 01-.26-1.43l1.297-2.247a1.125 1.125 0 011.37-.491l1.216.456c.356.133.751.072 1.076-.124.072-.044.146-.087.22-.128.332-.183.582-.495.644-.869l.214-1.281z",
                            }
                            path {
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                d: "M15 12a3 3 0 11-6 0 3 3 0 016 0z",
                            }
                        }
                    }
                }
            }

            // Main content area - scrollable
            main { class: "main-content flex-1 overflow-y-auto h-screen",
                div { class: "max-w-7xl mx-auto px-8 py-6",
                    if is_utils {
                        // Utils view
                        Outlet::<Route> {}
                    } else {
                        // Viewer content
                        div { class: "space-y-4",
                            // Header with file indicator
                            div { class: "flex items-center justify-between",
                                h1 { class: "text-primary text-xl font-semibold tracking-tight",
                                    "Parquet Viewer"
                                }

                                // Loaded files indicator
                                if !loaded_files().is_empty() {
                                    div { class: "flex items-center gap-1.5 flex-wrap",
                                        for file in loaded_files().iter() {
                                            div {
                                                key: "{file.registered_table_name()}",
                                                class: "dropdown dropdown-end",
                                                // Clickable chip
                                                div {
                                                    tabindex: "0",
                                                    role: "button",
                                                    class: "file-indicator flex items-center gap-1.5 px-2 py-1 rounded-md text-xs cursor-pointer",
                                                    // File icon
                                                    svg {
                                                        xmlns: "http://www.w3.org/2000/svg",
                                                        class: "w-3 h-3",
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
                                                    span { class: "font-medium truncate max-w-[150px]",
                                                        "{file.table_name()}"
                                                    }
                                                }
                                                // Dropdown content
                                                div {
                                                    tabindex: "0",
                                                    class: "dropdown-content z-50 mt-1 p-3 shadow-lg bg-base-100 rounded-lg border border-base-300 min-w-[280px]",
                                                    div { class: "space-y-2 text-sm",
                                                        div {
                                                            span { class: "text-tertiary", "Table: " }
                                                            span { class: "text-primary font-mono select-all", "{file.registered_table_name()}" }
                                                        }
                                                        div {
                                                            span { class: "text-tertiary", "Rows: " }
                                                            span { class: "text-primary", "{format_rows(file.metadata().row_count)}" }
                                                        }
                                                        div {
                                                            span { class: "text-tertiary", "Columns: " }
                                                            span { class: "text-primary", "{file.metadata().columns}" }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if !is_in_vscode {
                                ParquetReader { read_call_back: on_parquet_read, initial_url: url_param }
                            }

                            if let Some(msg) = error_message() {
                                div { class: "panel-soft p-4 border-l-2 border-red-400",
                                    pre { class: "text-sm text-red-600 dark:text-red-400 whitespace-pre-wrap break-words", "{msg}" }
                                }
                            }

                            if let Some(table) = loaded_files().last() {
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

                            div { class: "space-y-3",
                                for entry in query_results().iter().rev().filter(|r| r.display) {
                                    div {
                                        key: "{entry.id}",
                                        QueryResultView {
                                            id: entry.id,
                                            query: entry.query.clone(),
                                            parquet_table: entry.table.clone(),
                                            on_hide,
                                        }
                                    }
                                }
                            }

                            if let Some(table) = loaded_files().last() {
                                div { class: "space-y-4 mt-6",
                                    MetadataView { parquet_reader: table.clone() }
                                    SchemaSection { parquet_reader: table.clone() }
                                }
                            } else if !is_in_vscode {
                                div { class: "text-center text-tertiary py-12",
                                    p { class: "mb-2", "No file loaded" }
                                    a {
                                        class: "text-sm text-blue-500 hover:text-blue-600",
                                        href: "{DEFAULT_URL}",
                                        target: "_blank",
                                        "Try an example"
                                    }
                                }
                            }
                        }

                    }
                }
            }

            // Settings modal - rendered on top of everything
            Settings {
                show: show_settings(),
                on_close: move |_| show_settings.set(false),
            }
        }
    }
}
