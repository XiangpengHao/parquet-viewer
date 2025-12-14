use std::sync::{Arc, LazyLock};

use anyhow::Result;
use datafusion::prelude::{SessionConfig, SessionContext};
use dioxus::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen_futures::spawn_local;
use web_sys::js_sys;

use crate::utils::{send_message_to_vscode, vscode_env};
use components::QueryInput;
use parquet_ctx::ParquetResolved;
use storage::readers;
use views::metadata::MetadataView;
use views::parquet_reader::{ParquetReader, ParquetUnresolved};
use views::query_results::QueryResultView;
use views::schema::SchemaSection;
use views::settings::Settings;

mod components;
mod nl_to_sql;
mod parquet_ctx;
mod storage;
#[cfg(test)]
mod tests;
mod utils;
mod views;

pub(crate) static SESSION_CTX: LazyLock<Arc<SessionContext>> = LazyLock::new(|| {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();
    config.options_mut().execution.parquet.pushdown_filters = true;
    Arc::new(SessionContext::new_with_config(config))
});

const DEFAULT_URL: &str = "https://parquet-viewer.xiangpeng.systems/?url=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Fopen-r1%2FOpenR1-Math-220k%2Fresolve%2Fmain%2Fdata%2Ftrain-00003-of-00010.parquet";
pub(crate) const DEFAULT_QUERY: &str = "show first 10 rows";

#[derive(Clone)]
struct QueryEntry {
    id: usize,
    query: String,
    display: bool,
    table: Arc<ParquetResolved>,
}

#[component]
fn MainLayout() -> Element {
    let error_message = use_signal(|| None::<String>);
    let parquet_table = use_signal(|| None::<Arc<ParquetResolved>>);
    let query_input = use_signal(|| DEFAULT_QUERY.to_string());
    let query_results = use_signal(|| Vec::<QueryEntry>::new());

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
            let parquet_table = parquet_table;

            query_input.set(query.clone());
            let Some(table) = parquet_table().as_ref().cloned() else {
                return;
            };
            let mut next = query_results();
            let id = next.len();
            next.push(QueryEntry {
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
                let mut parquet_table = parquet_table;
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

                                let mut next = Vec::new();
                                next.push(QueryEntry {
                                    id: 0,
                                    query: DEFAULT_QUERY.to_string(),
                                    display: true,
                                    table,
                                });
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

    let vscode = vscode_env();
    let is_in_vscode = vscode.is_some();
    let mut vscode_initialized = use_signal(|| false);
    if let Some(vscode) = vscode {
        if !vscode_initialized() {
            vscode_initialized.set(true);
            send_message_to_vscode("ready", &vscode);

            let on_parquet_read = on_parquet_read;
            let handler: Closure<dyn FnMut(web_sys::MessageEvent)> =
                Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
                    let data = event.data();
                    if !data.is_object() {
                        return;
                    }
                    let obj = js_sys::Object::from(data);
                    if let Ok(type_val) = js_sys::Reflect::get(&obj, &"type".into())
                        && let Some(type_str) = type_val.as_string()
                    {
                        if type_str.as_str() == "parquetServerReady" {
                            readers::read_from_vscode(obj, move |res| on_parquet_read(res));
                        }
                    }
                }));

            if let Some(window) = web_sys::window() {
                let _ = window
                    .add_event_listener_with_callback("message", handler.as_ref().unchecked_ref());
            }
            handler.forget();
        }
    }

    rsx! {
        div { class: "container mx-auto px-4 py-4 text-xs",
            h1 { class: "text-2xl font-bold mb-2 flex items-center justify-between",
                span { "Parquet Viewer" }
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

            div { class: "space-y-3",
                if !is_in_vscode {
                    ParquetReader { read_call_back: on_parquet_read }
                }

                if let Some(msg) = error_message() {
                    div { class: "alert alert-error my-4",
                        pre { class: "whitespace-pre-wrap break-words", "{msg}" }
                    }
                }

                if let Some(table) = parquet_table() {
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

                if let Some(table) = parquet_table() {
                    div { class: "space-y-6 mt-4",
                        MetadataView { parquet_reader: table.clone() }
                        SchemaSection { parquet_reader: table.clone() }
                    }
                } else if !is_in_vscode {
                    div { class: "text-center text-gray-500 py-8",
                        "No file selected, try "
                        a {
                            class: "text-blue-500",
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

// We can import assets in dioxus with the `asset!` macro. This macro takes a path to an asset relative to the crate root.
// The macro returns an `Asset` type that will display as the path to the asset in the browser or a local path in desktop bundles.
const FAVICON: Asset = asset!("/assets/icon-192x192.png");
// The asset macro also minifies some assets like CSS and JS to make bundled smaller
const MAIN_CSS: Asset = asset!("/assets/styling/main.css");
const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

/// The Route enum is used to define the structure of internal routes in our app. All route enums need to derive
/// the [`Routable`] trait, which provides the necessary methods for the router to work.
/// 
/// Each variant represents a different URL pattern that can be matched by the router. If that pattern is matched,
/// the components for that route will be rendered.
#[derive(Debug, Clone, Routable, PartialEq)]
#[rustfmt::skip]
enum Route {
    #[layout(MainLayout)]
    #[route("/")]
    Index {},
    #[route("/settings")]
    SettingsRoute {},
}

#[component]
fn Index() -> Element {
    rsx! {}
}

#[component]
fn SettingsRoute() -> Element {
    let nav = use_navigator();
    rsx! {
        Settings {
            show: true,
            on_close: move || {
                nav.push(Route::Index {});
            },
        }
    }
}

#[component]
fn App() -> Element {
    rsx! {
        // In addition to element and text (which we will see later), rsx can contain other components. In this case,
        // we are using the `document::Link` component to add a link to our favicon and main CSS file into the head of our app.
        document::Link { rel: "icon", href: FAVICON }
        document::Link { rel: "stylesheet", href: MAIN_CSS }
        document::Link { rel: "stylesheet", href: TAILWIND_CSS }
        // Cloudflare Web Analytics
        document::Script {
            src: "https://static.cloudflareinsights.com/beacon.min.js",
            defer: true,
            "data-cf-beacon": r#"{{"token": "cdf9b270eac24614a52f26d4b465b8ae"}}"#,
        }

        // The router component renders the route enum we defined above. It will handle synchronization of the URL and render
        // the layouts and components for the active route.
        Router::<Route> {}
    }
}

fn main() {
    dioxus::launch(App);
}
