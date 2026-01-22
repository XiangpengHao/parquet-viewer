use std::sync::{Arc, LazyLock};

use components::toast::ToastProvider;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::config::Dialect;
use dioxus::prelude::*;
use views::main_layout::MainLayout;
use views::parquet_utils::ParquetUtils;

mod components;
mod nl_to_sql;
mod parquet_ctx;
mod storage;
#[cfg(test)]
mod tests;
mod utils;
mod views;

pub(crate) use parquet_ctx::ParquetResolved;

pub(crate) static SESSION_CTX: LazyLock<Arc<SessionContext>> = LazyLock::new(|| {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().sql_parser.dialect = Dialect::PostgreSQL;
    config.options_mut().execution.parquet.pushdown_filters = true;
    Arc::new(SessionContext::new_with_config(config))
});

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
    #[route("/?:url")]
    Index { url: Option<String> },
    #[route("/utils")]
    UtilsRoute {},
}

#[component]
fn Index(url: Option<String>) -> Element {
    // The url parameter is passed from the route, but we handle it in MainLayout
    _ = url;
    rsx! {}
}

#[component]
fn UtilsRoute() -> Element {
    rsx! {
        ParquetUtils {}
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

        ToastProvider { Router::<Route> {} }
    }
}

fn main() {
    dioxus::launch(App);
}
