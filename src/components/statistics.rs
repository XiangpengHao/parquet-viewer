use crate::utils::format_rows;
use dioxus::prelude::*;
use parquet::file::statistics::Statistics;

#[component]
pub fn StatisticsDisplay(statistics: Option<Statistics>) -> Element {
    match &statistics {
        Some(stats) => {
            let (min_val, max_val) = match stats {
                Statistics::Int32(s) => (
                    s.min_opt().map(|v| v.to_string()),
                    s.max_opt().map(|v| v.to_string()),
                ),
                Statistics::Int64(s) => (
                    s.min_opt().map(|v| v.to_string()),
                    s.max_opt().map(|v| v.to_string()),
                ),
                Statistics::Int96(s) => (
                    s.min_opt().map(|v| v.to_string()),
                    s.max_opt().map(|v| v.to_string()),
                ),
                Statistics::Boolean(s) => (
                    s.min_opt().map(|v| v.to_string()),
                    s.max_opt().map(|v| v.to_string()),
                ),
                Statistics::Float(s) => (
                    s.min_opt().map(|v| format!("{v:.2}")),
                    s.max_opt().map(|v| format!("{v:.2}")),
                ),
                Statistics::Double(s) => (
                    s.min_opt().map(|v| format!("{v:.2}")),
                    s.max_opt().map(|v| format!("{v:.2}")),
                ),
                Statistics::ByteArray(s) => (
                    s.min_opt()
                        .and_then(|v| v.as_utf8().ok().map(|s| s.to_string())),
                    s.max_opt()
                        .and_then(|v| v.as_utf8().ok().map(|s| s.to_string())),
                ),
                Statistics::FixedLenByteArray(s) => (
                    s.min_opt()
                        .and_then(|v| v.as_utf8().ok().map(|s| s.to_string())),
                    s.max_opt()
                        .and_then(|v| v.as_utf8().ok().map(|s| s.to_string())),
                ),
            };

            let null_count = stats.null_count_opt();
            let distinct_count = stats.distinct_count_opt();

            rsx! {
                div { class: "flex flex-wrap gap-2 text-xs",
                    if let Some(val) = min_val {
                        div { class: "flex-1 min-w-[200px] max-h-20 px-2 py-1 rounded border border-gray-200 overflow-y-auto",
                            span { class: "opacity-75 font-medium", "Min: " }
                            span { class: "text-base-content break-words", "{val}" }
                        }
                    }
                    if let Some(val) = max_val {
                        div { class: "flex-1 min-w-[200px] max-h-20 px-2 py-1 rounded border border-gray-200 overflow-y-auto",
                            span { class: "opacity-75 font-medium", "Max: " }
                            span { class: "text-base-content break-words", "{val}" }
                        }
                    }
                    if let Some(count) = null_count {
                        div { class: "flex-1 max-w-[50px] px-2 py-1 rounded border border-gray-200",
                            span { class: "opacity-75 font-medium", "Nulls: " }
                            span { class: "text-base-content", "{format_rows(count)}" }
                        }
                    }
                    if let Some(count) = distinct_count {
                        div { class: "flex-1 max-w-[50px] px-2 py-1 rounded border border-gray-200",
                            span { class: "opacity-75 font-medium", "Distinct: " }
                            span { class: "text-base-content", "{format_rows(count)}" }
                        }
                    }
                }
            }
        }
        None => rsx! {
            div { class: "opacity-50 text-sm italic", "No statistics available" }
        },
    }
}
