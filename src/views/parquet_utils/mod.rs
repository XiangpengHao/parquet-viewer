use dioxus::prelude::*;

mod parquet_merge;

use parquet_merge::ParquetMerge;

/// Available utility tools in the Parquet Utils section.
/// Add new variants here to extend with additional tools.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UtilTool {
    Merge,
    // Future tools can be added here:
    // Split,
    // Rewrite,
    // Convert,
}

impl UtilTool {
    fn label(&self) -> &'static str {
        match self {
            UtilTool::Merge => "Merge",
            // UtilTool::Split => "Split",
            // UtilTool::Rewrite => "Rewrite",
        }
    }

    fn description(&self) -> &'static str {
        match self {
            UtilTool::Merge => "Combine multiple Parquet files into one",
            // UtilTool::Split => "Split a Parquet file into multiple parts",
            // UtilTool::Rewrite => "Rewrite with different compression settings",
        }
    }

    fn all() -> &'static [UtilTool] {
        &[
            UtilTool::Merge,
            // Add new tools to this list
        ]
    }
}

#[component]
pub fn ParquetUtils() -> Element {
    let mut active_tool = use_signal(|| UtilTool::Merge);

    rsx! {
        div { class: "space-y-5",
            // Header
            div {
                h1 { class: "text-primary text-xl font-semibold tracking-tight",
                    "Utils"
                }
                p { class: "text-tertiary text-sm mt-1",
                    "Tools for working with Parquet files"
                }
            }

            // Tool tabs
            div { class: "flex items-center gap-1",
                for tool in UtilTool::all() {
                    button {
                        key: "{tool:?}",
                        class: if active_tool() == *tool { "tab-soft active" } else { "tab-soft" },
                        onclick: move |_| active_tool.set(*tool),
                        "{tool.label()}"
                    }
                }
            }

            // Tool description
            p { class: "text-tertiary text-xs",
                "{active_tool().description()}"
            }

            // Divider
            div { class: "divider-soft" }

            // Tool content
            match active_tool() {
                UtilTool::Merge => rsx! { ParquetMerge {} },
                // UtilTool::Split => rsx! { ParquetSplit {} },
                // UtilTool::Rewrite => rsx! { ParquetRewrite {} },
            }
        }
    }
}
