use dioxus::prelude::*;

mod tool;

use tool::ParquetRewriterTool;

#[component]
pub fn ParquetRewriter() -> Element {
    rsx! {
        ParquetRewriterTool {}
    }
}
