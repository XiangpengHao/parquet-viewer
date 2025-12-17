//! Shared DaisyUI component classes reused across components.
//! Using DaisyUI components while maintaining the existing green color scheme
//! for consistency with the original design.

use dioxus::prelude::*;

pub const PANEL: &str = "card bg-base-100 border border-base-300";
pub const INPUT_BASE: &str = "input input-bordered focus:outline-none focus:ring-2 focus:ring-green-500";

pub const BUTTON_PRIMARY: &str = "btn bg-green-500 text-white hover:bg-green-600 border-0";
pub const BUTTON_OUTLINE: &str = "btn btn-outline border-green-500 text-green-500 hover:border-green-600 hover:text-green-600 hover:bg-transparent";
pub const BUTTON_GHOST: &str = "btn btn-ghost border border-green-500 text-green-500 hover:bg-green-50";

#[component]
pub fn Panel(class: Option<String>, children: Element) -> Element {
    let extra = class.map(|c| c.trim().to_string()).unwrap_or_default();
    let combined = if extra.is_empty() {
        PANEL.to_string()
    } else {
        format!("{PANEL} {extra}")
    };

    rsx! {
        div { class: "{combined}", {children} }
    }
}

#[component]
pub fn SectionHeader(
    title: String,
    subtitle: Option<String>,
    class: Option<String>,
    trailing: Option<Element>,
) -> Element {
    let extra = class.map(|c| c.trim().to_string()).unwrap_or_default();
    let base = "flex flex-col gap-1 sm:flex-row sm:items-center sm:justify-between";
    let classes = if extra.is_empty() {
        base.to_string()
    } else {
        format!("{base} {extra}")
    };

    let trailing = trailing.unwrap_or_else(|| rsx!(span {}));

    rsx! {
        div { class: "{classes}",
            div {
                h2 { class: "text-base-content font-semibold", "{title}" }
                if let Some(text) = subtitle {
                    p { class: "text-xs text-base-content opacity-60", "{text}" }
                } else {
                    span {}
                }
            }
            div { class: "flex items-center gap-2", {trailing} }
        }
    }
}
