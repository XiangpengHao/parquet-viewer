//! Shared UI component classes - Notion/Descript inspired styling
//! Subtle shadows, soft colors, refined typography

use dioxus::prelude::*;

// Soft panel with subtle shadow instead of hard border
pub const PANEL: &str = "panel-soft";

// Input with soft styling
pub const INPUT_BASE: &str = "input-soft px-3 py-2 text-sm w-full";

// Button variants
pub const BUTTON_PRIMARY: &str = "btn-primary-soft cursor-pointer";
pub const BUTTON_GHOST: &str = "btn-soft cursor-pointer";
pub const BUTTON_OUTLINE: &str = "btn-soft cursor-pointer";

// Text styles

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

    let trailing = trailing.unwrap_or_else(|| rsx!(
        span {}

    ));

    rsx! {
        div { class: "{classes}",
            div {
                h2 { class: "text-primary font-semibold text-sm", "{title}" }
                if let Some(text) = subtitle {
                    p { class: "text-tertiary text-xs", "{text}" }
                } else {
                    span {}
                }
            }
            div { class: "flex items-center gap-2", {trailing} }
        }
    }
}

/// Sidebar navigation icon button
#[component]
pub fn SidebarIcon(icon: Element, active: bool, tooltip: String) -> Element {
    let class = if active {
        "sidebar-icon active"
    } else {
        "sidebar-icon"
    };

    rsx! {
        div { class: "{class}", title: "{tooltip}", {icon} }
    }
}
