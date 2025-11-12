//! Shared Tailwind utility bundles reused across components.
//! All values match the existing visual styling so the UI looks the same,
//! but the class strings now live in one place for easier maintenance.

use leptos::prelude::*;

pub const PANEL: &str = "bg-white rounded-md border border-gray-300";
pub const INPUT_BASE: &str = "px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500";

pub const BUTTON_PRIMARY: &str =
    "px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 whitespace-nowrap";
pub const BUTTON_OUTLINE: &str = "px-4 py-2 border border-green-500 text-green-500 rounded-md hover:border-green-600 hover:text-green-600";
pub const BUTTON_GHOST: &str =
    "px-4 py-2 border border-green-500 text-green-500 rounded-md hover:bg-green-50";

#[component]
pub fn Panel(#[prop(optional, into)] class: Option<String>, children: Children) -> impl IntoView {
    let extra = class.map(|c| c.trim().to_string()).unwrap_or_default();
    let combined = if extra.is_empty() {
        PANEL.to_string()
    } else {
        format!("{PANEL} {extra}")
    };
    view! {
        <div class=combined>
            {children()}
        </div>
    }
}

#[component]
pub fn SectionHeader(
    #[prop(into)] title: String,
    #[prop(optional)] subtitle: Option<String>,
    #[prop(optional, into)] class: Option<String>,
    #[prop(optional)] trailing: Option<AnyView>,
) -> impl IntoView {
    let extra = class.map(|c| c.trim().to_string()).unwrap_or_default();
    let base = "flex flex-col gap-1 sm:flex-row sm:items-center sm:justify-between";
    let classes = if extra.is_empty() {
        format!("{base}")
    } else {
        format!("{base} {extra}")
    };
    view! {
        <div class=classes>
            <div>
                <h2 class="text-gray-900 font-semibold">{title}</h2>
                {subtitle
                    .map(|text| view! { <p class="text-xs text-gray-500">{text}</p> }.into_any())
                    .unwrap_or_else(|| view! { <span></span> }.into_any())}
            </div>
            <div class="flex items-center gap-2">
                {trailing.unwrap_or_else(|| view! { <span></span> }.into_any())}
            </div>
        </div>
    }
}
