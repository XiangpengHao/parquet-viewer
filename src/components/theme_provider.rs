use dioxus::prelude::*;
use web_sys::window;

const THEME_STORAGE_KEY: &str = "parquet_viewer_theme";

#[derive(Clone, Copy, PartialEq)]
pub enum Theme {
    Light,
    Dark,
}

impl Theme {
    pub fn as_str(&self) -> &'static str {
        match self {
            Theme::Light => "light",
            Theme::Dark => "dark",
        }
    }

    pub fn toggle(&self) -> Self {
        match self {
            Theme::Light => Theme::Dark,
            Theme::Dark => Theme::Light,
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "dark" => Theme::Dark,
            _ => Theme::Light,
        }
    }
}

/// Get the stored theme preference, or default to light theme
fn get_stored_theme() -> Theme {
    window()
        .and_then(|w| w.local_storage().ok().flatten())
        .and_then(|storage| storage.get_item(THEME_STORAGE_KEY).ok().flatten())
        .map(|theme_str| Theme::from_str(&theme_str))
        .unwrap_or(Theme::Light)
}

/// Save the theme preference to localStorage
fn save_theme(theme: Theme) {
    if let Some(storage) = window().and_then(|w| w.local_storage().ok().flatten()) {
        let _ = storage.set_item(THEME_STORAGE_KEY, theme.as_str());
    }
}

/// Apply the theme to the document's HTML element
fn apply_theme_to_html(theme: Theme) {
    if let Some(document) = window().and_then(|w| w.document())
        && let Some(html) = document.document_element()
    {
        let _ = html.set_attribute("data-theme", theme.as_str());
    }
}

/// Hook to manage theme state and provide theme toggle functionality
pub fn use_theme() -> (Signal<Theme>, Callback<()>) {
    let mut theme = use_signal(get_stored_theme);

    // Apply theme on mount
    use_effect(move || {
        apply_theme_to_html(theme());
    });

    let toggle_theme = use_callback(move |_| {
        let new_theme = theme().toggle();
        theme.set(new_theme);
        save_theme(new_theme);
        apply_theme_to_html(new_theme);
    });

    (theme, toggle_theme)
}
