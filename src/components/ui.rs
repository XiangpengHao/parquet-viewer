//! Shared Tailwind utility bundles reused across components.
//! All values match the existing visual styling so the UI looks the same,
//! but the class strings now live in one place for easier maintenance.

pub const PANEL: &str = "bg-white rounded-md border border-gray-300";
pub const PANEL_PADDED_TIGHT: &str = "bg-white rounded-lg border border-gray-300 p-2";

pub const INPUT_BASE: &str = "px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500";

pub const BUTTON_PRIMARY: &str =
    "px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 whitespace-nowrap";
pub const BUTTON_OUTLINE: &str = "px-4 py-2 border border-green-500 text-green-500 rounded-md hover:border-green-600 hover:text-green-600";
pub const BUTTON_GHOST: &str =
    "px-4 py-2 border border-green-500 text-green-500 rounded-md hover:bg-green-50";
