use dioxus::prelude::*;

use crate::components::ui::{BUTTON_PRIMARY, INPUT_BASE};

#[component]
pub fn QueryInput(
    value: String,
    on_value_change: EventHandler<String>,
    on_user_submit_query: EventHandler<String>,
) -> Element {
    let on_submit_enter = {
        let value = value.clone();
        move || on_user_submit_query.call(value.clone())
    };
    let on_submit_click = {
        let value = value.clone();
        move || on_user_submit_query.call(value.clone())
    };

    rsx! {
        div { class: "flex w-full flex-col gap-2",
            div { class: "flex w-full flex-col gap-2 sm:flex-row sm:items-center",
                input {
                    r#type: "text",
                    class: "flex-1 {INPUT_BASE}",
                    value: "{value}",
                    oninput: move |ev| on_value_change.call(ev.value()),
                    onkeydown: move |ev| {
                        if ev.key() == Key::Enter {
                            on_submit_enter();
                        }
                    },
                }
                div { class: "flex items-center gap-1",
                    button {
                        class: "{BUTTON_PRIMARY}",
                        onclick: move |_| on_submit_click(),
                        "Run Query"
                    }
                    div { class: "relative group",
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            class: "h-5 w-5 text-gray-500 hover:text-gray-700 cursor-help",
                            fill: "none",
                            view_box: "0 0 24 24",
                            stroke: "currentColor",
                            path {
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                stroke_width: "2",
                                d: "M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z",
                            }
                        }
                        div { class: "pointer-events-none absolute bottom-full right-0 mb-2 w-64 rounded bg-gray-800 p-2 text-xs text-white opacity-0 shadow-lg transition-opacity duration-200 group-hover:opacity-100",
                            "SQL (begin with 'SELECT') or natural language, your choice!"
                        }
                    }
                }
            }
        }
    }
}
