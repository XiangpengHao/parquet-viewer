use dioxus::prelude::*;

use crate::{
    components::ui::{BUTTON_PRIMARY, INPUT_BASE, PANEL, SectionHeader},
    utils::{get_stored_value, save_to_storage},
};

pub(crate) const ANTHROPIC_API_KEY: &str = "claude_api_key";
pub(crate) const S3_ENDPOINT_KEY: &str = "s3_endpoint";
pub(crate) const S3_ACCESS_KEY_ID_KEY: &str = "s3_access_key_id";
pub(crate) const S3_SECRET_KEY_KEY: &str = "s3_secret_key";

#[component]
pub fn Settings(show: bool, on_close: EventHandler<()>) -> Element {
    let mut anthropic_key = use_signal(|| get_stored_value(ANTHROPIC_API_KEY).unwrap_or_default());
    let mut s3_endpoint = use_signal(|| {
        get_stored_value(S3_ENDPOINT_KEY).unwrap_or("https://s3.amazonaws.com".to_string())
    });
    let mut s3_access_key_id =
        use_signal(|| get_stored_value(S3_ACCESS_KEY_ID_KEY).unwrap_or_default());
    let mut s3_secret_key = use_signal(|| get_stored_value(S3_SECRET_KEY_KEY).unwrap_or_default());

    if !show {
        return rsx! {};
    }

    rsx! {
        div {
            class: "modal modal-open",
            onclick: move |_| on_close.call(()),
            div {
                class: "modal-box max-w-4xl w-full max-h-[90vh] p-8",
                onclick: move |ev| ev.stop_propagation(),
                SectionHeader {
                    title: "Settings".to_string(),
                    subtitle: None,
                    class: Some("mb-4".to_string()),
                    trailing: Some(rsx! {
                        button {
                            class: "btn btn-sm btn-circle btn-ghost",
                            onclick: move |ev| {
                                ev.stop_propagation();
                                on_close.call(());
                            },
                            aria_label: "Close",
                            svg {
                                xmlns: "http://www.w3.org/2000/svg",
                                class: "h-6 w-6",
                                fill: "none",
                                view_box: "0 0 24 24",
                                stroke: "currentColor",
                                path {
                                    stroke_linecap: "round",
                                    stroke_linejoin: "round",
                                    stroke_width: "2",
                                    d: "M6 18L18 6M6 6l12 12",
                                }
                            }
                        }
                    }),
                }

                div {
                    class: "space-y-8 overflow-y-auto flex-1",
                    style: "max-height: calc(90vh - 160px)",
                    div { class: "grid grid-cols-1 md:grid-cols-2 gap-4",
                        div { class: "card bg-base-200 p-4",
                            h3 { class: "text-lg font-medium mb-5", "Natural Language to SQL" }
                            div { class: "mb-5",
                                label { class: "label font-medium",
                                    "Claude API Key"
                                    a {
                                        href: "https://console.anthropic.com/account/keys",
                                        target: "_blank",
                                        class: "link link-primary ml-1",
                                        "(get key)"
                                    }
                                }
                                input {
                                    r#type: "password",
                                    class: "w-full {INPUT_BASE}",
                                    value: "{anthropic_key()}",
                                    oninput: move |ev| {
                                        let value = ev.value();
                                        save_to_storage(ANTHROPIC_API_KEY, &value);
                                        anthropic_key.set(value);
                                    },
                                }
                                p { class: "mt-3 opacity-75 italic text-sm",
                                    "If no API key is provided, it uses Xiangpeng's personal token -- use reasonably and "
                                    a {
                                        href: "https://github.com/XiangpengHao",
                                        class: "link link-primary hover:underline",
                                        target: "_blank",
                                        "consider donating"
                                    }
                                    "; no data is collected, but CloudFlare may temporarily log the prompt and schema."
                                }
                            }
                        }

                        div { class: "card bg-base-200 p-6",
                            h3 { class: "text-lg font-medium mb-5", "S3 Configuration" }
                            div { class: "space-y-3",
                                div {
                                    label { class: "label font-medium",
                                        "S3 Endpoint"
                                    }
                                    input {
                                        r#type: "text",
                                        class: "w-full {INPUT_BASE}",
                                        value: "{s3_endpoint()}",
                                        oninput: move |ev| {
                                            let value = ev.value();
                                            save_to_storage(S3_ENDPOINT_KEY, &value);
                                            s3_endpoint.set(value);
                                        },
                                    }
                                }
                                div {
                                    label { class: "label font-medium",
                                        "Access Key ID"
                                    }
                                    input {
                                        r#type: "text",
                                        class: "w-full {INPUT_BASE}",
                                        value: "{s3_access_key_id()}",
                                        oninput: move |ev| {
                                            let value = ev.value();
                                            save_to_storage(S3_ACCESS_KEY_ID_KEY, &value);
                                            s3_access_key_id.set(value);
                                        },
                                    }
                                }
                                div {
                                    label { class: "label font-medium",
                                        "Secret Access Key"
                                    }
                                    input {
                                        r#type: "password",
                                        class: "w-full {INPUT_BASE}",
                                        value: "{s3_secret_key()}",
                                        oninput: move |ev| {
                                            let value = ev.value();
                                            save_to_storage(S3_SECRET_KEY_KEY, &value);
                                            s3_secret_key.set(value);
                                        },
                                    }
                                }
                            }
                        }
                    }
                }

                div { class: "modal-action mt-3 pt-2 border-t border-base-300 flex justify-between items-center w-full",
                    div { class: "opacity-75 text-left",
                        "Built by "
                        a {
                            href: "https://xiangpeng.systems",
                            class: "link link-primary",
                            target: "_blank",
                            "Xiangpeng Hao"
                        }
                        " as a part of "
                        a {
                            href: "https://github.com/XiangpengHao/liquid-cache",
                            class: "link link-primary",
                            target: "_blank",
                            "LiquidCache"
                        }
                    }
                    button {
                        class: "{BUTTON_PRIMARY}",
                        onclick: move |ev| {
                            ev.stop_propagation();
                            on_close.call(());
                        },
                        "Done"
                    }
                }
            }
        }
    }
}
