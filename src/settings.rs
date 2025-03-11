use leptos::html::*;
use leptos::prelude::*;
use leptos::*;

pub(crate) const ANTHROPIC_API_KEY: &str = "claude_api_key";
pub(crate) const S3_ENDPOINT_KEY: &str = "s3_endpoint";
pub(crate) const S3_ACCESS_KEY_ID_KEY: &str = "s3_access_key_id";
pub(crate) const S3_SECRET_KEY_KEY: &str = "s3_secret_key";

pub(crate) fn get_stored_value(key: &str, default: &str) -> String {
    let window = web_sys::window().unwrap();
    let storage = window.local_storage().unwrap().unwrap();
    storage
        .get_item(key)
        .unwrap()
        .unwrap_or_else(|| default.to_string())
}

fn save_to_storage(key: &str, value: &str) {
    if let Some(window) = web_sys::window() {
        if let Ok(Some(storage)) = window.local_storage() {
            let _ = storage.set_item(key, value);
        }
    }
}

#[component]
pub fn Settings(show: ReadSignal<bool>, set_show: WriteSignal<bool>) -> impl IntoView {
    let (anthropic_key, set_anthropic_key) = signal(get_stored_value(ANTHROPIC_API_KEY, ""));
    let (s3_endpoint, set_s3_endpoint) = signal(get_stored_value(
        S3_ENDPOINT_KEY,
        "https://s3.amazonaws.com",
    ));
    let (s3_access_key_id, set_s3_access_key_id) =
        signal(get_stored_value(S3_ACCESS_KEY_ID_KEY, ""));
    let (s3_secret_key, set_s3_secret_key) = signal(get_stored_value(S3_SECRET_KEY_KEY, ""));

    let close_modal = move |_| {
        set_show.set(false);
    };

    let button_close = move |ev: ev::MouseEvent| {
        ev.stop_propagation();
        set_show.set(false);
    };

    let stop_propagation = move |ev: ev::MouseEvent| {
        ev.stop_propagation();
    };

    view! {
        <Show when=move || show.get() fallback=|| ()>
            <div
                class="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-auto h-full w-full z-50 flex items-center justify-center transition-opacity duration-300 ease-in-out"
                on:click=close_modal
            >
                <div
                    class="relative bg-white rounded-lg shadow-xl p-8 mx-4 my-8 max-w-4xl w-full max-h-[90vh] flex flex-col transform transition-transform duration-300"
                    on:click=stop_propagation
                >
                    // Header with close button
                    <div class="flex justify-between items-center mb-2">
                        <h2 class="text-2xl font-bold">"Settings"</h2>
                        <button
                            class="text-gray-400 hover:text-gray-600 p-2 rounded-lg transition-colors duration-200"
                            on:click=close_modal
                            aria-label="Close"
                        >
                            <svg
                                xmlns="http://www.w3.org/2000/svg"
                                class="h-6 w-6"
                                fill="none"
                                viewBox="0 0 24 24"
                                stroke="currentColor"
                            >
                                <path
                                    stroke-linecap="round"
                                    stroke-linejoin="round"
                                    stroke-width="2"
                                    d="M6 18L18 6M6 6l12 12"
                                />
                            </svg>
                        </button>
                    </div>

                    // Scrollable content with increased spacing
                    <div
                        class="space-y-8 overflow-y-auto flex-1"
                        style="max-height: calc(90vh - 160px)"
                    >
                        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                            // Anthropic API Section
                            <div class="bg-gray-50 p-4 rounded-lg">
                                <h3 class="text-xl font-medium mb-5">"Natural Language to SQL"</h3>
                                <div class="mb-5">
                                    <label class="block text-sm font-medium text-gray-700 mb-2">
                                        "Claude API Key"
                                        <a
                                            href="https://console.anthropic.com/account/keys"
                                            target="_blank"
                                            class="text-blue-500 hover:text-blue-700 ml-1 transition-colors duration-200"
                                        >
                                            "(get key)"
                                        </a>
                                    </label>
                                    <input
                                        type="password"
                                        on:input=move |ev| {
                                            let value = event_target_value(&ev);
                                            save_to_storage(ANTHROPIC_API_KEY, &value);
                                            set_anthropic_key.set(value);
                                        }
                                        prop:value=anthropic_key
                                        class="w-full px-4 py-3 text-base border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors duration-200"
                                    />
                                    <p class="mt-3 text-sm text-gray-600 italic">
                                        "If no API key is provided, the app will use Xiangpeng's personal token. Please use reasonably and "
                                        <a
                                            href="https://github.com/XiangpengHao"
                                            class="text-blue-500 hover:underline"
                                            target="_blank"
                                        >
                                            "consider donating"
                                        </a> " to support this service or provide your own API key."
                                    </p>
                                </div>
                            </div>

                            // S3 Configuration Section
                            <div class="bg-gray-50 p-6 rounded-lg">
                                <h3 class="text-xl font-medium mb-5">"S3 Configuration"</h3>
                                <div class="space-y-5">
                                    <div>
                                        <label class="block text-sm font-medium text-gray-700 mb-2">
                                            "S3 Endpoint"
                                        </label>
                                        <input
                                            type="text"
                                            on:input=move |ev| {
                                                let value = event_target_value(&ev);
                                                save_to_storage(S3_ENDPOINT_KEY, &value);
                                                set_s3_endpoint.set(value);
                                            }
                                            prop:value=s3_endpoint
                                            class="w-full px-4 py-3 text-base border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors duration-200"
                                        />
                                    </div>
                                    <div>
                                        <label class="block text-sm font-medium text-gray-700 mb-2">
                                            "Access Key ID"
                                        </label>
                                        <input
                                            type="text"
                                            on:input=move |ev| {
                                                let value = event_target_value(&ev);
                                                save_to_storage(S3_ACCESS_KEY_ID_KEY, &value);
                                                set_s3_access_key_id.set(value);
                                            }
                                            prop:value=s3_access_key_id
                                            class="w-full px-4 py-3 text-base border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors duration-200"
                                        />
                                    </div>
                                    <div>
                                        <label class="block text-sm font-medium text-gray-700 mb-2">
                                            "Secret Access Key"
                                        </label>
                                        <input
                                            type="password"
                                            on:input=move |ev| {
                                                let value = event_target_value(&ev);
                                                save_to_storage(S3_SECRET_KEY_KEY, &value);
                                                set_s3_secret_key.set(value);
                                            }
                                            prop:value=s3_secret_key
                                            class="w-full px-4 py-3 text-base border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors duration-200"
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    // Footer with Done button
                    <div class="mt-3 pt-2 border-t border-gray-200 flex justify-between items-center">
                        <div class="text-sm text-gray-600 text-left">
                            "Built by"
                            <a
                                href="https://xiangpeng.systems"
                                class="text-blue-500"
                                target="_blank"
                            >
                                Xiangpeng Hao
                            </a> "as part of "
                            <a
                                href="https://github.com/XiangpengHao/liquid-cache"
                                class="text-blue-500"
                                target="_blank"
                            >
                                LiquidCache
                            </a>
                        </div>
                        <button
                            on:click=button_close
                            class="px-5 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 transition-colors duration-200 text-base font-medium"
                        >
                            "Done"
                        </button>
                    </div>
                </div>
            </div>
        </Show>
    }
}
