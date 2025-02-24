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

    view! {
        <div class=move || {
            if show.get() {
                "fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full flex items-center justify-center z-50"
            } else {
                "hidden"
            }
        }>
            <div class="relative bg-white rounded-lg shadow-xl p-8 max-w-2xl w-full mx-4">
                <div class="flex justify-between items-center mb-6">
                    <h2 class="text-2xl font-bold">"Settings"</h2>
                    <button
                        class="text-gray-400 hover:text-gray-600 p-2 rounded-lg"
                        on:click=move |ev| {
                            ev.prevent_default();
                            set_show.set(false);
                        }
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

                <div class="space-y-6">

                    // Anthropic API Section
                    <div>
                        <h3 class="text-lg font-medium mb-4">"Natural Language to SQL"</h3>
                        <div class="mb-4">
                            <label class="block text-sm font-medium text-gray-700 mb-1">
                                "Claude API Key"
                                <a
                                    href="https://console.anthropic.com/account/keys"
                                    target="_blank"
                                    class="text-blue-500 hover:text-blue-700 ml-1"
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
                                class="w-full px-3 py-2 border border-gray-300 rounded-md"
                            />
                            <p class="mt-2 text-sm text-gray-600 italic">
                                "If no API key is provided, the app will use Xiangpeng's personal token. Please use reasonably and "
                                <a href="https://github.com/XiangpengHao" class="text-blue-500 hover:underline" target="_blank">
                                    "consider donating"
                                </a>
                                " to support this service or provide your own API key."
                            </p>
                        </div>
                    </div>

                    // S3 Configuration Section
                    <div>
                        <h3 class="text-lg font-medium mb-4">"S3 Configuration"</h3>
                        <div class="space-y-4">
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">
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
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md"
                                />
                            </div>
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">
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
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md"
                                />
                            </div>
                            <div>
                                <label class="block text-sm font-medium text-gray-700 mb-1">
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
                                    class="w-full px-3 py-2 border border-gray-300 rounded-md"
                                />
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }.into_view()
}
