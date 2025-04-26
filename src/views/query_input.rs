use leptos::prelude::*;

use crate::utils::get_stored_value;

#[component]
pub fn QueryInput(
    user_input: ReadSignal<Option<String>>,
    on_user_submit_query: impl Fn(String) + 'static + Send + Copy,
) -> impl IntoView {
    let stored_api_key = get_stored_value("claude_api_key").unwrap_or_default();
    let (api_key, _) = signal(stored_api_key);

    Effect::new(move |_| {
        if let Some(window) = web_sys::window() {
            if let Ok(Some(storage)) = window.local_storage() {
                let _ = storage.set_item("claude_api_key", &api_key.get());
            }
        }
    });

    let (input_value, set_input_value) = signal(user_input.get_untracked());

    Effect::new(move |_| {
        set_input_value.set(user_input.get());
    });

    let key_down = move |ev: web_sys::KeyboardEvent| {
        if ev.key() == "Enter" {
            let input = input_value.get();
            if let Some(input) = input {
                on_user_submit_query(input);
            }
        }
    };

    let button_press = move |_ev: web_sys::MouseEvent| {
        let input = input_value.get();
        if let Some(input) = input {
            on_user_submit_query(input);
        }
    };

    view! {
        <div class="flex gap-2 items-center flex-col relative">
            <div class="w-full flex gap-2 items-center">
                <input
                    type="text"
                    on:input=move |ev| set_input_value(Some(event_target_value(&ev)))
                    prop:value=input_value
                    on:keydown=key_down
                    class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                />
                <div class="flex items-center gap-1">
                    <button
                        on:click=button_press
                        class="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 whitespace-nowrap"
                    >
                        "Run Query"
                    </button>
                    <div class="relative group">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            class="h-5 w-5 text-gray-500 hover:text-gray-700 cursor-help"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                        >
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                            />
                        </svg>
                        <div class="absolute bottom-full right-0 mb-2 w-64 p-2 bg-gray-800 text-white text-xs rounded shadow-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none">
                            "SQL (begin with 'SELECT') or natural language, your choice!"
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }
}
