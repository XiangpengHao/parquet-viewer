use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_array::downcast_integer;
use arrow_cast::display::array_value_to_string;
use datafusion::physical_plan::ExecutionPlan;
use leptos::{logging, prelude::*};
use web_sys::js_sys;
use web_sys::wasm_bindgen::{JsCast, JsValue};

use crate::SESSION_CTX;
use crate::utils::{export_to_csv_inner, export_to_parquet_inner, format_arrow_type};
use crate::views::plan_visualizer::PhysicalPlan;
use crate::{ParquetResolved, utils::execute_query_inner};

// Helper macro for width configuration
macro_rules! width_for_type {
    ($t:ty, $width:expr) => {
        $width
    };
}

#[derive(Clone)]
pub(crate) struct QueryResult {
    id: usize,
    query_result: LocalResource<std::result::Result<ExecutionResult, String>>,
    generated_sql: LocalResource<Result<String, String>>,
    display: bool,
    user_input: String,
}

#[derive(Clone)]
pub(crate) struct ExecutionResult {
    record_batches: Arc<Vec<RecordBatch>>,
    physical_plan: Arc<dyn ExecutionPlan>,
}

const TOOLTIP_CLASSES: &str = "absolute bottom-full left-1/2 transform -translate-x-1/2 px-2 py-1 bg-gray-800 text-white text-xs rounded opacity-0 group-hover:opacity-100 whitespace-nowrap pointer-events-none";
const BASE_BUTTON_CLASSES: &str = "p-1 text-gray-500 hover:text-gray-700 relative group";
const SVG_CLASSES: &str = "h-5 w-5";

impl QueryResult {
    pub fn new(id: usize, user_query: String, parquet_table: Arc<ParquetResolved>) -> Self {
        let user_query_clone = user_query.clone();
        let generated_sql = LocalResource::new(move || {
            let user_query = user_query.clone();
            let parquet_table = parquet_table.clone();
            async move {
                crate::nl_to_sql::user_input_to_sql(&user_query, &parquet_table)
                    .await
                    .map_err(|e| e.to_string())
            }
        });
        let query_result = LocalResource::new(move || async move {
            let sql = generated_sql.await?;
            let (results, execution_plan) = execute_query_inner(&sql, &SESSION_CTX)
                .await
                .map_err(|e| e.to_string())?;
            let row_cnt = results.iter().map(|r| r.num_rows()).sum::<usize>();
            logging::log!("finished executing: {:?}, row_cnt: {}", sql, row_cnt);
            Ok(ExecutionResult {
                record_batches: Arc::new(results),
                physical_plan: execution_plan,
            })
        });

        Self {
            id,
            query_result,
            generated_sql,
            user_input: user_query_clone,
            display: true,
        }
    }

    pub(crate) fn display(&self) -> bool {
        self.display
    }

    pub(crate) fn toggle_display(&mut self) {
        self.display = !self.display;
    }

    pub(crate) fn id(&self) -> usize {
        self.id
    }
}

#[component]
pub fn QueryResultViewInner(result: ExecutionResult, sql: String, id: usize) -> impl IntoView {
    let (show_plan, set_show_plan) = signal(false);
    let query_result_clone1 = result.record_batches.clone();
    let query_result_clone2 = result.record_batches.clone();
    let merged_record_batch = concat_batches(
        &result.record_batches[0].schema(),
        result.record_batches.iter().collect::<Vec<_>>(),
    )
    .expect("Failed to merge record batches");
    let sql_clone = sql.clone();
    let schema = merged_record_batch.schema();
    let total_rows = merged_record_batch.num_rows();
    let (current_page, set_current_page) = signal(1);
    let visible_rows = move || (current_page.get() * 20).min(total_rows);
    let table_container = NodeRef::<leptos::html::Div>::new();

    let handle_scroll = move |ev: web_sys::Event| {
        let target = ev.target().unwrap();
        let container = target.dyn_into::<web_sys::HtmlElement>().unwrap();

        let scroll_top = container.scroll_top();
        let scroll_height = container.scroll_height();
        let client_height = container.client_height();

        if scroll_top >= (scroll_height - client_height - 1) {
            // Only load more if we have more rows to show
            if visible_rows() < total_rows {
                logging::log!("current_page: {}", current_page.get());
                set_current_page.update(|page| *page += 1);
            }
        }
    };

    let highlighted_sql_input = format!(
        "hljs.highlight({},{{ language: 'sql' }}).value",
        js_sys::JSON::stringify(&JsValue::from_str(&sql)).unwrap()
    );
    let highlighted_sql_input = match js_sys::eval(&highlighted_sql_input) {
        Ok(v) => v.as_string().unwrap(),
        Err(e) => {
            logging::log!("Error highlighting SQL: {e:?}");
            sql
        }
    };

    view! {
        <div class="flex items-center mb-2 mt-2">
            <div class="w-3/4 font-mono overflow-auto relative group max-h-[200px]">
                <pre class="whitespace-pre rounded">
                    <code class="language-sql" inner_html=highlighted_sql_input></code>
                </pre>
            </div>
            <div class="w-1/4">
                <div class="flex justify-end">
                    <div class="flex items-center rounded-md">
                        <div class="text-gray-500 font-mono relative group">
                            <span class=TOOLTIP_CLASSES>{format!("SELECT * FROM view_{id}")}</span>
                            {format!("view_{id}")}
                        </div>
                        {
                            view! {
                                <button
                                    class=BASE_BUTTON_CLASSES
                                    aria-label="Export to CSV"
                                    on:click=move |_| export_to_csv_inner(&query_result_clone2)
                                >
                                    <span class=TOOLTIP_CLASSES>"Export to CSV"</span>
                                    <svg
                                        xmlns="http://www.w3.org/2000/svg"
                                        class=SVG_CLASSES
                                        fill="none"
                                        viewBox="0 0 24 24"
                                        stroke="currentColor"
                                    >
                                        <path
                                            stroke-linecap="round"
                                            stroke-linejoin="round"
                                            stroke-width="2"
                                            d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4"
                                        />
                                    </svg>
                                </button>
                                <button
                                    class=BASE_BUTTON_CLASSES
                                    aria-label="Export to Parquet"
                                    on:click=move |_| export_to_parquet_inner(&query_result_clone1)
                                >
                                    <span class=TOOLTIP_CLASSES>"Export to Parquet"</span>
                                    <svg
                                        xmlns="http://www.w3.org/2000/svg"
                                        class=SVG_CLASSES
                                        fill="none"
                                        viewBox="0 0 24 24"
                                        stroke="currentColor"
                                    >
                                        <path
                                            stroke-linecap="round"
                                            stroke-linejoin="round"
                                            stroke-width="2"
                                            d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4"
                                        />
                                    </svg>
                                </button>
                                <button
                                    class=format!("{} animate-on-click", BASE_BUTTON_CLASSES)
                                    aria-label="Copy SQL"
                                    on:click=move |_| {
                                        let window = web_sys::window().unwrap();
                                        let navigator = window.navigator();
                                        let clipboard = navigator.clipboard();
                                        let _ = clipboard.write_text(&sql_clone);
                                    }
                                >
                                    <style>
                                        {".animate-on-click:active { animation: quick-bounce 0.2s; }
                                        @keyframes quick-bounce {
                                        0%, 100% { transform: scale(1); }
                                        50% { transform: scale(0.95); }
                                        }"}
                                    </style>
                                    <span class=TOOLTIP_CLASSES>"Copy SQL"</span>
                                    <svg
                                        xmlns="http://www.w3.org/2000/svg"
                                        class=SVG_CLASSES
                                        fill="none"
                                        viewBox="0 0 24 24"
                                        stroke="currentColor"
                                    >
                                        <path
                                            stroke-linecap="round"
                                            stroke-linejoin="round"
                                            stroke-width="2"
                                            d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3"
                                        />
                                    </svg>
                                </button>
                                <button
                                    class=format!(
                                        "{} {}",
                                        BASE_BUTTON_CLASSES,
                                        if show_plan() { "text-blue-600" } else { "" },
                                    )
                                    aria-label="Execution plan"
                                    on:click=move |_| set_show_plan.update(|v| *v = !*v)
                                >
                                    <span class=TOOLTIP_CLASSES>"Execution plan"</span>
                                    <svg
                                        xmlns="http://www.w3.org/2000/svg"
                                        class=SVG_CLASSES
                                        fill="none"
                                        viewBox="0 0 24 24"
                                        stroke="currentColor"
                                    >
                                        <path
                                            stroke-linecap="round"
                                            stroke-linejoin="round"
                                            stroke-width="2"
                                            d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"
                                        />
                                    </svg>
                                </button>
                            }
                        }
                    </div>
                </div>
            </div>
        </div>

        {move || {
            show_plan()
                .then(|| {
                    view! {
                        <div class="mb-4">
                            <PhysicalPlan physical_plan=result.physical_plan.clone() />
                        </div>
                    }
                })
        }}

        <div
            class="max-h-[32rem] overflow-auto relative"
            node_ref=table_container
            on:scroll=handle_scroll
        >
            <table class="min-w-full bg-white table-fixed">
                <thead class="sticky top-0 z-10 bg-gray-50">
                    <tr>
                        {schema
                            .fields()
                            .iter()
                            .map(|field| {
                                let data_type = field.data_type();
                                let width = downcast_integer! {
                                    data_type => (width_for_type, ""),
                                    _ => "min-w-[200px]"
                                };
                                view! {
                                    <th class=format!("px-1 py-1 text-left {width} leading-tight")>
                                        <div class="truncate" title=field.name().clone()>
                                            {field.name().clone()}
                                        </div>
                                        <div
                                            class="text-xs text-gray-400 truncate"
                                            title=format_arrow_type(field.data_type())
                                        >
                                            {format_arrow_type(field.data_type())}
                                        </div>
                                    </th>
                                }
                            })
                            .collect::<Vec<_>>()}
                    </tr>
                </thead>
                <tbody>
                    <For
                        each=move || (0..visible_rows())
                        key=|row_idx| *row_idx
                        children=move |row_idx| {
                            view! {
                                <tr class="hover:bg-gray-50">
                                    {(0..merged_record_batch.num_columns())
                                        .map(|col_idx| {
                                            let column = merged_record_batch.column(col_idx);
                                            let cell_value = array_value_to_string(column.as_ref(), row_idx).unwrap_or_else(|_| "NULL".to_string());

                                            view! {
                                                <td class="px-1 py-1 leading-tight text-gray-700 break-words">
                                                    {if cell_value.len() > 100 {
                                                        view! {
                                                            <details class="custom-details relative">
                                                                <style>
                                                                    {".custom-details > summary { list-style: none; }
                                                                    .custom-details > summary::-webkit-details-marker { display: none; }
                                                                    .custom-details > summary::after {
                                                                    content: '...';
                                                                    font-size: 0.7em;
                                                                    margin-left: 5px;
                                                                    color: #6B7280;
                                                                    display: inline-block;
                                                                    transition: transform 0.2s;
                                                                    }
                                                                    .custom-details[open] > summary::after {
                                                                    content: '';
                                                                    }"}
                                                                </style>
                                                                <summary class="outline-none cursor-pointer">
                                                                    <span class="text-gray-700">
                                                                        {cell_value.chars().take(100).collect::<String>()}
                                                                    </span>
                                                                </summary>
                                                                <div class="mt-1 text-gray-700">
                                                                    {cell_value.chars().skip(100).collect::<String>()}
                                                                </div>
                                                            </details>
                                                        }
                                                            .into_any()
                                                    } else {
                                                        view! { <span>{cell_value}</span> }.into_any()
                                                    }}
                                                </td>
                                            }
                                        })
                                        .collect::<Vec<_>>()}
                                </tr>
                            }
                        }
                    />
                </tbody>
            </table>
        </div>
    }
}

#[component]
pub fn QueryResultView(
    result: QueryResult,
    toggle_display: impl Fn(usize) + 'static + Send + Clone,
) -> impl IntoView {
    let id = result.id;

    let (progress, set_progress) = signal("Generating SQL...".to_string());

    let toggle_display = toggle_display.clone();

    view! {
        <div class="p-3 bg-white border border-gray-300 rounded-md hover:shadow-md transition-shadow duration-200">
            <div class="flex justify-between items-center border-b border-gray-100 mb-2">
                <div class="text-gray-500">{result.user_input}</div>
                <div class="flex items-center">
                    <div class="text-gray-500 mr-2">
                        {move || {
                            let now = js_sys::Date::new_0();
                            format!(
                                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                                now.get_full_year(),
                                now.get_month() + 1,
                                now.get_date(),
                                now.get_hours(),
                                now.get_minutes(),
                                now.get_seconds(),
                            )
                        }}
                    </div>
                    <div>
                        <button
                            class=format!("{} hover:text-red-600", BASE_BUTTON_CLASSES)
                            aria-label="Hide"
                            on:click=move |_| toggle_display(id)
                        >
                            <span class=TOOLTIP_CLASSES>"Hide"</span>
                            <svg
                                xmlns="http://www.w3.org/2000/svg"
                                class=SVG_CLASSES
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
                </div>
            </div>
            <Suspense fallback=move || {
                view! { <div>{move || progress()}</div> }
            }>
                {move || {
                    Suspend::new(async move {
                        let sql = match result.generated_sql.await {
                            Ok(sql) => sql,
                            Err(e) => {
                                return view! { <pre>Error generating SQL: {e}</pre> }.into_any();
                            }
                        };
                        let message = format!("Executing SQL...\n\n{sql}");
                        set_progress.set(message);
                        let result = result.query_result.await;
                        match result {
                            Ok(result) => {

                                view! { <QueryResultViewInner result=result sql=sql id=id /> }
                                    .into_any()
                            }
                            Err(e) => {
                                let message = format!(
                                    "Error executing query, context below:\nSQL:\t{sql}\nError:\t{e}",
                                );
                                view! { <pre>{message}</pre> }.into_any()
                            }
                        }
                    })
                }}
            </Suspense>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    use super::*;

    wasm_bindgen_test_configure!(run_in_browser);

    fn test_render_query_result_view(result: ExecutionResult) {
        let document = document();
        let test_wrapper = document.create_element("section").unwrap();
        let _ = document.body().unwrap().append_child(&test_wrapper);

        let _dispose = mount_to(test_wrapper.clone().unchecked_into(), move || {
            view! {
            <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/sql.min.js"></script>
            <QueryResultViewInner result=result.clone() sql="SELECT * FROM test".to_string() id=0 /> }
        });
    }

    #[wasm_bindgen_test]
    async fn test_query_result_view_with_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let plan = PlaceholderRowExec::new(schema.clone());
        let result = ExecutionResult {
            record_batches: Arc::new(vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
                )
                .unwrap(),
            ]),
            physical_plan: Arc::new(plan),
        };
        test_render_query_result_view(result);
    }
}
