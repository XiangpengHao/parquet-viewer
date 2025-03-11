use std::sync::Arc;

use arrow::array::{Array, types::*};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::{
    common::cast::{as_binary_array, as_binary_view_array, as_string_view_array},
    physical_plan::{
        DisplayFormatType, ExecutionPlan, ExecutionPlanVisitor, accept,
        display::DisplayableExecutionPlan,
    },
};
use leptos::{logging, prelude::*};
use parquet::arrow::ArrowWriter;
use web_sys::js_sys;
use web_sys::wasm_bindgen::JsCast;

fn download_data(file_name: &str, data: Vec<u8>) {
    let blob =
        web_sys::Blob::new_with_u8_array_sequence(&js_sys::Array::of1(&data.into())).unwrap();
    let url = web_sys::Url::create_object_url_with_blob(&blob).unwrap();
    let a = web_sys::window()
        .unwrap()
        .document()
        .unwrap()
        .create_element("a")
        .unwrap();
    a.set_attribute("href", &url).unwrap();
    a.set_attribute("download", file_name).unwrap();
    a.dyn_ref::<web_sys::HtmlElement>().unwrap().click();
    web_sys::Url::revoke_object_url(&url).unwrap();
}

pub(crate) fn export_to_csv_inner(query_result: &[RecordBatch]) {
    let mut data = Vec::new();
    let mut writer = arrow::csv::WriterBuilder::new().build(&mut data);
    for batch in query_result {
        writer.write(batch).unwrap();
    }
    drop(writer);
    download_data("query_results.csv", data);
}

pub(crate) fn export_to_parquet_inner(query_result: &[RecordBatch]) {
    let mut buf = Vec::new();

    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::LZ4)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buf, query_result[0].schema(), Some(props))
        .expect("Failed to create parquet writer");

    // Write all record batches
    for batch in query_result {
        writer.write(batch).expect("Failed to write record batch");
    }

    writer.close().expect("Failed to close writer");

    download_data("query_results.parquet", buf);
}

#[derive(Debug, Clone)]
pub(crate) struct QueryResult {
    id: usize,
    sql_query: String,
    query_result: Arc<Vec<RecordBatch>>,
    physical_plan: Arc<dyn ExecutionPlan>,
    display: bool,
}

impl QueryResult {
    pub fn new(
        id: usize,
        sql_query: String,
        query_result: Arc<Vec<RecordBatch>>,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            id,
            sql_query,
            query_result,
            physical_plan,
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
pub fn QueryResultView(
    result: QueryResult,
    toggle_display: impl Fn(usize) + 'static,
) -> impl IntoView {
    let (show_plan, set_show_plan) = signal(false);
    let query_result_clone1 = result.query_result.clone();
    let query_result_clone2 = result.query_result.clone();
    let sql = result.sql_query.clone();
    let sql_clone = sql.clone();
    let id = result.id();
    let total_rows = result.query_result[0].num_rows();
    let (current_page, set_current_page) = signal(1);
    let visible_rows = move || (current_page.get() * 20).min(total_rows);
    let table_container = NodeRef::<leptos::html::Div>::new();

    let handle_scroll = move |ev: web_sys::Event| {
        let target = ev.target().unwrap();
        let container = target.dyn_into::<web_sys::HtmlElement>().unwrap();

        let scroll_top = container.scroll_top();
        let scroll_height = container.scroll_height();
        let client_height = container.client_height();

        if scroll_top >= scroll_height - client_height {
            // Only load more if we have more rows to show
            if visible_rows() < total_rows {
                logging::log!("current_page: {}", current_page.get());
                set_current_page.update(|page| *page += 1);
            }
        }
    };

    Effect::new(move |_| {
        let _window = web_sys::window().unwrap();
        let _ = js_sys::eval("hljs.highlightAll()");
        || ()
    });
    let tooltip_classes = "absolute bottom-full left-1/2 transform -translate-x-1/2 px-2 py-1 bg-gray-800 text-white text-xs rounded opacity-0 group-hover:opacity-100 whitespace-nowrap pointer-events-none";
    let base_button_classes = "p-1 text-gray-500 hover:text-gray-700 relative group";
    let svg_classes = "h-5 w-5";

    view! {
        <div class="p-3 bg-white border border-gray-300 rounded-md hover:shadow-md transition-shadow duration-200">
            <div class="flex items-center mb-4">
                <div class="w-3/4 font-mono text-sm overflow-auto relative group max-h-[200px]">
                    <pre class="whitespace-pre">
                        <code class="language-sql">{sql_clone}</code>
                    </pre>
                </div>
                <div class="w-1/4">
                    <div class="flex justify-end">
                        <div class="flex items-center rounded-md">
                            <div class="text-sm text-gray-500 font-mono relative group">
                                <span class=tooltip_classes>
                                    {format!("SELECT * FROM view_{}", id)}
                                </span>
                                {format!("view_{}", id)}
                            </div>
                            {
                                view! {
                                    <button
                                        class=base_button_classes
                                        aria-label="Export to CSV"
                                        on:click=move |_| export_to_csv_inner(&query_result_clone2)
                                    >
                                        <span class=tooltip_classes>"Export to CSV"</span>
                                        <svg
                                            xmlns="http://www.w3.org/2000/svg"
                                            class=svg_classes
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
                                        class=base_button_classes
                                        aria-label="Export to Parquet"
                                        on:click=move |_| export_to_parquet_inner(
                                            &query_result_clone1,
                                        )
                                    >
                                        <span class=tooltip_classes>"Export to Parquet"</span>
                                        <svg
                                            xmlns="http://www.w3.org/2000/svg"
                                            class=svg_classes
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
                                        class=format!("{} animate-on-click", base_button_classes)
                                        aria-label="Copy SQL"
                                        on:click=move |_| {
                                            let window = web_sys::window().unwrap();
                                            let navigator = window.navigator();
                                            let clipboard = navigator.clipboard();
                                            let _ = clipboard.write_text(&sql);
                                        }
                                    >
                                        <style>
                                            {".animate-on-click:active { animation: quick-bounce 0.2s; }
                                            @keyframes quick-bounce {
                                            0%, 100% { transform: scale(1); }
                                            50% { transform: scale(0.95); }
                                            }"}
                                        </style>
                                        <span class=tooltip_classes>"Copy SQL"</span>
                                        <svg
                                            xmlns="http://www.w3.org/2000/svg"
                                            class=svg_classes
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
                                            base_button_classes,
                                            if show_plan() { "text-blue-600" } else { "" },
                                        )
                                        aria-label="Execution plan"
                                        on:click=move |_| set_show_plan.update(|v| *v = !*v)
                                    >
                                        <span class=tooltip_classes>"Execution plan"</span>
                                        <svg
                                            xmlns="http://www.w3.org/2000/svg"
                                            class=svg_classes
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
                                    <button
                                        class=format!("{} hover:text-red-600", base_button_classes)
                                        aria-label="Hide"
                                        on:click=move |_| toggle_display(id)
                                    >
                                        <span class=tooltip_classes>"Hide"</span>
                                        <svg
                                            xmlns="http://www.w3.org/2000/svg"
                                            class=svg_classes
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
                    <thead class="sticky top-0 z-10 bg-white shadow-[0_1px_0_rgba(229,231,235)]">
                        <tr class="border-b border-gray-200">
                            {result
                                .query_result[0]
                                .schema()
                                .fields()
                                .iter()
                                .map(|field| {
                                    view! {
                                        <th class="px-4 py-1 text-left w-48 min-w-48 leading-tight text-gray-700">
                                            <div class="truncate" title=field.name().clone()>
                                                {field.name().clone()}
                                            </div>
                                            <div
                                                class="text-xs text-gray-600 truncate"
                                                title=field.data_type().to_string()
                                            >
                                                {field.data_type().to_string()}
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
                                        {(0..result.query_result[0].num_columns())
                                            .map(|col_idx| {
                                                let column = result.query_result[0].column(col_idx);
                                                let cell_value = column.as_ref().value_to_string(row_idx);
                                                view! {
                                                    <td class="px-4 py-1 w-48 min-w-48 leading-tight text-gray-700">
                                                        {cell_value}
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
        </div>
    }
}

trait ArrayExt {
    fn value_to_string(&self, index: usize) -> String;
}

impl ArrayExt for dyn Array {
    fn value_to_string(&self, index: usize) -> String {
        use arrow::array::*;

        let array = self;

        downcast_primitive_array!(
            array => {
                format!("{:?}", array.value(index))
            }
            DataType::Utf8 => {
                let array = as_string_array(array);
                array.value(index).to_string()
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array).unwrap();
                array.value(index).to_string()
            }
            DataType::Binary => {
                let array = as_binary_array(array).unwrap();
                let value = array.value(index);
                String::from_utf8_lossy(value).to_string()
            }
            DataType::BinaryView => {
                let array = as_binary_view_array(array).unwrap();
                let value = array.value(index);
                String::from_utf8_lossy(value).to_string()
            }
            DataType::Dictionary(key_type, _) => {
                match key_type.as_ref() {
                    DataType::Int8 => {
                        let array = as_dictionary_array::<Int8Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::Int16 => {
                        let array = as_dictionary_array::<Int16Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::Int32 => {
                        let array = as_dictionary_array::<Int32Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::Int64 => {
                        let array = as_dictionary_array::<Int64Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::UInt8 => {
                        let array = as_dictionary_array::<UInt8Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::UInt16 => {
                        let array = as_dictionary_array::<UInt16Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::UInt32 => {
                        let array = as_dictionary_array::<UInt32Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::UInt64 => {
                        let array = as_dictionary_array::<UInt64Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    _ => format!("Unsupported dictionary key type {}", key_type),
                }
            }
            t => format!("Unsupported datatype {}", t)
        )
    }
}

#[derive(Debug, Clone)]
struct PlanNode {
    _id: usize,
    name: String,
    label: String,
    metrics: Option<String>,
    children: Vec<PlanNode>,
}

struct TreeBuilder {
    next_id: usize,
    current_path: Vec<PlanNode>,
}

struct DisplayPlan<'a> {
    plan: &'a dyn ExecutionPlan,
}

impl std::fmt::Display for DisplayPlan<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.plan.fmt_as(DisplayFormatType::Default, f)
    }
}

impl ExecutionPlanVisitor for TreeBuilder {
    type Error = std::fmt::Error;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        let name = plan.name().to_string();
        let label = format!("{}", DisplayPlan { plan });

        let metrics = plan.metrics().map(|m| {
            let metrics = m
                .aggregate_by_name()
                .sorted_for_display()
                .timestamps_removed();
            format!("{metrics}")
        });

        let node = PlanNode {
            _id: self.next_id,
            name,
            label,
            metrics,
            children: vec![],
        };

        self.next_id += 1;
        self.current_path.push(node);
        Ok(true)
    }

    fn post_visit(&mut self, _: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        if self.current_path.len() >= 2 {
            let child = self.current_path.pop().unwrap();
            self.current_path.last_mut().unwrap().children.push(child);
        }
        Ok(true)
    }
}

#[component]
fn PlanNode(node: PlanNode) -> impl IntoView {
    view! {
        <div class="relative">
            <div class="flex flex-col items-center">
                <div class="p-4 border rounded-lg bg-white shadow-sm hover:shadow-md transition-shadow">
                    <div class="font-medium">{node.name}</div>
                    <div class="text-sm text-gray-700 mt-1 font-mono">{node.label}</div>
                    {node
                        .metrics
                        .map(|m| {
                            view! { <div class="text-sm text-blue-600 mt-1 italic">{m}</div> }
                        })}
                </div>

                {(!node.children.is_empty())
                    .then(|| {
                        view! {
                            <div class="relative pt-4">
                                <svg
                                    class="absolute top-0 left-1/2 -translate-x-[0.5px] h-4 w-1 z-10"
                                    overflow="visible"
                                >
                                    <line
                                        x1="0.5"
                                        y1="16"
                                        x2="0.5"
                                        y2="0"
                                        stroke="#D1D5DB"
                                        stroke-width="1"
                                        marker-end="url(#global-arrowhead)"
                                    />
                                </svg>

                                <div class="relative flex items-center justify-center">
                                    {(node.children.len() > 1)
                                        .then(|| {
                                            view! {
                                                <svg
                                                    class="absolute top-0 h-[1px]"
                                                    style="left: 25%; width: 50%;"
                                                    overflow="visible"
                                                >
                                                    <line
                                                        x1="0"
                                                        y1="0.5"
                                                        x2="100%"
                                                        y2="0.5"
                                                        stroke="#D1D5DB"
                                                        stroke-width="1"
                                                    />
                                                </svg>
                                            }
                                        })}
                                </div>

                                <div class="flex gap-8">
                                    {node
                                        .children
                                        .into_iter()
                                        .map(|child| view! { <PlanNode node=child /> })
                                        .collect::<Vec<_>>()}
                                </div>
                            </div>
                        }
                    })}
            </div>
        </div>
    }
    .into_any()
}

#[component]
pub fn PhysicalPlan(physical_plan: Arc<dyn ExecutionPlan>) -> impl IntoView {
    let mut builder = TreeBuilder {
        next_id: 0,
        current_path: vec![],
    };
    let displayable_plan = DisplayableExecutionPlan::with_metrics(physical_plan.as_ref());
    accept(physical_plan.as_ref(), &mut builder).unwrap();
    let root = builder.current_path.pop().unwrap();
    logging::log!("{}", displayable_plan.indent(true).to_string());

    view! {
        <div class="relative">
            <svg class="absolute" width="0" height="0">
                <defs>
                    <marker
                        id="global-arrowhead"
                        markerWidth="10"
                        markerHeight="7"
                        refX="9"
                        refY="3.5"
                        orient="auto"
                    >
                        <polygon points="0 0, 10 3.5, 0 7" fill="#D1D5DB" />
                    </marker>
                </defs>
            </svg>

            <div class="p-8 overflow-auto">
                <PlanNode node=root />
            </div>
        </div>
    }
}
