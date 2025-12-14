use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_cast::display::array_value_to_string;
use datafusion::physical_plan::ExecutionPlan;
use dioxus::prelude::*;

use crate::components::ui::Panel;
use crate::utils::{export_to_csv_inner, export_to_parquet_inner, format_arrow_type};
use crate::views::plan_visualizer::physical_plan_view;
use crate::{ParquetResolved, SESSION_CTX, utils::execute_query_inner};

#[derive(Clone, Debug)]
pub(crate) struct ExecutionResult {
    pub(crate) record_batches: Arc<Vec<RecordBatch>>,
    pub(crate) physical_plan: Arc<dyn ExecutionPlan>,
}

#[derive(Clone, Debug)]
struct QueryExecutionState {
    progress: String,
    generated_sql: Option<String>,
    execution_result: Option<Result<ExecutionResult, String>>,
}

#[component]
pub fn QueryResultView(
    id: usize,
    query: String,
    parquet_table: Arc<ParquetResolved>,
    on_hide: EventHandler<usize>,
) -> Element {
    let show_plan = use_signal(|| false);
    let visible_rows = use_signal(|| 20usize);

    // Clone the query string for display purposes
    let query_display = query.clone();

    // Use use_resource for reactive async data fetching
    let query_execution = use_resource(move || {
        let query = query.clone();
        let parquet_table = parquet_table.clone();
        async move {
            let sql = match crate::nl_to_sql::user_input_to_sql(&query, &parquet_table)
                .await
                .map_err(|e| e.to_string())
            {
                Ok(sql) => sql,
                Err(e) => {
                    return QueryExecutionState {
                        progress: "Error generating SQL".to_string(),
                        generated_sql: None,
                        execution_result: Some(Err(format!("Error generating SQL: {e}"))),
                    };
                }
            };

            let progress_with_sql = format!("Executing SQL...\n\n{sql}");

            let result = execute_query_inner(&sql, &SESSION_CTX)
                .await
                .map_err(|e| e.to_string())
                .map(|(results, plan)| ExecutionResult {
                    record_batches: Arc::new(results),
                    physical_plan: plan,
                });

            QueryExecutionState {
                progress: progress_with_sql,
                generated_sql: Some(sql),
                execution_result: Some(result),
            }
        }
    });

    let state = query_execution.read();
    let (progress, generated_sql, execution_result) = match state.as_ref() {
        Some(s) => (
            s.progress.as_str(),
            s.generated_sql.as_ref(),
            s.execution_result.as_ref(),
        ),
        None => ("Generating SQL...", None, None),
    };

    rsx! {
        Panel { class: Some("p-3".to_string()),
            div { class: "flex flex-col gap-2 mb-3",
                div { class: "flex items-start justify-between gap-4",
                    div {
                        div { class: "font-semibold text-gray-900 break-words", "{query_display}" }
                        if let Some(sql) = generated_sql {
                            pre { class: "mt-2 text-xs bg-gray-50 border border-gray-200 rounded p-2 overflow-auto max-h-48",
                                "{sql}"
                            }
                        }
                    }
                    div { class: "flex items-center gap-2",
                        button {
                            class: "p-1 text-gray-500 hover:text-gray-700",
                            title: "Export to CSV",
                            onclick: move |_| {
                                if let Some(state) = query_execution.read().as_ref() {
                                    if let Some(Ok(res)) = state.execution_result.as_ref() {
                                        export_to_csv_inner(res.record_batches.as_ref());
                                    }
                                }
                            },
                            "CSV"
                        }
                        button {
                            class: "p-1 text-gray-500 hover:text-gray-700",
                            title: "Export to Parquet",
                            onclick: move |_| {
                                if let Some(state) = query_execution.read().as_ref() {
                                    if let Some(Ok(res)) = state.execution_result.as_ref() {
                                        export_to_parquet_inner(res.record_batches.as_ref());
                                    }
                                }
                            },
                            "Parquet"
                        }
                        button {
                            class: "p-1 text-gray-500 hover:text-gray-700",
                            title: "Copy SQL",
                            onclick: move |_| {
                                if let Some(state) = query_execution.read().as_ref() {
                                    if let Some(sql) = &state.generated_sql {
                                        if let Some(window) = web_sys::window() {
                                            let clipboard = window.navigator().clipboard();
                                            let _ = clipboard.write_text(sql);
                                        }
                                    }
                                }
                            },
                            "Copy"
                        }
                        button {
                            class: "p-1 text-gray-500 hover:text-gray-700",
                            title: "Execution plan",
                            onclick: move |_| {
                                let mut show_plan = show_plan;
                                show_plan.set(!show_plan());
                            },
                            "Plan"
                        }
                        button {
                            class: "p-1 text-gray-500 hover:text-red-600",
                            title: "Hide",
                            onclick: move |_| on_hide.call(id),
                            "Hide"
                        }
                    }
                }
            }

            {

                match execution_result {
                    None => rsx! {
                        pre { class: "text-gray-600 text-xs whitespace-pre-wrap", "{progress}" }
                    },
                    Some(Err(e)) => rsx! {
                        pre { class: "text-red-700 text-xs whitespace-pre-wrap", "{e}" }
                    },
                    Some(Ok(result)) => {
                        let merged_record_batch = concat_batches(
                                &result.record_batches[0].schema(),
                                result.record_batches.iter().collect::<Vec<_>>(),
                            )
                            .expect("Failed to merge record batches");
                        let schema = merged_record_batch.schema();
                        let total_rows = merged_record_batch.num_rows();
                        let show_rows = visible_rows().min(total_rows);
                        rsx! {
                            if show_plan() {
                                div { class: "mb-4", {physical_plan_view(result.physical_plan.clone())} }
                            }


                            div { class: "max-h-[32rem] overflow-auto overflow-x-auto relative",
                                table { class: "min-w-full bg-white table-fixed",
                                    thead { class: "sticky top-0 z-10 bg-gray-50",
                                        tr {
                                            for field in schema.fields().iter() {
                                                th { class: "px-1 py-1 text-left min-w-[200px] leading-tight",
                                                    div { class: "truncate", title: "{field.name()}", "{field.name()}" }
                                                    div {
                                                        class: "text-xs text-gray-400 truncate",
                                                        title: "{format_arrow_type(field.data_type())}",
                                                        "{format_arrow_type(field.data_type())}"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    tbody {
                                        for row_idx in 0..show_rows {
                                            tr { class: "hover:bg-gray-50",
                                                for col_idx in 0..merged_record_batch.num_columns() {
                                                    {
                                                        let column = merged_record_batch.column(col_idx);
                                                        let cell_value = array_value_to_string(column.as_ref(), row_idx)
                                                            .unwrap_or_else(|_| "NULL".to_string());
                                                        let preview = cell_value.chars().take(200).collect::<String>();
                                                        rsx! {
                                                            td { class: "px-1 py-1 leading-tight text-gray-700 break-words",
                                                                if cell_value.len() > 200 {
                                                                    details {
                                                                        summary { class: "cursor-pointer select-none", "{preview}..." }
                                                                        pre { class: "whitespace-pre-wrap", "{cell_value}" }
                                                                    }
                                                                } else {
                                                                    "{cell_value}"
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if show_rows < total_rows {
                                div { class: "mt-2 flex justify-center",
                                    button {
                                        class: "px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 text-xs",
                                        onclick: move |_| {
                                            let mut visible_rows = visible_rows;
                                            visible_rows.set(visible_rows() + 20);
                                        },
                                        "Load more"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn test_query_result_view_renders() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let plan = PlaceholderRowExec::new(schema.clone());
        let result = ExecutionResult {
            record_batches: Arc::new(vec![
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                    .unwrap(),
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![3, 4]))])
                    .unwrap(),
            ]),
            physical_plan: Arc::new(plan),
        };

        // Rendering the table component should not panic.
        let _ = concat_batches(
            &result.record_batches[0].schema(),
            result.record_batches.iter().collect::<Vec<_>>(),
        )
        .unwrap();
    }
}
