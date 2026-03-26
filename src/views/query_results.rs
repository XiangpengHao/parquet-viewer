use std::sync::Arc;

use arrow::array::AsArray;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_cast::base64::{BASE64_STANDARD, Engine};
use arrow_cast::display::array_value_to_string;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use dioxus::prelude::*;
use futures::StreamExt;
use mimetype_detector::detect;

use crate::components::ui::Panel;
use crate::utils::{export_to_csv_inner, export_to_parquet_inner, format_arrow_type};
use crate::views::plan_visualizer::physical_plan_view;
use crate::{ParquetResolved, SESSION_CTX, utils::execute_query_first_batch_inner};

async fn poll_next_batch(
    mut remaining_stream: Signal<Option<SendableRecordBatchStream>>,
    mut record_batches: Signal<Vec<RecordBatch>>,
) -> Result<bool, String> {
    let Some(mut stream) = remaining_stream.with_mut(|stream| stream.take()) else {
        return Ok(false);
    };

    match stream.next().await {
        Some(Ok(batch)) => {
            record_batches.with_mut(|batches| batches.push(batch));
            remaining_stream.set(Some(stream));
            Ok(true)
        }
        Some(Err(e)) => Err(e.to_string()),
        None => {
            remaining_stream.set(None);
            Ok(false)
        }
    }
}

async fn drain_remaining_batches(
    remaining_stream: Signal<Option<SendableRecordBatchStream>>,
    record_batches: Signal<Vec<RecordBatch>>,
) -> Result<(), String> {
    while poll_next_batch(remaining_stream, record_batches).await? {}
    Ok(())
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
    let loading_next_batch = use_signal(|| false);
    let mut initialized = use_signal(|| false);

    let progress = use_signal(|| "Generating SQL...".to_string());
    let generated_sql = use_signal(|| None::<String>);
    let execution_error = use_signal(|| None::<String>);
    let physical_plan = use_signal(|| None::<Arc<dyn ExecutionPlan>>);
    let record_batches = use_signal(Vec::<RecordBatch>::new);
    let remaining_stream = use_signal(|| None::<SendableRecordBatchStream>);

    let mut decode_images = use_signal(|| false);
    let mut expanded_image_url = use_signal(|| None::<Arc<str>>);

    if !initialized() {
        initialized.set(true);
        let query = query.clone();
        let parquet_table = parquet_table.clone();
        let mut progress = progress;
        let mut generated_sql = generated_sql;
        let mut execution_error = execution_error;
        let mut physical_plan = physical_plan;
        let mut record_batches = record_batches;
        let mut remaining_stream = remaining_stream;

        spawn(async move {
            let sql = match crate::nl_to_sql::user_input_to_sql(&query, &parquet_table)
                .await
                .map_err(|e| e.to_string())
            {
                Ok(sql) => sql,
                Err(e) => {
                    execution_error.set(Some(format!("Error generating SQL: {e}")));
                    return;
                }
            };

            generated_sql.set(Some(sql.clone()));
            progress.set(format!("Executing SQL...\n\n{sql}"));

            match execute_query_first_batch_inner(&sql, &SESSION_CTX).await {
                Ok((first_batches, stream, plan)) => {
                    physical_plan.set(Some(plan));
                    record_batches.set(first_batches);
                    remaining_stream.set(stream);
                }
                Err(e) => execution_error.set(Some(format!("Error executing query: {e}"))),
            }
        });
    }

    let query_display = query.clone();
    let sql_for_display = generated_sql();
    let maybe_error = execution_error();
    let plan_for_render = physical_plan();
    let batches = record_batches();
    let has_more_batches = remaining_stream.read().is_some();

    rsx! {
        Panel { class: Some("p-3".to_string()),
            div { class: "flex flex-col gap-2 mb-3",
                div { class: "flex items-start justify-between gap-4",
                    div {
                        div { class: "font-semibold break-words", "{query_display}" }
                        if let Some(sql) = sql_for_display.clone() {
                            pre { class: "mt-2 text-xs bg-base-200 border border-base-300 rounded p-2 overflow-auto max-h-48",
                                "{sql}"
                            }
                        }
                    }
                    div { class: "flex items-center gap-2",
                        button {
                            class: "btn btn-xs btn-ghost",
                            title: "Export to CSV",
                            onclick: move |_| {
                                if physical_plan().is_none() {
                                    return;
                                }

                                let mut execution_error = execution_error;
                                let remaining_stream = remaining_stream;
                                let record_batches = record_batches;
                                spawn(async move {
                                    execution_error.set(None);
                                    if let Err(e) =
                                        drain_remaining_batches(remaining_stream, record_batches).await
                                    {
                                        execution_error.set(Some(format!("Error exporting CSV: {e}")));
                                        return;
                                    }
                                    let batches = record_batches();
                                    export_to_csv_inner(&batches);
                                });
                            },
                            "CSV"
                        }
                        button {
                            class: "btn btn-xs btn-ghost",
                            title: "Export to Parquet",
                            onclick: move |_| {
                                if physical_plan().is_none() {
                                    return;
                                }

                                let mut execution_error = execution_error;
                                let remaining_stream = remaining_stream;
                                let record_batches = record_batches;
                                spawn(async move {
                                    execution_error.set(None);
                                    if let Err(e) =
                                        drain_remaining_batches(remaining_stream, record_batches).await
                                    {
                                        execution_error
                                            .set(Some(format!("Error exporting Parquet: {e}")));
                                        return;
                                    }

                                    let batches = record_batches();
                                    if batches.is_empty() {
                                        execution_error.set(Some(
                                            "Cannot export Parquet: query returned no rows".to_string(),
                                        ));
                                        return;
                                    }
                                    export_to_parquet_inner(&batches);
                                });
                            },
                            "Parquet"
                        }
                        button {
                            class: "btn btn-xs btn-ghost",
                            title: "Copy SQL",
                            onclick: move |_| {
                                if let Some(sql) = generated_sql()
                                    && let Some(window) = web_sys::window()
                                {
                                    let clipboard = window.navigator().clipboard();
                                    let _ = clipboard.write_text(&sql);
                                }
                            },
                            "Copy"
                        }
                        button {
                            class: "btn btn-xs btn-ghost",
                            title: "Execution plan",
                            onclick: move |_| {
                                let mut show_plan = show_plan;
                                show_plan.set(!show_plan());
                            },
                            "Plan"
                        }
                        button {
                            class: "btn btn-xs btn-ghost hover:text-error",
                            title: "Hide",
                            onclick: move |_| on_hide.call(id),
                            "Hide"
                        }
                        button {
                            class: if decode_images() { "btn btn-xs btn-primary" } else { "btn btn-xs btn-ghost" },
                            title: "Decode bytes as images",
                            onclick: move |_| decode_images.set(!decode_images()),
                            "Decode bytes as images"
                        }
                    }
                }
            }

            if let Some(err) = maybe_error {
                div { class: "alert alert-error text-xs",
                    pre { class: "whitespace-pre-wrap", "{err}" }
                }
            } else if plan_for_render.is_none() {
                pre { class: "text-base-content opacity-75 text-xs whitespace-pre-wrap", "{progress()}" }
            } else {
                if show_plan()
                    && let Some(plan) = plan_for_render.clone()
                {
                    div { class: "mb-4", {physical_plan_view(plan)} }
                }

                if let Some(url) = expanded_image_url() {
                    div {
                        class: "modal modal-open",
                        onclick: move |_| expanded_image_url.set(None),
                        div {
                            class: "modal-box w-fit max-w-[80vw] max-h-[80vh] overflow-auto",
                            onclick: move |ev| ev.stop_propagation(),
                            img { src: "{url}" }
                        }
                    }
                }

                if batches.is_empty() {
                    div { class: "text-xs text-base-content opacity-75",
                        "Query executed successfully, no rows returned."
                    }
                } else {
                    {
                        let merged_record_batch = concat_batches(
                            &batches[0].schema(),
                            batches.iter().collect::<Vec<_>>(),
                        )
                        .expect("Failed to merge record batches");
                        let schema = merged_record_batch.schema();
                        let total_rows = merged_record_batch.num_rows();
                        let show_rows = visible_rows().min(total_rows);
                        let decode_images = decode_images();
                        rsx! {
                            div { class: "max-h-[32rem] overflow-auto overflow-x-auto relative",
                                table { class: "table table-zebra table-pin-rows table-xs",
                                    thead {
                                        tr {
                                            for field in schema.fields().iter() {
                                                th { class: "px-1 py-1 text-left min-w-[200px] leading-tight",
                                                    div { class: "truncate", title: "{field.name()}", "{field.name()}" }
                                                    div {
                                                        class: "text-xs opacity-60 truncate",
                                                        title: "{format_arrow_type(field.data_type())}",
                                                        "{format_arrow_type(field.data_type())}"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    tbody {
                                        for row_idx in 0..show_rows {
                                            tr { class: "hover",
                                                for col_idx in 0..merged_record_batch.num_columns() {
                                                    {
                                                        let column = merged_record_batch.column(col_idx);
                                                        let cell_value = array_value_to_string(column.as_ref(), row_idx)
                                                            .unwrap_or_else(|_| "NULL".to_string());
                                                        let preview = cell_value.chars().take(200).collect::<String>();

                                                        let image_data_url: Option<String> = if decode_images {
                                                            let column_value: Option<&[u8]> = if column.is_null(row_idx){
                                                                None
                                                            } else {
                                                                match column.data_type() {
                                                                    DataType::BinaryView => Some(column.as_binary_view().value(row_idx)),
                                                                    DataType::Binary => Some(column.as_binary::<i32>().value(row_idx)),
                                                                    DataType::LargeBinary => Some(column.as_binary::<i64>().value(row_idx)),
                                                                    _ => None,
                                                                }
                                                            };

                                                            column_value.and_then(|bytes| {
                                                                let mime = detect(bytes);
                                                                if !mime.kind().is_image() {
                                                                    return None;
                                                                }

                                                                let b64_string = BASE64_STANDARD.encode(bytes);
                                                                Some(format!("data:{};base64,{}", mime.mime(), b64_string))
                                                            })
                                                        } else {
                                                            None
                                                        };
                                                        rsx! {
                                                            td { class: "px-1 py-1 leading-tight break-words",
                                                                if let Some(url) = &image_data_url {
                                                                    img {
                                                                        class: "max-h-24 max-w-xs object-contain cursor-pointer hover:opacity-80 transition-opacity",
                                                                        src: "{url}",
                                                                        onclick: {
                                                                            let url = Arc::from(url.as_str());
                                                                            move |_| expanded_image_url.set(Some(Arc::clone(&url)))
                                                                        },
                                                                    }
                                                                } else if cell_value.len() > 200 {
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
                            if show_rows < total_rows || has_more_batches {
                                div { class: "mt-2 flex justify-center",
                                    button {
                                        class: "btn btn-sm btn-outline",
                                        disabled: loading_next_batch(),
                                        onclick: move |_| {
                                            let mut visible_rows = visible_rows;
                                            if show_rows < total_rows {
                                                visible_rows.set(visible_rows() + 20);
                                                return;
                                            }

                                            if loading_next_batch() {
                                                return;
                                            }

                                            let mut loading_next_batch = loading_next_batch;
                                            let mut execution_error = execution_error;
                                            let remaining_stream = remaining_stream;
                                            let record_batches = record_batches;
                                            loading_next_batch.set(true);
                                            spawn(async move {
                                                execution_error.set(None);
                                                match poll_next_batch(remaining_stream, record_batches).await {
                                                    Ok(true) => {
                                                        visible_rows.set(visible_rows() + 20);
                                                    }
                                                    Ok(false) => {}
                                                    Err(e) => execution_error
                                                        .set(Some(format!("Error loading next batch: {e}"))),
                                                }
                                                loading_next_batch.set(false);
                                            });
                                        },
                                        if loading_next_batch() {
                                            "Loading next batch..."
                                        } else if show_rows < total_rows {
                                            "Load more"
                                        } else {
                                            "Load next batch"
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn test_batches_can_be_merged() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batches = vec![
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap(),
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![3, 4]))])
                .unwrap(),
        ];

        let merged = concat_batches(&batches[0].schema(), batches.iter().collect::<Vec<_>>());
        assert!(merged.is_ok());
        assert_eq!(merged.unwrap().num_rows(), 4);
    }
}
