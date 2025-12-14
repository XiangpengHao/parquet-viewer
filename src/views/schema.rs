use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow_schema::Field;
use byte_unit::{Byte, UnitType};
use dioxus::prelude::*;
use parquet::file::metadata::ParquetMetaData;
use wasm_bindgen_futures::spawn_local;

use crate::components::ui::{Panel, SectionHeader};
use crate::utils::{execute_query_inner, format_arrow_type, get_column_chunk_page_info};
use crate::{ParquetResolved, SESSION_CTX};

#[derive(Clone)]
struct SchemaRow {
    arrow_index: usize,
    arrow_name: String,
    arrow_type: String,
    arrow_nullable: String,

    parquet_id: Option<usize>,
    parquet_name: Option<String>,
    parquet_path: Option<String>,
    parquet_type: Option<String>,
    logical_size: Option<u64>,
    encoded_size: Option<u64>,
    compressed_size: Option<u64>,
    compression_ratio: Option<f32>,
    logical_compression_ratio: Option<f32>,
    null_count: Option<u32>,
    encodings: Option<String>,
    compression_summary: Option<String>,
}

fn calculate_arrow_memory_size(metadata: &ParquetMetaData, column_index: usize) -> Option<u64> {
    let total_rows: u64 = metadata
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as u64)
        .sum::<u64>();

    if total_rows == 0 {
        return Some(0);
    }

    let first_col = metadata.row_group(0).column(column_index);
    let physical_type = first_col.column_type();

    let bytes_per_value = match physical_type {
        parquet::basic::Type::BOOLEAN => 1,
        parquet::basic::Type::INT32 => 4,
        parquet::basic::Type::INT64 => 8,
        parquet::basic::Type::INT96 => 12,
        parquet::basic::Type::FLOAT => 4,
        parquet::basic::Type::DOUBLE => 8,
        parquet::basic::Type::BYTE_ARRAY => return None,
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => first_col.column_descr().type_length() as u64,
    };

    let data_size = total_rows * bytes_per_value;
    let validity_bitmap_size = total_rows.div_ceil(8);
    let metadata_overhead = 64;
    Some(data_size + validity_bitmap_size + metadata_overhead)
}

#[derive(Clone)]
struct ParquetColumnDisplay {
    id: usize,
    name: String,
    path: Vec<String>,
    physical_type: String,
    logical_size: Option<u64>,
    encoded_size: u64,
    compressed_size: u64,
    compression_ratio: Option<f32>,
    logical_compression_ratio: Option<f32>,
    null_count: u32,
    encodings: String,
    compression_summary: String,
}

#[derive(Clone)]
struct ArrowFieldNode {
    index: usize,
    field: Field,
    parquet_columns: Vec<usize>,
}

#[derive(Clone, Default)]
struct ColumnAggregate {
    compressed_size: u64,
    encoded_size: u64,
    null_count: u64,
    encodings: HashSet<String>,
    compressions: HashMap<String, u32>,
}

fn format_data_size(size: Option<u64>) -> String {
    match size {
        Some(value) => format!(
            "{:.2}",
            Byte::from_u64(value).get_appropriate_unit(UnitType::Binary)
        ),
        None => "-".to_string(),
    }
}

fn format_ratio(value: Option<f32>) -> String {
    match value {
        Some(ratio) if ratio < 10.0 => format!("{ratio:.2}x"),
        Some(ratio) if ratio < 100.0 => format!("{ratio:.1}x"),
        Some(ratio) => format!("{ratio:.0}x"),
        None => "-".to_string(),
    }
}

async fn calculate_distinct(column_name: &str, registered_table_name: &str) -> u32 {
    let distinct_query =
        format!("SELECT COUNT(DISTINCT \"{column_name}\") from \"{registered_table_name}\"");
    let (results, _) = execute_query_inner(&distinct_query, &SESSION_CTX)
        .await
        .unwrap();
    let first_batch = results.first().unwrap();
    let distinct_value = first_batch.column(0).as_primitive::<Int64Type>().value(0);
    distinct_value as u32
}

async fn calculate_page_encodings(
    parquet_reader: Arc<ParquetResolved>,
    column_id: usize,
) -> String {
    let mut column_reader = parquet_reader.reader().clone();
    let metadata = parquet_reader.metadata().metadata.clone();

    let mut encoding_counts: HashMap<parquet::basic::Encoding, u32> = HashMap::new();
    let mut total_pages = 0u32;

    for (row_group_id, _rg) in metadata.row_groups().iter().enumerate() {
        let pages = match get_column_chunk_page_info(
            &mut column_reader,
            &metadata,
            row_group_id,
            column_id,
        )
        .await
        {
            Ok(pages) => pages,
            Err(_) => continue,
        };

        for page in pages {
            total_pages += 1;
            *encoding_counts.entry(page.encoding).or_insert(0) += 1;
        }
    }

    if total_pages == 0 {
        return "No pages found".to_string();
    }

    let mut sorted_encodings: Vec<_> = encoding_counts.into_iter().collect();
    sorted_encodings.sort_by_key(|(encoding, _)| *encoding);

    sorted_encodings
        .iter()
        .map(|(encoding, count)| {
            format!(
                "{encoding:?} [{:.2}%]",
                (*count as f32 / total_pages as f32) * 100.0
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[component]
pub fn SchemaSection(parquet_reader: Arc<ParquetResolved>) -> Element {
    let parquet_info = parquet_reader.metadata().clone();
    let schema = parquet_info.schema.clone();
    let metadata = parquet_info.metadata.clone();
    let registered_table_name = parquet_reader.registered_table_name().to_string();

    let schema_descriptor = metadata.file_metadata().schema_descr();
    let parquet_column_count = schema_descriptor.columns().len();

    let mut aggregated_column_info = vec![ColumnAggregate::default(); parquet_column_count];
    for rg in metadata.row_groups() {
        for (i, col) in rg.columns().iter().enumerate() {
            aggregated_column_info[i].compressed_size += col.compressed_size() as u64;
            aggregated_column_info[i].encoded_size += col.uncompressed_size() as u64;
            aggregated_column_info[i].null_count += col
                .statistics()
                .and_then(|statistics| statistics.null_count_opt())
                .unwrap_or(0);

            for encoding_it in col.encodings() {
                aggregated_column_info[i]
                    .encodings
                    .insert(format!("{encoding_it:?}"));
            }

            *aggregated_column_info[i]
                .compressions
                .entry(format!("{:?}", col.compression()))
                .or_insert(0) += 1;
        }
    }

    let parquet_columns: Vec<ParquetColumnDisplay> = schema_descriptor
        .columns()
        .iter()
        .enumerate()
        .map(|(i, descriptor)| {
            let path = descriptor.path().parts().to_vec();
            let logical_size = calculate_arrow_memory_size(&metadata, i);
            let aggregate = aggregated_column_info.get(i).cloned().unwrap_or_default();
            let encoded_size = aggregate.encoded_size;
            let compressed_size = aggregate.compressed_size;

            let compression_ratio = if compressed_size > 0 {
                Some(encoded_size as f32 / compressed_size as f32)
            } else {
                None
            };

            let logical_compression_ratio = logical_size.and_then(|size| {
                if compressed_size > 0 {
                    Some(size as f32 / compressed_size as f32)
                } else {
                    None
                }
            });

            let mut encodings: Vec<String> = aggregate.encodings.into_iter().collect();
            encodings.sort();
            let encodings = encodings.join(", ");

            let total: u32 = aggregate.compressions.values().sum();
            let compression_summary = if total == 0 {
                String::new()
            } else {
                aggregate
                    .compressions
                    .into_iter()
                    .map(|(k, v)| format!("{k} [{:.0}%]", v as f32 * 100.0 / total as f32))
                    .collect::<Vec<_>>()
                    .join(", ")
            };

            ParquetColumnDisplay {
                id: i,
                name: descriptor.name().to_string(),
                path,
                physical_type: format!("{:?}", descriptor.physical_type()),
                logical_size,
                encoded_size,
                compressed_size,
                compression_ratio,
                logical_compression_ratio,
                null_count: aggregate.null_count as u32,
                encodings,
                compression_summary,
            }
        })
        .collect();

    let mut columns_by_root: HashMap<String, Vec<usize>> = HashMap::new();
    for column in &parquet_columns {
        if let Some(root) = column.path.first() {
            columns_by_root
                .entry(root.clone())
                .or_default()
                .push(column.id);
        }
    }

    let arrow_field_nodes: Vec<ArrowFieldNode> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| ArrowFieldNode {
            index: idx,
            field: field.as_ref().clone(),
            parquet_columns: columns_by_root
                .get(field.name())
                .cloned()
                .unwrap_or_default(),
        })
        .collect();

    let schema_rows: Vec<SchemaRow> = arrow_field_nodes
        .iter()
        .flat_map(|arrow_node| {
            let arrow_index = arrow_node.index;
            let arrow_name = arrow_node.field.name().to_string();
            let arrow_type = format_arrow_type(arrow_node.field.data_type());
            let arrow_nullable = if arrow_node.field.is_nullable() {
                "Y"
            } else {
                "N"
            }
            .to_string();

            if arrow_node.parquet_columns.is_empty() {
                vec![SchemaRow {
                    arrow_index,
                    arrow_name,
                    arrow_type,
                    arrow_nullable,
                    parquet_id: None,
                    parquet_name: None,
                    parquet_path: None,
                    parquet_type: None,
                    logical_size: None,
                    encoded_size: None,
                    compressed_size: None,
                    compression_ratio: None,
                    logical_compression_ratio: None,
                    null_count: None,
                    encodings: None,
                    compression_summary: None,
                }]
            } else {
                arrow_node
                    .parquet_columns
                    .iter()
                    .map(|&parquet_idx| {
                        let pq_col = &parquet_columns[parquet_idx];
                        SchemaRow {
                            arrow_index,
                            arrow_name: arrow_name.clone(),
                            arrow_type: arrow_type.clone(),
                            arrow_nullable: arrow_nullable.clone(),
                            parquet_id: Some(pq_col.id),
                            parquet_name: Some(pq_col.name.clone()),
                            parquet_path: Some(pq_col.path.join(".")),
                            parquet_type: Some(pq_col.physical_type.clone()),
                            logical_size: pq_col.logical_size,
                            encoded_size: Some(pq_col.encoded_size),
                            compressed_size: Some(pq_col.compressed_size),
                            compression_ratio: pq_col.compression_ratio,
                            logical_compression_ratio: pq_col.logical_compression_ratio,
                            null_count: Some(pq_col.null_count),
                            encodings: Some(pq_col.encodings.clone()),
                            compression_summary: Some(pq_col.compression_summary.clone()),
                        }
                    })
                    .collect()
            }
        })
        .collect();

    let grouped_rows: Vec<(SchemaRow, bool, usize)> = {
        let mut result = Vec::new();
        let mut i = 0;

        while i < schema_rows.len() {
            let current_arrow_index = schema_rows[i].arrow_index;
            let group_start = i;
            while i < schema_rows.len() && schema_rows[i].arrow_index == current_arrow_index {
                i += 1;
            }
            let group_size = i - group_start;

            for (offset, row) in schema_rows[group_start..i].iter().enumerate() {
                let is_first_in_group = offset == 0;
                result.push((row.clone(), is_first_in_group, group_size));
            }
        }
        result
    };

    let distinct_counts = use_signal(|| vec![None::<u32>; arrow_field_nodes.len()]);
    let distinct_loading = use_signal(|| vec![false; arrow_field_nodes.len()]);
    let page_encodings = use_signal(|| vec![None::<String>; parquet_column_count]);
    let page_loading = use_signal(|| vec![false; parquet_column_count]);

    let on_show_distinct: Rc<dyn Fn(usize, String)> = Rc::new({
        let registered_table_name = registered_table_name.clone();
        move |field_index: usize, field_name: String| {
            let registered_table_name = registered_table_name.clone();
            spawn_local(async move {
                let mut distinct_counts = distinct_counts;
                let mut distinct_loading = distinct_loading;
                let mut loading = distinct_loading();
                if loading.get(field_index).copied().unwrap_or(false)
                    || distinct_counts()[field_index].is_some()
                {
                    return;
                }
                loading[field_index] = true;
                distinct_loading.set(loading);

                let cnt = calculate_distinct(&field_name, &registered_table_name).await;

                let mut counts = distinct_counts();
                counts[field_index] = Some(cnt);
                distinct_counts.set(counts);

                let mut loading = distinct_loading();
                loading[field_index] = false;
                distinct_loading.set(loading);
            });
        }
    });

    let on_show_page_encodings: Rc<dyn Fn(usize)> = Rc::new({
        let parquet_reader = parquet_reader.clone();
        move |column_id: usize| {
            let parquet_reader = parquet_reader.clone();
            spawn_local(async move {
                let mut page_encodings = page_encodings;
                let mut page_loading = page_loading;
                let mut loading = page_loading();
                if loading.get(column_id).copied().unwrap_or(false)
                    || page_encodings()[column_id].is_some()
                {
                    return;
                }
                loading[column_id] = true;
                page_loading.set(loading);

                let encodings = calculate_page_encodings(parquet_reader, column_id).await;

                let mut values = page_encodings();
                values[column_id] = Some(encodings);
                page_encodings.set(values);

                let mut loading = page_loading();
                loading[column_id] = false;
                page_loading.set(loading);
            });
        }
    });

    let distinct_counts_now = distinct_counts();
    let distinct_loading_now = distinct_loading();
    let page_encodings_now = page_encodings();
    let page_loading_now = page_loading();

    rsx! {
        Panel { class: Some("rounded-lg p-3 flex-1 overflow-auto space-y-4".to_string()),
            SectionHeader {
                title: "Schema".to_string(),
                subtitle: None,
                class: Some("mb-1".to_string()),
                trailing: None,
            }

            if schema_rows.is_empty() {
                div { class: "text-sm text-gray-500", "No Arrow columns found in this file." }
            } else {
                div { class: "rounded-lg border border-gray-200 bg-white overflow-x-auto",
                    table { class: "min-w-full text-xs",
                        thead { class: "sticky top-0 bg-gray-50 z-10",
                            tr { class: "text-[11px] uppercase tracking-wide text-gray-500 text-left border-b-2 border-gray-300",
                                th { class: "py-2 px-3 font-medium", "Arrow Column" }
                                th { class: "py-2 px-3 font-medium", "Arrow Type" }
                                th { class: "py-2 px-3 font-medium", "Null?" }
                                th { class: "py-2 px-3 font-medium border-r-2", "Distinct" }
                                th { class: "py-2 px-3 font-medium", "Parquet Column" }
                                th { class: "py-2 px-3 font-medium", "Parquet Type" }
                                th { class: "py-2 px-3 font-medium", "Logical (L)*" }
                                th { class: "py-2 px-3 font-medium", "Encoded (E)*" }
                                th { class: "py-2 px-3 font-medium", "Compressed (C)*" }
                                th { class: "py-2 px-3 font-medium", "E/C" }
                                th { class: "py-2 px-3 font-medium", "L/C" }
                                th { class: "py-2 px-3 font-medium", "Nulls" }
                                th { class: "py-2 px-3 font-medium", "Encodings**" }
                                th { class: "py-2 px-3 font-medium", "Page***" }
                                th { class: "py-2 px-3 font-medium", "Compression" }
                            }
                        }
                        tbody {
                            for (row , is_first_in_group , group_size) in grouped_rows.into_iter() {
                                tr { class: "align-top hover:bg-gray-50 border-b border-gray-100",
                                    if is_first_in_group {
                                        td {
                                            class: "py-1.5 px-3",
                                            rowspan: "{group_size}",
                                            div { class: "flex flex-col gap-0.5",
                                                span { class: "font-mono text-[11px] text-gray-500",
                                                    "#{row.arrow_index}"
                                                }
                                                span { class: "font-semibold text-gray-900",
                                                    "{row.arrow_name}"
                                                }
                                            }
                                        }
                                        td {
                                            class: "py-1.5 px-3",
                                            rowspan: "{group_size}",
                                            div { class: "font-mono text-gray-800 break-all",
                                                "{row.arrow_type}"
                                            }
                                        }
                                        td {
                                            class: "py-1.5 px-3",
                                            rowspan: "{group_size}",
                                            span { class: "font-semibold text-gray-700",
                                                "{row.arrow_nullable}"
                                            }
                                        }
                                        td {
                                            class: "py-1.5 px-3 border-r-2",
                                            rowspan: "{group_size}",
                                            match distinct_counts_now[row.arrow_index] {
                                                Some(cnt) => rsx! {
                                                    span { class: "font-mono text-gray-800", "{cnt}" }
                                                },
                                                None => {
                                                    if distinct_loading_now[row.arrow_index] {
                                                        rsx! {
                                                            span { class: "text-gray-400", "..." }
                                                        }
                                                    } else {
                                                        let field_index = row.arrow_index;
                                                        let field_name = row.arrow_name.clone();
                                                        let on_show_distinct = on_show_distinct.clone();
                                                        rsx! {
                                                            button {
                                                                class: "text-blue-500 hover:underline focus:outline-none",
                                                                onclick: move |_| (on_show_distinct)(field_index, field_name.clone()),
                                                                "show"
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    td { class: "py-1.5 px-3",
                                        if let Some(name) = row.parquet_name.as_ref() {
                                            div { class: "flex flex-col gap-0.5",
                                                span { class: "font-mono text-[11px] text-gray-500",
                                                    "#{row.parquet_id.unwrap_or_default()}"
                                                }
                                                span { class: "font-semibold text-gray-900",
                                                    "{name}"
                                                }
                                                if let Some(path) = row.parquet_path.as_ref() {
                                                    span { class: "font-mono text-[10px] text-gray-400 break-all",
                                                        "{path}"
                                                    }
                                                }
                                            }
                                        } else {
                                            span { class: "text-gray-400", "-" }
                                        }
                                    }
                                    td { class: "py-1.5 px-3",
                                        "{row.parquet_type.clone().unwrap_or_else(|| \"-\".to_string())}"
                                    }
                                    td { class: "py-1.5 px-3 font-mono",
                                        "{format_data_size(row.logical_size)}"
                                    }
                                    td { class: "py-1.5 px-3 font-mono",
                                        "{format_data_size(row.encoded_size)}"
                                    }
                                    td { class: "py-1.5 px-3 font-mono",
                                        "{format_data_size(row.compressed_size)}"
                                    }
                                    td { class: "py-1.5 px-3 font-mono",
                                        "{format_ratio(row.compression_ratio)}"
                                    }
                                    td { class: "py-1.5 px-3 font-mono",
                                        "{format_ratio(row.logical_compression_ratio)}"
                                    }
                                    td { class: "py-1.5 px-3 font-mono",
                                        "{row.null_count.map(|v| v.to_string()).unwrap_or_else(|| \"-\".to_string())}"
                                    }
                                    td { class: "py-1.5 px-3",
                                        "{row.encodings.clone().unwrap_or_else(|| \"-\".to_string())}"
                                    }
                                    td { class: "py-1.5 px-3",
                                        if let Some(column_id) = row.parquet_id {
                                            match page_encodings_now[column_id].as_ref() {
                                                Some(enc) => rsx! {
                                                    span { "{enc}" }
                                                },
                                                None => {
                                                    if page_loading_now[column_id] {
                                                        rsx! {
                                                            span { class: "text-gray-400", "..." }
                                                        }
                                                    } else {
                                                        let on_show_page_encodings = on_show_page_encodings.clone();
                                                        rsx! {
                                                            button {
                                                                class: "text-blue-500 hover:underline focus:outline-none",
                                                                onclick: move |_| (on_show_page_encodings)(column_id),
                                                                "show"
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        } else {
                                            span { class: "text-gray-400", "-" }
                                        }
                                    }
                                    td { class: "py-1.5 px-3",
                                        "{row.compression_summary.clone().unwrap_or_else(|| \"-\".to_string())}"
                                    }
                                }
                            }
                        }
                    }
                }
            }

            div { class: "text-xs text-gray-600 space-y-1",
                p {
                    "*: "
                    strong { "Logical size (L)" }
                    ": estimated in-memory size. "
                    strong { "Encoded size (E)" }
                    ": size before compression. "
                    strong { "Compressed size (C)" }
                    ": size after compression."
                }
                p {
                    "**: "
                    strong { "All encodings" }
                    " comes from file metadata (may include repetition/definition level encodings)."
                }
                p {
                    "***: "
                    strong { "Page encodings" }
                    " scan page data and ignore repetition/definition level encodings."
                }
            }

            if !schema.metadata().is_empty() {
                div { class: "mt-2",
                    details {
                        summary { class: "cursor-pointer text-sm font-medium text-gray-700 py-2",
                            "Metadata"
                        }
                        div { class: "pl-4 pt-2 pb-2 border-l-2 border-gray-200 mt-2 text-sm",
                            pre { class: "whitespace-pre-wrap break-words bg-gray-50 p-2 rounded font-mono text-xs overflow-auto max-h-60",
                                {format!("{:#?}", schema.metadata())}
                            }
                        }
                    }
                }
            }
        }
    }
}
