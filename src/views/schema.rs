use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use byte_unit::{Byte, UnitType};
use dioxus::prelude::*;
use parquet::file::metadata::ParquetMetaData;

use crate::components::ui::{Panel, SectionHeader};
use crate::utils::{execute_query_inner, format_arrow_type, get_column_chunk_page_info};
use crate::{ParquetResolved, SESSION_CTX};

#[derive(Clone)]
struct SchemaRow {
    arrow_index: usize,
    arrow_name: String,
    arrow_type: String,
    arrow_nullable: String,
    parquet_columns: Vec<ParquetColumnDisplay>,
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

async fn calculate_distinct(column_name: &str, registered_table_name: &str) -> Result<u32> {
    let distinct_query =
        format!("SELECT COUNT(DISTINCT \"{column_name}\") from \"{registered_table_name}\"");
    let (results, _) = execute_query_inner(&distinct_query, &SESSION_CTX).await?;
    let first_batch = results
        .first()
        .ok_or_else(|| anyhow!("No record batch returned for distinct count"))?;
    if first_batch.num_rows() == 0 {
        return Ok(0);
    }
    let distinct_value = first_batch.column(0).as_primitive::<Int64Type>().value(0);
    Ok(distinct_value as u32)
}

async fn calculate_page_encodings(
    parquet_reader: Arc<ParquetResolved>,
    column_id: usize,
) -> Result<String> {
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
        return Ok("No pages found".to_string());
    }

    let mut sorted_encodings: Vec<_> = encoding_counts.into_iter().collect();
    sorted_encodings.sort_by_key(|(encoding, _)| *encoding);

    Ok(sorted_encodings
        .iter()
        .map(|(encoding, count)| {
            format!(
                "{encoding:?} [{:.2}%]",
                (*count as f32 / total_pages as f32) * 100.0
            )
        })
        .collect::<Vec<_>>()
        .join(", "))
}

#[component]
fn DistinctCell(field_name: String, registered_table_name: String) -> Element {
    let mut action = use_action(move || {
        let field_name = field_name.clone();
        let registered_table_name = registered_table_name.clone();
        async move { calculate_distinct(&field_name, &registered_table_name).await }
    });

    if action.pending() {
        return rsx! {
            span { class: "opacity-50", "..." }
        };
    }

    match action.value() {
        Some(Ok(cnt)) => rsx! {
            span { class: "font-mono text-base-content", "{cnt.read()}" }
        },
        Some(Err(_e)) => rsx! {
            button {
                class: "text-red-500 hover:underline focus:outline-none",
                onclick: move |_| {
                    action.call();
                },
                "retry"
            }
        },
        None => rsx! {
            button {
                class: "link link-primary",
                onclick: move |_| {
                    action.call();
                },
                "show"
            }
        },
    }
}

#[component]
fn PageEncodingsCell(parquet_reader: Arc<ParquetResolved>, column_id: usize) -> Element {
    let mut action = use_action(move || {
        let parquet_reader = parquet_reader.clone();
        async move { calculate_page_encodings(parquet_reader, column_id).await }
    });

    if action.pending() {
        return rsx! {
            span { class: "opacity-50", "..." }
        };
    }

    match action.value() {
        Some(Ok(enc)) => rsx! {
            span { "{enc.read()}" }
        },
        Some(Err(_e)) => rsx! {
            button {
                class: "text-red-500 hover:underline focus:outline-none",
                onclick: move |_| {
                    action.call();
                },
                "retry"
            }
        },
        None => rsx! {
            button {
                class: "link link-primary",
                onclick: move |_| {
                    action.call();
                },
                "show"
            }
        },
    }
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

    let schema_rows: Vec<SchemaRow> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(arrow_index, field)| {
            let field = field.as_ref();
            let parquet_columns_for_field: Vec<ParquetColumnDisplay> = columns_by_root
                .get(field.name())
                .into_iter()
                .flatten()
                .filter_map(|&parquet_idx| parquet_columns.get(parquet_idx).cloned())
                .collect();

            SchemaRow {
                arrow_index,
                arrow_name: field.name().to_string(),
                arrow_type: format_arrow_type(field.data_type()),
                arrow_nullable: if field.is_nullable() {
                    "Y".to_string()
                } else {
                    "N".to_string()
                },
                parquet_columns: parquet_columns_for_field,
            }
        })
        .collect();

    rsx! {
        Panel { class: Some("rounded-lg p-3 flex-1 overflow-auto space-y-4".to_string()),
            SectionHeader {
                title: "Schema".to_string(),
                subtitle: None,
                class: Some("mb-1".to_string()),
                trailing: None,
            }
            div { class: "rounded-lg border border-base-300 bg-base-100 overflow-x-auto",
                table { class: "min-w-full text-xs",
                    thead { class: "sticky top-0 bg-base-200 z-10",
                        tr { class: "text-[11px] uppercase tracking-wide opacity-60 text-left border-b-2 border-base-300",
                            th { class: "py-2 px-3 font-medium", "Arrow Column" }
                            th { class: "py-2 px-3 font-medium", "Arrow Type" }
                            th { class: "py-2 px-3 font-medium", "Null?" }
                            th { class: "py-2 px-3 font-medium border-r-2 border-base-300",
                                "Distinct"
                            }
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
                        for row in schema_rows.iter() {
                            {
                                let group_size = row.parquet_columns.len().max(1);
                                if row.parquet_columns.is_empty() {
                                    rsx! {
                                        tr {
                                            key: "{row.arrow_index}-none",
                                            class: "align-top hover:bg-base-200 border-b border-base-200",

            

            
                                            td { class: "py-1.5 px-3", rowspan: "{group_size}",
            

                                                div { class: "flex flex-col gap-0.5",
                                                    span { class: "font-mono text-[11px] opacity-60", "#{row.arrow_index}" }
                                                    span { class: "font-semibold font-semibold", "{row.arrow_name}" }
                                                }
                                            }
                                            td { class: "py-1.5 px-3", rowspan: "{group_size}",
                                                div { class: "font-mono text-base-content break-all", "{row.arrow_type}" }
                                            }
                                            td { class: "py-1.5 px-3", rowspan: "{group_size}",
                                                span { class: "font-semibold opacity-75", "{row.arrow_nullable}" }
                                            }
                                            td {
                                                class: "py-1.5 px-3 border-r-2 border-base-300",
                                                rowspan: "{group_size}",
                                                DistinctCell {
                                                    field_name: row.arrow_name.clone(),
                                                    registered_table_name: registered_table_name.clone(),
                                                }
                                            }

            
            
                                            td { class: "py-1.5 px-3",
            

                                                span { class: "opacity-50", "-" }
                                            }
                                            td { class: "py-1.5 px-3", "-" }
                                            td { class: "py-1.5 px-3 font-mono", "-" }
                                            td { class: "py-1.5 px-3 font-mono", "-" }
                                            td { class: "py-1.5 px-3 font-mono", "-" }
                                            td { class: "py-1.5 px-3 font-mono", "-" }
                                            td { class: "py-1.5 px-3 font-mono", "-" }
                                            td { class: "py-1.5 px-3 font-mono", "-" }
                                            td { class: "py-1.5 px-3", "-" }
                                            td { class: "py-1.5 px-3",
                                                span { class: "opacity-50", "-" }
                                            }
                                            td { class: "py-1.5 px-3", "-" }
                                        }
                                    }
                                } else {
                                    let first_pq_col = &row.parquet_columns[0];
                                    rsx! {
                                        tr {
                                            key: "{row.arrow_index}-{first_pq_col.id}",
                                            class: "align-top hover:bg-base-200 border-b border-base-200",
                                            td { class: "py-1.5 px-3", rowspan: "{group_size}",
                                                div { class: "flex flex-col gap-0.5",
                                                    span { class: "font-mono text-[11px] opacity-60", "#{row.arrow_index}" }
                                                    span { class: "font-semibold font-semibold", "{row.arrow_name}" }
                                                }
                                            }
                                            td { class: "py-1.5 px-3", rowspan: "{group_size}",
                                                div { class: "font-mono text-base-content break-all", "{row.arrow_type}" }
                                            }
                                            td { class: "py-1.5 px-3", rowspan: "{group_size}",
                                                span { class: "font-semibold opacity-75", "{row.arrow_nullable}" }
                                            }
                                            td {
                                                class: "py-1.5 px-3 border-r-2 border-base-300",
                                                rowspan: "{group_size}",
                                                DistinctCell {
                                                    field_name: row.arrow_name.clone(),
                                                    registered_table_name: registered_table_name.clone(),
                                                }
                                            }
            
                                            td { class: "py-1.5 px-3",
                                                div { class: "flex flex-col gap-0.5",
                                                    span { class: "font-mono text-[11px] opacity-60", "#{first_pq_col.id}" }
                                                    span { class: "font-semibold font-semibold", "{first_pq_col.name}" }
                                                    span { class: "font-mono text-[10px] opacity-50 break-all",
                                                        "{first_pq_col.path.join(\".\")}"
                                                    }
                                                }
                                            }
                                            td { class: "py-1.5 px-3", "{first_pq_col.physical_type}" }
                                            td { class: "py-1.5 px-3 font-mono", "{format_data_size(first_pq_col.logical_size)}" }
                                            td { class: "py-1.5 px-3 font-mono", "{format_data_size(Some(first_pq_col.encoded_size))}" }
                                            td { class: "py-1.5 px-3 font-mono", "{format_data_size(Some(first_pq_col.compressed_size))}" }
                                            td { class: "py-1.5 px-3 font-mono", "{format_ratio(first_pq_col.compression_ratio)}" }
                                            td { class: "py-1.5 px-3 font-mono", "{format_ratio(first_pq_col.logical_compression_ratio)}" }
                                            td { class: "py-1.5 px-3 font-mono", "{first_pq_col.null_count}" }
                                            td { class: "py-1.5 px-3", "{first_pq_col.encodings}" }
                                            td { class: "py-1.5 px-3",
                                                PageEncodingsCell {
                                                    parquet_reader: parquet_reader.clone(),
                                                    column_id: first_pq_col.id,
                                                }
                                            }
                                            td { class: "py-1.5 px-3", "{first_pq_col.compression_summary}" }
                                        }
            
                                        for pq_col in row.parquet_columns.iter().skip(1) {
                                            tr {
                                                key: "{row.arrow_index}-{pq_col.id}",
                                                class: "align-top hover:bg-base-200 border-b border-base-200",
                                                td { class: "py-1.5 px-3",
                                                    div { class: "flex flex-col gap-0.5",
                                                        span { class: "font-mono text-[11px] opacity-60", "#{pq_col.id}" }
                                                        span { class: "font-semibold font-semibold", "{pq_col.name}" }
                                                        span { class: "font-mono text-[10px] opacity-50 break-all", "{pq_col.path.join(\".\")}" }
                                                    }
                                                }
                                                td { class: "py-1.5 px-3", "{pq_col.physical_type}" }
                                                td { class: "py-1.5 px-3 font-mono", "{format_data_size(pq_col.logical_size)}" }
                                                td { class: "py-1.5 px-3 font-mono", "{format_data_size(Some(pq_col.encoded_size))}" }
                                                td { class: "py-1.5 px-3 font-mono", "{format_data_size(Some(pq_col.compressed_size))}" }
                                                td { class: "py-1.5 px-3 font-mono", "{format_ratio(pq_col.compression_ratio)}" }
                                                td { class: "py-1.5 px-3 font-mono", "{format_ratio(pq_col.logical_compression_ratio)}" }
                                                td { class: "py-1.5 px-3 font-mono", "{pq_col.null_count}" }
                                                td { class: "py-1.5 px-3", "{pq_col.encodings}" }
                                                td { class: "py-1.5 px-3",
                                                    PageEncodingsCell { parquet_reader: parquet_reader.clone(), column_id: pq_col.id }
                                                }
                                                td { class: "py-1.5 px-3", "{pq_col.compression_summary}" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "text-xs opacity-75 space-y-1",
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
                        summary { class: "cursor-pointer text-sm font-medium opacity-75 py-2",
                            "Metadata"
                        }
                        div { class: "pl-4 pt-2 pb-2 border-l-2 border-base-300 mt-2 text-sm",
                            pre { class: "whitespace-pre-wrap break-words bg-base-200 p-2 rounded font-mono text-xs overflow-auto max-h-60",
                                {format!("{:#?}", schema.metadata())}
                            }
                        }
                    }
                }
            }
        }
    }
}
