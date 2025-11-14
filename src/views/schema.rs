use crate::components::ui::{Panel, SectionHeader};
use crate::utils::{execute_query_inner, get_column_chunk_page_info};
use crate::{ParquetResolved, SESSION_CTX, utils::format_arrow_type};
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow_schema::Field;
use byte_unit::{Byte, UnitType};
use leptos::prelude::*;
use parquet::file::metadata::ParquetMetaData;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Flattened row structure combining Arrow and Parquet data
#[derive(Clone)]
struct SchemaRow {
    // Arrow info
    arrow_index: usize,
    arrow_name: String,
    arrow_type: String,
    arrow_nullable: String,

    // Parquet info (Option because Arrow fields may not have Parquet columns)
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

/// Estimate Arrow in-memory size for a column based on its Parquet physical type
/// Returns None for variable-length data types that cannot be reliably estimated
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
        parquet::basic::Type::BYTE_ARRAY => {
            // Variable-length data - cannot estimate reliably
            return None;
        }
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => first_col.column_descr().type_length() as u64,
    };

    // Estimate Arrow memory: data + validity bitmap + metadata overhead
    let data_size = total_rows * bytes_per_value;
    let validity_bitmap_size = total_rows.div_ceil(8); // Round up to nearest byte
    let metadata_overhead = 64; // Rough estimate for array metadata
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

#[component]
pub fn SchemaSection(parquet_reader: Arc<ParquetResolved>) -> impl IntoView {
    let parquet_info = parquet_reader.metadata().clone();
    let schema = parquet_info.schema.clone();
    let metadata = parquet_info.metadata.clone();

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
                .unwrap_or(0) as u64;

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
                    .collect::<Vec<String>>()
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

    // Build flattened schema rows for easy sorting
    let schema_rows: Arc<Vec<SchemaRow>> = Arc::new(arrow_field_nodes
        .iter()
        .flat_map(|arrow_node| {
            let arrow_index = arrow_node.index;
            let arrow_name = arrow_node.field.name().to_string();
            let arrow_type = format_arrow_type(arrow_node.field.data_type());
            let arrow_nullable = if arrow_node.field.is_nullable() { "Y" } else { "N" }.to_string();

            if arrow_node.parquet_columns.is_empty() {
                // Arrow field without parquet columns
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
                // Create one row per parquet column
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
        .collect());

    let (col_page_encodings, set_col_page_encodings) = signal(
        (0..parquet_column_count)
            .map(|_| None)
            .collect::<Vec<Option<LocalResource<String>>>>(),
    );
    let (col_distinct_count, set_col_distinct_count) = signal(
        (0..arrow_field_nodes.len())
            .map(|_| None)
            .collect::<Vec<Option<LocalResource<u32>>>>(),
    );

    let table_name = parquet_reader.table_name().to_string();

    view! {
        <Panel class="rounded-lg p-3 flex-1 overflow-auto space-y-4">
            <SectionHeader title="Schema" class="mb-1" />
            {schema_rows.is_empty()
                .then(|| {
                    view! {
                        <div class="text-sm text-gray-500">
                            "No Arrow columns found in this file."
                        </div>
                    }
                })}
            {(!(**schema_rows).is_empty()).then(|| {
                view! {
                    <div class="rounded-lg border border-gray-200 bg-white overflow-x-auto">
                        <table class="min-w-full text-xs">
                            <thead class="sticky top-0 bg-gray-50 z-10">
                                <tr class="text-[11px] uppercase tracking-wide text-gray-500 text-left border-b-2 border-gray-300">
                                    <th class="py-2 px-3 font-medium">"Arrow Column"</th>
                                    <th class="py-2 px-3 font-medium">"Arrow Type"</th>
                                    <th class="py-2 px-3 font-medium">"Null?"</th>
                                    <th class="py-2 px-3 font-medium border-r-2">"Distinct"</th>
                                    <th class="py-2 px-3 font-medium">"Parquet Column"</th>
                                    <th class="py-2 px-3 font-medium">"Parquet Type"</th>
                                    <th class="py-2 px-3 font-medium">"Logical (L)*"</th>
                                    <th class="py-2 px-3 font-medium">"Encoded (E)*"</th>
                                    <th class="py-2 px-3 font-medium">"Compressed (C)*"</th>
                                    <th class="py-2 px-3 font-medium">"E/C"</th>
                                    <th class="py-2 px-3 font-medium">"L/C"</th>
                                    <th class="py-2 px-3 font-medium">"Nulls"</th>
                                    <th class="py-2 px-3 font-medium">"Encodings**"</th>
                                    <th class="py-2 px-3 font-medium">"Page***"</th>
                                    <th class="py-2 px-3 font-medium">"Compression"</th>
                                </tr>
                            </thead>
                            <tbody>
                                <SchemaTableBody
                                    rows=schema_rows.clone()
                                    col_distinct_count=col_distinct_count
                                    set_col_distinct_count=set_col_distinct_count
                                    table_name=table_name.clone()
                                    col_page_encodings=col_page_encodings
                                    set_col_page_encodings=set_col_page_encodings
                                    parquet_reader=parquet_reader.clone()
                                />
                            </tbody>
                        </table>
                    </div>
                }
            })}
            <div class="text-xs text-gray-600 space-y-1">
                <p>
                    "*: " <strong>"Logical size (L)"</strong>": estimated in-memory size."
                    " " <strong>"Encoded size (E)"</strong>": size before compression."
                    " " <strong>"Compressed size (C)"</strong>": size after compression."
                </p>
                <p>
                    "**: " <strong>"All encodings"</strong>
                    " comes from file metadata (may include repetition/definition level encodings)."
                </p>
                <p>
                    "***: " <strong>"Page encodings"</strong>
                    " scan page data and ignore repetition/definition level encodings."
                </p>
            </div>
            {(!schema.metadata().is_empty())
                .then(|| {
                    view! {
                        <div class="mt-2">
                            <details>
                                <summary class="cursor-pointer text-sm font-medium text-gray-700 py-2">
                                    "Metadata"
                                </summary>
                                <div class="pl-4 pt-2 pb-2 border-l-2 border-gray-200 mt-2 text-sm">
                                    <pre class="whitespace-pre-wrap break-words bg-gray-50 p-2 rounded font-mono text-xs overflow-auto max-h-60">
                                        {format!("{:#?}", schema.metadata())}
                                    </pre>
                                </div>
                            </details>
                        </div>
                    }
                })}
        </Panel>
    }
}

#[component]
fn SchemaTableBody(
    rows: Arc<Vec<SchemaRow>>,
    col_distinct_count: ReadSignal<Vec<Option<LocalResource<u32>>>>,
    set_col_distinct_count: WriteSignal<Vec<Option<LocalResource<u32>>>>,
    table_name: String,
    col_page_encodings: ReadSignal<Vec<Option<LocalResource<String>>>>,
    set_col_page_encodings: WriteSignal<Vec<Option<LocalResource<String>>>>,
    parquet_reader: Arc<ParquetResolved>,
) -> impl IntoView {
    // Group consecutive rows with same arrow_index
    let grouped_rows: Vec<(SchemaRow, bool, usize)> = {
        let mut result = Vec::new();
        let mut i = 0;

        while i < rows.len() {
            let current_arrow_index = rows[i].arrow_index;
            let group_start = i;
            while i < rows.len() && rows[i].arrow_index == current_arrow_index {
                i += 1;
            }
            let group_size = i - group_start;

            for (offset, row) in rows[group_start..i].iter().enumerate() {
                let is_first_in_group = offset == 0;
                result.push((row.clone(), is_first_in_group, group_size));
            }
        }
        result
    };

    view! {
        <For
            each=move || grouped_rows.clone()
            key=|(row, _, _)| (row.arrow_index, row.parquet_id)
            children={
                let table_name = table_name.clone();
                let parquet_reader = parquet_reader.clone();
                move |(row, is_first_in_group, group_size)| {
                    view! {
                        <SchemaTableRow
                            row=row
                            is_first_in_group=is_first_in_group
                            group_size=group_size
                            col_distinct_count=col_distinct_count
                            set_col_distinct_count=set_col_distinct_count
                            table_name=table_name.clone()
                            col_page_encodings=col_page_encodings
                            set_col_page_encodings=set_col_page_encodings
                            parquet_reader=parquet_reader.clone()
                        />
                    }
                }
            }
        />
    }
}

#[component]
fn SchemaTableRow(
    row: SchemaRow,
    is_first_in_group: bool,
    group_size: usize,
    col_distinct_count: ReadSignal<Vec<Option<LocalResource<u32>>>>,
    set_col_distinct_count: WriteSignal<Vec<Option<LocalResource<u32>>>>,
    table_name: String,
    col_page_encodings: ReadSignal<Vec<Option<LocalResource<String>>>>,
    set_col_page_encodings: WriteSignal<Vec<Option<LocalResource<String>>>>,
    parquet_reader: Arc<ParquetResolved>,
) -> impl IntoView {
    let arrow_index = row.arrow_index;
    let arrow_name = row.arrow_name.clone();
    let arrow_type = row.arrow_type.clone();
    let arrow_nullable = row.arrow_nullable.clone();

    let parquet_id = row.parquet_id;
    let parquet_name = row.parquet_name.clone();
    let parquet_path = row.parquet_path.clone();
    let parquet_type = row.parquet_type.clone();

    view! {
        <tr class="align-top hover:bg-gray-50 border-b border-gray-100">
            {if is_first_in_group {
                let distinct_view = render_distinct_count(
                    arrow_index,
                    arrow_name.clone(),
                    table_name.clone(),
                    col_distinct_count,
                    set_col_distinct_count,
                );
                view! {
                    <td class="py-1.5 px-3" rowspan=group_size>
                        <div class="flex flex-col gap-0.5">
                            <span class="font-mono text-[11px] text-gray-500">{format!("#{arrow_index}")}</span>
                            <span class="font-semibold text-gray-900">{arrow_name}</span>
                        </div>
                    </td>
                    <td class="py-1.5 px-3" rowspan=group_size>
                        <span class="font-mono text-[11px] text-gray-600">{arrow_type}</span>
                    </td>
                    <td class="py-1.5 px-3" rowspan=group_size>
                        <span class="text-[11px] text-gray-600">{arrow_nullable}</span>
                    </td>
                    <td class="py-1.5 px-3 border-r-2" rowspan=group_size>
                        <span class="text-xs text-gray-600">{distinct_view}</span>
                    </td>
                }.into_any()
            } else {
                view! {}.into_any()
            }}

            {if let Some(pq_id) = parquet_id {
                let pq_name = parquet_name.unwrap_or_default();
                let pq_path = parquet_path.unwrap_or_default();
                let pq_type = parquet_type.unwrap_or_default();

                let path_display = if pq_path.is_empty() {
                    pq_name.clone()
                } else {
                    format!("|- {}", pq_path)
                };

                let encodings_display = row.encodings.as_ref()
                    .filter(|s| !s.is_empty())
                    .map(|s| s.as_str())
                    .unwrap_or("-");

                let compression_display = row.compression_summary.as_ref()
                    .filter(|s| !s.is_empty())
                    .map(|s| s.as_str())
                    .unwrap_or("-");

                let page_encodings = render_page_encodings(
                    pq_id,
                    parquet_reader.clone(),
                    col_page_encodings,
                    set_col_page_encodings,
                );

                view! {
                    <td class="py-1.5 px-3 font-medium text-gray-900">
                        <div class="flex flex-col leading-tight">
                            <span>{format!("#{} {}", pq_id, pq_name)}</span>
                            <span class="font-mono text-[11px] text-gray-500">{path_display}</span>
                        </div>
                    </td>
                    <td class="py-1.5 px-3 font-mono text-[11px] text-gray-600">{pq_type}</td>
                    <td class="py-1.5 px-3">{format_data_size(row.logical_size)}</td>
                    <td class="py-1.5 px-3">{format_data_size(row.encoded_size)}</td>
                    <td class="py-1.5 px-3">{format_data_size(row.compressed_size)}</td>
                    <td class="py-1.5 px-3">{format_ratio(row.compression_ratio)}</td>
                    <td class="py-1.5 px-3">{format_ratio(row.logical_compression_ratio)}</td>
                    <td class="py-1.5 px-3">{row.null_count.unwrap_or(0)}</td>
                    <td class="py-1.5 px-3">{encodings_display}</td>
                    <td class="py-1.5 px-3">{page_encodings}</td>
                    <td class="py-1.5 px-3">{compression_display}</td>
                }.into_any()
            } else {
                view! {
                    <td colspan="11" class="py-1.5 px-3 text-gray-500 text-xs italic">
                        "No Parquet columns matched this Arrow field."
                    </td>
                }.into_any()
            }}
        </tr>
    }
}

fn render_distinct_count(
    field_index: usize,
    field_name: String,
    table_name: String,
    col_distinct_count: ReadSignal<Vec<Option<LocalResource<u32>>>>,
    set_col_distinct_count: WriteSignal<Vec<Option<LocalResource<u32>>>>,
) -> AnyView {
    col_distinct_count.with(
        move |col_distinct_count| match &col_distinct_count[field_index] {
            Some(cnt) => {
                let cnt = *cnt;
                view! {
                    {move || {
                        Suspend::new(async move {
                            let cnt = cnt.await;
                            format!("{cnt}").into_any()
                        })
                    }}
                }
                .into_any()
            }
            None => view! {
                <button
                    class="text-blue-500 hover:underline focus:outline-none"
                    on:click=move |_| {
                        let column_name = field_name.clone();
                        let table = table_name.clone();
                        set_col_distinct_count.update(|col_distinct_count| {
                            col_distinct_count[field_index] =
                                Some(calculate_distinct(&column_name, &table));
                        });
                    }
                >
                    "show"
                </button>
            }
            .into_any(),
        },
    )
}

fn render_page_encodings(
    column_idx: usize,
    parquet_reader: Arc<ParquetResolved>,
    col_page_encodings: ReadSignal<Vec<Option<LocalResource<String>>>>,
    set_col_page_encodings: WriteSignal<Vec<Option<LocalResource<String>>>>,
) -> AnyView {
    let parquet_reader_clone = parquet_reader.clone();
    col_page_encodings.with(
        move |col_page_encodings| match &col_page_encodings[column_idx] {
            Some(res) => {
                let res = *res;
                view! {
                    {move || {
                        Suspend::new(async move {
                            let encodings = res.await;
                            encodings.into_any()
                        })
                    }}
                }
                .into_any()
            }
            None => view! {
                <button
                    class="text-blue-500 hover:underline focus:outline-none"
                    on:click=move |_| {
                        let reader = parquet_reader_clone.clone();
                        set_col_page_encodings.update(|col_page_encodings| {
                            col_page_encodings[column_idx] =
                                Some(calculate_page_encodings(reader.clone(), column_idx));
                        });
                    }
                >
                    "show"
                </button>
            }
            .into_any(),
        },
    )
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

fn calculate_page_encodings(
    parquet_reader: Arc<ParquetResolved>,
    column_id: usize,
) -> LocalResource<String> {
    LocalResource::new(move || {
        let mut column_reader = parquet_reader.reader().clone();
        let metadata = parquet_reader.metadata().metadata.clone();
        async move {
            let mut encoding_counts = HashMap::new();
            let mut total_pages = 0;

            for (row_group_id, _rg) in metadata.row_groups().iter().enumerate() {
                match get_column_chunk_page_info(
                    &mut column_reader,
                    &metadata,
                    row_group_id,
                    column_id,
                )
                .await
                {
                    Ok(pages) => {
                        for page in pages {
                            total_pages += 1;
                            *encoding_counts.entry(page.encoding).or_insert(0) += 1;
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }

            if total_pages == 0 {
                return "No pages found".to_string();
            }

            let mut sorted_encodings: Vec<_> = encoding_counts.into_iter().collect();
            // The `Encoding` enum derives `Ord`, so we can sort by the key.
            sorted_encodings.sort_by_key(|(encoding, _)| *encoding);

            sorted_encodings
                .iter()
                .map(|(encoding, count)| {
                    format!(
                        "{:?} [{:.2}%]",
                        encoding,
                        (*count as f32 / total_pages as f32) * 100.0
                    )
                })
                .collect::<Vec<String>>()
                .join(", ")
        }
    })
}

fn calculate_distinct(column_name: &str, table_name: &str) -> LocalResource<u32> {
    let distinct_query = format!("SELECT COUNT(DISTINCT \"{column_name}\") from \"{table_name}\"",);
    LocalResource::new(move || {
        let query = distinct_query.clone();
        async move {
            let (results, _) = execute_query_inner(&query, &SESSION_CTX).await.unwrap();

            let first_batch = results.first().unwrap();
            let distinct_value = first_batch.column(0).as_primitive::<Int64Type>().value(0);
            distinct_value as u32
        }
    })
}
