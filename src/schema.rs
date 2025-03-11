use crate::{ParquetTable, execute_query_inner};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use leptos::{logging, prelude::*};
use std::clone::Clone;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
struct ColumnData {
    id: usize,
    name: String,
    data_type: String,
    compressed_size: u64,
    uncompressed_size: u64,
    compression_ratio: f64,
    null_count: i32,
}

#[derive(Clone, Copy, PartialEq)]
enum SortField {
    Id,
    Name,
    DataType,
    CompressedSize,
    UncompressedSize,
    CompressionRatio,
    NullCount,
}

#[component]
pub fn SchemaSection(parquet_reader: Arc<ParquetTable>) -> impl IntoView {
    let parquet_info = parquet_reader.display_info.clone();
    let schema = parquet_info.schema.clone();
    let metadata = parquet_info.metadata.clone();
    let mut column_info = vec![
        (
            0,
            0,
            metadata
                .row_groups()
                .first()
                .and_then(|rg| rg.columns().first().map(|c| c.compression())),
            0,
        );
        schema.fields.len()
    ];
    for rg in metadata.row_groups() {
        for (i, col) in rg.columns().iter().enumerate() {
            column_info[i].0 += col.compressed_size() as u64;
            column_info[i].1 += col.uncompressed_size() as u64;
            column_info[i].2 = Some(col.compression());
            column_info[i].3 = match col.statistics() {
                None => 0,
                Some(statistics) => statistics.null_count_opt().unwrap_or(0),
            }
        }
    }

    let (sort_field, set_sort_field) = signal(SortField::Id);
    let (sort_ascending, set_sort_ascending) = signal(true);

    let table_name = Memo::new(move |_| parquet_reader.table_name.clone());
    // Transform the data into ColumnData structs
    let column_data = Memo::new(move |_| {
        let mut data: Vec<ColumnData> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let compressed = column_info[i].0;
                let uncompressed = column_info[i].1;
                let null_count = column_info[i].3 as i32;
                ColumnData {
                    id: i,
                    name: field.name().to_string(),
                    data_type: format!("{}", field.data_type()),
                    compressed_size: compressed,
                    uncompressed_size: uncompressed,
                    compression_ratio: if uncompressed > 0 {
                        compressed as f64 / uncompressed as f64
                    } else {
                        0.0
                    },
                    null_count,
                }
            })
            .collect();

        // Sort the data based on current sort field
        data.sort_by(|a, b| {
            let cmp = match sort_field.get() {
                SortField::Id => a.id.cmp(&b.id),
                SortField::Name => a.name.cmp(&b.name),
                SortField::DataType => a.data_type.cmp(&b.data_type),
                SortField::CompressedSize => a.compressed_size.cmp(&b.compressed_size),
                SortField::UncompressedSize => a.uncompressed_size.cmp(&b.uncompressed_size),
                SortField::CompressionRatio => a
                    .compression_ratio
                    .partial_cmp(&b.compression_ratio)
                    .unwrap(),
                SortField::NullCount => a.null_count.cmp(&b.null_count),
            };
            if sort_ascending.get() {
                cmp
            } else {
                cmp.reverse()
            }
        });
        data
    });

    let sort_by = move |field: SortField| {
        if sort_field.get() == field {
            set_sort_ascending.update(|v| *v = !*v);
        } else {
            set_sort_field.set(field);
            set_sort_ascending.set(true);
        }
    };

    fn format_size(size: u64) -> String {
        if size > 1_048_576 {
            // 1MB
            format!("{:.2} MB", size as f64 / 1_048_576.0)
        } else if size > 1024 {
            // 1KB
            format!("{:.2} KB", size as f64 / 1024.0)
        } else {
            format!("{} B", size)
        }
    }

    fn calculate_distinct(
        set_distinct_values: WriteSignal<HashMap<usize, String>>,
        col_id: usize,
        column_name: &String,
        table_name: &String,
    ) {
        let distinct_query = format!(
            "SELECT COUNT(DISTINCT \"{}\") from \"{}\"",
            column_name, table_name
        );
        leptos::task::spawn_local(async move {
            match execute_query_inner(&distinct_query).await {
                Ok((results, _)) => {
                    if let Some(first_batch) = results.first() {
                        let distinct_value =
                            first_batch.column(0).as_primitive::<Int64Type>().value(0);
                        set_distinct_values.update(|m| {
                            m.insert(col_id, distinct_value.to_string());
                        });
                    }
                }
                Err(e) => {
                    logging::log!("Failed to find distinct value. Error '{}'", e);
                }
            }
        });
    }

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-6 flex-1 overflow-auto">
            <h2 class="text-xl font-semibold mb-4">"Arrow Schema"</h2>
            <table class="min-w-full table-fixed">
                <thead>
                    <tr class="bg-gray-50">
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100 text-left"
                            on:click=move |_| sort_by(SortField::Id)
                        >
                            "ID"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100 text-left"
                            on:click=move |_| sort_by(SortField::Name)
                        >
                            "Name"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100 text-left"
                            on:click=move |_| sort_by(SortField::DataType)
                        >
                            "Type"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100 text-left"
                            on:click=move |_| sort_by(SortField::CompressedSize)
                        >
                            "Compressed"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100 text-left"
                            on:click=move |_| sort_by(SortField::UncompressedSize)
                        >
                            "Uncompressed"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100 text-left"
                            on:click=move |_| sort_by(SortField::CompressionRatio)
                        >
                            "Ratio"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100 text-left"
                            on:click=move |_| sort_by(SortField::NullCount)
                        >
                            "Null Count"
                        </th>
                        <th class="px-4 py-2 cursor-pointer hover:bg-gray-100 text-left">
                            "Distinct Count"
                        </th>
                    </tr>
                </thead>
                <tbody>
                    {move || {
                        let (distinct_values, set_distinct_values) = signal(
                            HashMap::<usize, String>::new(),
                        );
                        column_data
                            .get()
                            .into_iter()
                            .map(|col| {
                                set_distinct_values
                                    .update(|texts| {
                                        texts.insert(col.id, String::from("👁️‍🗨"));
                                    });

                                view! {
                                    <tr class="hover:bg-gray-50">
                                        <td class="px-4 py-2 text-gray-700">{col.id}</td>
                                        <td class="px-4 py-2 text-gray-700">{col.name.clone()}</td>
                                        <td class="px-4 py-2 text-gray-500">{col.data_type}</td>
                                        <td class="px-4 py-2 text-gray-500">
                                            {format_size(col.compressed_size)}
                                        </td>
                                        <td class="px-4 py-2 text-gray-500">
                                            {format_size(col.uncompressed_size)}
                                        </td>
                                        <td class="px-4 py-2 text-gray-500">
                                            {format!("{:.2}%", col.compression_ratio * 100.0)}
                                        </td>
                                        <td class="px-4 py-2 text-gray-500">{col.null_count}</td>
                                        <td class="px-4 py-2 text-gray-500">
                                            <button
                                                disabled=move || {
                                                    distinct_values
                                                        .get()
                                                        .get(&col.id)
                                                        .unwrap_or(&String::from("Not Available"))
                                                        .clone() != "👁️‍🗨"
                                                }
                                                on:click=move |_| {
                                                    calculate_distinct(
                                                        set_distinct_values,
                                                        col.id,
                                                        &col.name.clone(),
                                                        &table_name.get(),
                                                    );
                                                }
                                            >
                                                {move || {
                                                    distinct_values
                                                        .get()
                                                        .get(&col.id)
                                                        .unwrap_or(&String::from("Not Available"))
                                                        .clone()
                                                }}
                                            </button>
                                        </td>
                                    </tr>
                                }
                            })
                            .collect::<Vec<_>>()
                    }}
                </tbody>
            </table>
        </div>
    }
}
