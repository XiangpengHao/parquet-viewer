use crate::{ParquetResolved, execute_query_inner, utils::format_arrow_type};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use leptos::prelude::*;
use std::sync::Arc;

#[derive(Clone)]
struct ColumnData {
    id: usize,
    name: String,
    data_type: String,
    compressed_size: u64,
    uncompressed_size: u64,
    compression_ratio: f64,
    null_count: i32,
}

impl PartialEq for ColumnData {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.data_type == other.data_type
            && self.compressed_size == other.compressed_size
            && self.uncompressed_size == other.uncompressed_size
            && self.compression_ratio == other.compression_ratio
            && self.null_count == other.null_count
    }
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
pub fn SchemaSection(parquet_reader: Arc<ParquetResolved>) -> impl IntoView {
    let parquet_info = parquet_reader.display_info.clone();
    let schema = parquet_info.schema.clone();

    let metadata = parquet_info.metadata.clone();

    let mut aggregated_column_info = vec![(0, 0, None, 0); metadata.row_group(0).columns().len()];
    for rg in metadata.row_groups() {
        for (i, col) in rg.columns().iter().enumerate() {
            aggregated_column_info[i].0 += col.compressed_size() as u64;
            aggregated_column_info[i].1 += col.uncompressed_size() as u64;
            aggregated_column_info[i].2 = Some(col.compression());
            aggregated_column_info[i].3 += match col.statistics() {
                None => 0,
                Some(statistics) => statistics.null_count_opt().unwrap_or(0),
            };
        }
    }

    let (sort_field, set_sort_field) = signal(SortField::Id);
    let (sort_ascending, set_sort_ascending) = signal(true);

    let (distinct_count, set_distinct_count) =
        signal(vec![None::<LocalResource<usize>>; schema.fields.len()]);

    let table_name = Memo::new(move |_| parquet_reader.table_name.clone());
    // Transform the data into ColumnData structs
    let column_data = Memo::new(move |_| {
        let mut data: Vec<ColumnData> = aggregated_column_info
            .iter()
            .enumerate()
            .map(|(i, aggregated)| {
                let compressed = aggregated.0;
                let uncompressed = aggregated.1;
                let null_count = aggregated.3 as i32;
                let field_name = metadata.row_group(0).columns()[i].column_descr().name();
                let data_type = metadata.row_group(0).columns()[i].column_type();
                ColumnData {
                    id: i,
                    name: field_name.to_string(),
                    data_type: format!("{}", data_type),
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

    let calculate_distinct = move |col_id: usize, column_name: &String, table_name: &String| {
        let distinct_query = format!(
            "SELECT COUNT(DISTINCT \"{}\") from \"{}\"",
            column_name, table_name
        );
        let distinct_column_count = LocalResource::new(move || {
            let query = distinct_query.clone();
            async move {
                let (results, _) = execute_query_inner(&query).await.unwrap();

                let first_batch = results.first().unwrap();
                let distinct_value = first_batch.column(0).as_primitive::<Int64Type>().value(0);
                distinct_value as usize
            }
        });
        set_distinct_count.update(|distinct_count| {
            distinct_count[col_id] = Some(distinct_column_count);
        });
    };

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-6 flex-1 overflow-auto">
            <h2 class="font-semibold mb-4">"Parquet Columns"</h2>
            <table class="min-w-full table-fixed text-sm">
                <thead>
                    <tr class="bg-gray-50 text-gray-700 font-medium">
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
                            "Null"
                        </th>
                    </tr>
                </thead>
                <tbody class="text-gray-700">
                    {move || {
                        column_data
                            .get()
                            .into_iter()
                            .map(|col| {
                                view! {
                                    <tr class="hover:bg-gray-50">
                                        <td class="px-4 py-2">{col.id}</td>
                                        <td class="px-4 py-2">{col.name.clone()}</td>
                                        <td class="px-4 py-2">{col.data_type}</td>
                                        <td class="px-4 py-2">
                                            {format_size(col.compressed_size)}
                                        </td>
                                        <td class="px-4 py-2">
                                            {format_size(col.uncompressed_size)}
                                        </td>
                                        <td class="px-4 py-2">
                                            {format!("{:.2}%", col.compression_ratio * 100.0)}
                                        </td>
                                        <td class="px-4 py-2">{col.null_count}</td>
                                    </tr>
                                }
                            })
                            .collect::<Vec<_>>()
                    }}
                </tbody>
            </table>
            <h2 class="font-semibold mb-4 mt-8">"Arrow Schema"</h2>
            <table class="min-w-full text-sm">
                <thead>
                    <tr class="bg-gray-50 text-gray-700 font-medium">
                        <th class="px-4 py-2 text-left">Field Name</th>
                        <th class="px-4 py-2 text-left">Data Type</th>
                        <th class="px-4 py-2 text-left">Nullable</th>
                        <th class="px-4 py-2 text-left">Distinct Count</th>
                    </tr>
                </thead>
                <tbody>
                    {schema
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(idx, field)| {
                            let type_display = format_arrow_type(field.data_type());
                            let field_name = field.name().to_string();
                            let field_id = idx as i32;

                            view! {
                                <tr class="border-b hover:bg-gray-50 text-gray-700">
                                    <td class="px-4 py-2 font-medium">{field_name.clone()}</td>
                                    <td class="px-4 py-2 font-mono">{type_display}</td>
                                    <td class="px-4 py-2">
                                        {if field.is_nullable() { "✓" } else { "✗" }}
                                    </td>
                                    <td class="px-4 py-2">
                                        <button
                                            on:click=move |_| {
                                                calculate_distinct(
                                                    field_id as usize,
                                                    &field_name,
                                                    &table_name.get(),
                                                );
                                            }
                                            class="hover:bg-gray-100 px-2 py-1 rounded"
                                        >
                                            {move || {
                                                distinct_count
                                                    .with(|distinct_count| {
                                                        distinct_count[field_id as usize]
                                                            .as_ref()
                                                            .and_then(|count| count.get().map(|c| c.to_string()))
                                                            .unwrap_or("👁️‍🗨".to_string())
                                                    })
                                            }}
                                        </button>
                                    </td>
                                </tr>
                            }
                        })
                        .collect::<Vec<_>>()}
                </tbody>
            </table>

            {(!schema.metadata().is_empty())
                .then(|| {
                    view! {
                        <div class="mt-4">
                            <details>
                                <summary class="cursor-pointer text-sm font-medium text-gray-700 py-2">
                                    Metadata
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
        </div>
    }
}
