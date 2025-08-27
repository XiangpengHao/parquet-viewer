use crate::SESSION_CTX;
use crate::components::{RecordBatchTable, RecordFormatter};
use crate::utils::{ColumnChunk, execute_query_inner};
use crate::{ParquetResolved, utils::format_arrow_type};
use arrow::array::AsArray;
use arrow::datatypes::{Float32Type, Int64Type, UInt64Type};
use arrow_array::{BooleanArray, Float32Array, RecordBatch, UInt64Array};
use arrow_array::{StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use byte_unit::{Byte, UnitType};
use leptos::{logging, prelude::*};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::serialized_reader::SerializedPageReader;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Estimate Arrow in-memory size for a column based on its Parquet physical type
/// Returns None for variable-length data types that cannot be reliably estimated
fn calculate_arrow_memory_size(metadata: &ParquetMetaData, column_index: usize) -> Option<u64> {
    let total_rows: usize = metadata
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .sum();

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
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => {
            first_col.column_descr().type_length() as usize
        }
    };

    // Estimate Arrow memory: data + validity bitmap + metadata overhead
    let data_size = total_rows * bytes_per_value;
    let validity_bitmap_size = total_rows.div_ceil(8); // Round up to nearest byte
    let metadata_overhead = 64; // Rough estimate for array metadata
    Some((data_size + validity_bitmap_size + metadata_overhead) as u64)
}

#[component]
pub fn SchemaSection(parquet_reader: Arc<ParquetResolved>) -> impl IntoView {
    let parquet_info = parquet_reader.metadata().clone();
    let schema = parquet_info.schema.clone();

    let metadata = parquet_info.metadata.clone();

    let parquet_column_count = metadata
        .row_groups()
        .first()
        .map(|rg| rg.columns().len())
        .unwrap_or(0);

    // aggregated_column_info has followings:
    // [0]: compressed_size
    // [1]: uncompressed_size
    // [2]: null_count from col.statistics
    // [3]: all encodings
    // [4]: all compression
    let mut aggregated_column_info = vec![
        (
            0,
            0,
            0,
            HashSet::<String>::new(),
            HashMap::<String, u32>::new()
        );
        parquet_column_count
    ];
    for rg in metadata.row_groups() {
        for (i, col) in rg.columns().iter().enumerate() {
            // [0]: compressed_size
            aggregated_column_info[i].0 += col.compressed_size() as u64;

            // [1]: uncompressed_size
            aggregated_column_info[i].1 += col.uncompressed_size() as u64;

            // [2]: null_count from col.statistics
            aggregated_column_info[i].2 += match col.statistics() {
                None => 0,
                Some(statistics) => statistics.null_count_opt().unwrap_or(0),
            };

            // [3]: all encodings
            for encoding_it in col.encodings() {
                aggregated_column_info[i]
                    .3
                    .insert(format!("{encoding_it:?}"));
                // Note that would contain the encodings from definition and repetation level
            }

            // [4]: all compression
            *aggregated_column_info[i]
                .4
                .entry(format!("{:?}", col.compression()))
                .or_insert(0) += 1;
        }
    }

    let parquet_columns = Memo::new(move |_| {
        let schema = Schema::new(vec![
            Field::new("ID", DataType::UInt32, false),
            Field::new("Name", DataType::Utf8, false), // String
            Field::new("Type", DataType::Utf8, false), // String
            Field::new("Logical size (L)*", DataType::UInt64, false),
            Field::new("Encoded size (E)*", DataType::UInt64, false),
            Field::new("Compressed size (C)*", DataType::UInt64, false),
            Field::new("Compression ratio = E/C", DataType::Float32, false),
            Field::new("Encoded compression ratio = L/C", DataType::Float32, false),
            Field::new("Null count", DataType::UInt32, false),
            Field::new("All encodings**", DataType::Utf8, false), // String
            Field::new("Page encodings***", DataType::Utf8, true), // String
            Field::new("All compressions", DataType::Utf8, false), // String
        ]);
        let id = UInt32Array::from_iter_values(
            aggregated_column_info
                .iter()
                .enumerate()
                .map(|(i, _col)| i as u32),
        );
        let name = StringArray::from_iter_values(aggregated_column_info.iter().enumerate().map(
            |(i, _col)| {
                let field_name = metadata.row_group(0).columns()[i].column_descr().name();
                field_name.to_string()
            },
        ));
        let data_type = StringArray::from_iter_values(
            aggregated_column_info.iter().enumerate().map(|(i, _col)| {
                let field_type = metadata.row_group(0).columns()[i].column_type();
                field_type.to_string()
            }),
        );
        let compressed =
            UInt64Array::from_iter_values(aggregated_column_info.iter().map(|col| col.0));
        let uncompressed =
            UInt64Array::from_iter_values(aggregated_column_info.iter().map(|col| col.1));

        let mut raw_data_sizes = Vec::new();
        let mut compression_ratios = Vec::new();
        let mut raw_compression_ratios = Vec::new();
        for (i, col) in aggregated_column_info.iter().enumerate() {
            let compression_ratio = if col.0 > 0 {
                col.1 as f32 / col.0 as f32
            } else {
                0.0
            };
            let (raw_data_size, raw_compression_ratio) =
                match calculate_arrow_memory_size(&metadata, i) {
                    Some(raw_size) => {
                        let ratio = if col.0 > 0 {
                            raw_size as f32 / col.0 as f32
                        } else {
                            0.0
                        };
                        (raw_size, ratio)
                    }
                    None => {
                        (0, 0.0) // For variable-length data, set to 0 for now (will be displayed as "-")
                    }
                };
            raw_data_sizes.push(raw_data_size);
            compression_ratios.push(compression_ratio);
            raw_compression_ratios.push(raw_compression_ratio);
        }
        let raw_data_size = UInt64Array::from_iter_values(raw_data_sizes);
        let compression_ratio = Float32Array::from_iter_values(compression_ratios);
        let raw_compression_ratio = Float32Array::from_iter_values(raw_compression_ratios);

        let null_count =
            UInt32Array::from_iter_values(aggregated_column_info.iter().map(|col| col.2 as u32));

        let all_encoding_types =
            StringArray::from_iter_values(aggregated_column_info.iter().map(|col| {
                let mut encodings: Vec<String> = col.3.iter().cloned().collect();
                encodings.sort(); // Sort for consistent ordering
                encodings.join(", ")
            }));

        let page_encodings =
            StringArray::from_iter((0..parquet_column_count).map(|_| None::<String>));

        // a hashmap of all compression types
        let all_compression_types =
            StringArray::from_iter_values(aggregated_column_info.iter().map(|col| {
                let total: u32 = col.4.values().sum();
                assert_ne!(total, 0, "The total number of compressions cannot be zero");
                col.4
                    .iter()
                    .map(|(k, v)| format!("{} [{:.0}%]", k, *v as f32 * 100.0 / total as f32))
                    .collect::<Vec<String>>()
                    .join(", ")
            }));

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id),
                Arc::new(name),
                Arc::new(data_type),
                Arc::new(raw_data_size),
                Arc::new(uncompressed),
                Arc::new(compressed),
                Arc::new(compression_ratio),
                Arc::new(raw_compression_ratio),
                Arc::new(null_count),
                Arc::new(all_encoding_types),
                Arc::new(page_encodings),
                Arc::new(all_compression_types),
            ],
        )
        .unwrap()
    });

    let (col_page_encodings, set_col_page_encodings) = signal(
        (0..parquet_column_count)
            .map(|_| None)
            .collect::<Vec<Option<LocalResource<String>>>>(),
    );
    let parquet_reader_clone = parquet_reader.clone();
    let page_encodings_formatter =
        move |_batch: &RecordBatch, (_col_idx, row_idx): (usize, usize)| {
            let parquet_reader = parquet_reader_clone.clone();
            col_page_encodings.with(
                move |col_page_encodings| match &col_page_encodings[row_idx] {
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
                        <span
                            class="text-gray-500 cursor-pointer"
                            on:click=move |_| {
                                logging::log!(
                                    "Click to compute page encodings for column {}",
                                    row_idx
                                );
                                set_col_page_encodings
                                    .update(|col_page_encodings| {
                                        col_page_encodings[row_idx] = Some(
                                            calculate_page_encodings(parquet_reader.clone(), row_idx),
                                        );
                                    });
                            }
                        >
                            "Click to compute"
                        </span>
                    }
                    .into_any(),
                },
            )
        };

    // parquet_formatter must match with the defined parquet_columns
    let parquet_formatter: Vec<Option<RecordFormatter>> = vec![
        None,                                  // id
        None,                                  // name
        None,                                  // data_type
        Some(Box::new(format_u64_size)),       // in-memory raw data size - show "-" for BYTE_ARRAY
        Some(Box::new(format_u64_size)),       // uncompressed
        Some(Box::new(format_u64_size)),       // compressed
        Some(Box::new(format_f32_percentage)), // compression_ratio
        Some(Box::new(format_f32_percentage)), // raw_compression_ratio - show "-" for BYTE_ARRAY
        None,                                  // null_count
        None,                                  // all_encoding_types
        Some(Box::new(page_encodings_formatter)),
        None, // all_compression_types
    ];

    let arrow_column_count = schema.fields().len();
    let (col_distinct_count, set_col_distinct_count) = signal(
        (0..arrow_column_count)
            .map(|_| None)
            .collect::<Vec<Option<LocalResource<u32>>>>(),
    );

    let schema_clone = schema.clone();
    let arrow_schema_table = Memo::new(move |_| {
        let display_schema = Schema::new(vec![
            Field::new("ID", DataType::UInt32, false),
            Field::new("Field name", DataType::Utf8, false),
            Field::new("Data type", DataType::Utf8, false),
            Field::new("Nullable", DataType::Boolean, false),
            Field::new("Distinct count", DataType::UInt32, true),
        ]);
        let id = UInt32Array::from_iter_values(
            schema_clone
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _col)| i as u32),
        );
        let field_name = StringArray::from_iter_values(
            schema_clone
                .fields()
                .iter()
                .map(|col| col.name().to_string()),
        );
        let data_type = StringArray::from_iter_values(
            schema_clone
                .fields()
                .iter()
                .map(|col| format_arrow_type(col.data_type())),
        );
        let nullable = BooleanArray::from_iter(
            schema_clone
                .fields()
                .iter()
                .map(|col| Some(col.is_nullable())),
        );
        let distinct_count = UInt32Array::from_iter(
            col_distinct_count
                .get()
                .iter()
                .enumerate()
                .map(|(i, v)| if v.is_some() { Some(i as u32) } else { None }),
        );
        RecordBatch::try_new(
            Arc::new(display_schema),
            vec![
                Arc::new(id),
                Arc::new(field_name),
                Arc::new(data_type),
                Arc::new(nullable),
                Arc::new(distinct_count),
            ],
        )
        .unwrap()
    });

    let table_name = parquet_reader.table_name().to_string();
    let schema_clone = schema.clone();
    let distinct_formatter = move |_batch: &RecordBatch, (_col_idx, row_idx): (usize, usize)| {
        let table_name = table_name.clone();
        let schema_clone = schema_clone.clone();

        col_distinct_count.with(
            move |col_distinct_count| match col_distinct_count[row_idx] {
                Some(cnt) => view! {
                    {move || {
                        Suspend::new(async move {
                            let cnt = cnt.await;
                            format!("{cnt}").into_any()
                        })
                    }}
                }
                .into_any(),
                None => view! {
                    <span
                        class="text-gray-500"
                        on:click=move |_| {
                            let col_name = schema_clone.field(row_idx).name().to_string();
                            set_col_distinct_count
                                .update(|col_distinct_count| {
                                    col_distinct_count[row_idx] = Some(
                                        calculate_distinct(&col_name, &table_name),
                                    );
                                });
                        }
                    >
                        Click to compute
                    </span>
                }
                .into_any(),
            },
        )
    };

    let schema_formatter: Vec<Option<RecordFormatter>> =
        vec![None, None, None, None, Some(Box::new(distinct_formatter))];

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-3 flex-1 overflow-auto">
            <h2 class="font-semibold mb-4">"Parquet Columns"</h2>
            <div class="overflow-x-auto w-full">
                <RecordBatchTable data=parquet_columns.get() formatter=parquet_formatter />
            </div>
            <div class="text-xs text-gray-600 mt-2">
                <p>
                "*: " <strong>Logical size</strong>" (before encoding or compression) -> " 
                      <strong>Encoded size</strong>" (after encoding, before compression) -> " 
                      <strong>Compressed size</strong>" (after both encoding and compression)"
                </p>
                <p>
                "**: " <strong>All encodings</strong> " lists all encodings read from file metadata (may include repetition/definition level encodings)."
                </p>
                <p>
                "***: " <strong>Page encodings</strong> " would scan all pages and collect the encodings for page data (not necessarily use the encodings of repetition/definition level)."
                </p>
            </div>

            <h2 class="font-semibold mb-4 mt-8">"Arrow Schema"</h2>
            <RecordBatchTable data=arrow_schema_table.get() formatter=schema_formatter />

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

            for rg in metadata.row_groups() {
                let col = rg.column(column_id);
                let byte_range = col.byte_range();
                let bytes = column_reader
                    .get_bytes(byte_range.0..(byte_range.0 + byte_range.1))
                    .await
                    .unwrap();

                let chunk = ColumnChunk::new(bytes, byte_range);

                let page_reader =
                    SerializedPageReader::new(Arc::new(chunk), col, rg.num_rows() as usize, None)
                        .unwrap();

                for page in page_reader.flatten() {
                    total_pages += 1;

                    // Count the encoding type
                    *encoding_counts.entry(page.encoding()).or_insert(0) += 1;
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

fn calculate_distinct(column_name: &String, table_name: &String) -> LocalResource<u32> {
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

fn format_u64_size(val: &RecordBatch, (col_idx, row_idx): (usize, usize)) -> AnyView {
    let col = val.column(col_idx).as_primitive::<UInt64Type>();
    let size = col.value(row_idx);

    // Check if this should show "-" for variable-length types (BYTE_ARRAY)
    if size == 0 {
        let type_col = val.column(2).as_string::<i32>(); // Type column is at index 2
        let type_str = type_col.value(row_idx);
        if type_str == "BYTE_ARRAY" {
            return "-".into_any();
        }
    }
    format!(
        "{:.2}",
        Byte::from_u64(size).get_appropriate_unit(UnitType::Binary)
    )
    .into_any()
}

fn format_f32_percentage(val: &RecordBatch, (col_idx, row_idx): (usize, usize)) -> AnyView {
    let col = val.column(col_idx).as_primitive::<Float32Type>();
    let percentage = col.value(row_idx);

    // Check if this should show "-" for variable-length types (BYTE_ARRAY)
    if percentage == 0.0 {
        let type_col = val.column(2).as_string::<i32>(); // Type column is at index 2
        let type_str = type_col.value(row_idx);
        if type_str == "BYTE_ARRAY" {
            return "-".into_any();
        }
    }
    format!("{:.0}%", percentage * 100.0).into_any()
}
