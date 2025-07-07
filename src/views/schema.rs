use crate::SESSION_CTX;
use crate::components::{RecordBatchTable, RecordFormatter};
use crate::utils::execute_query_inner;
use crate::{ParquetResolved, utils::format_arrow_type};
use arrow::array::AsArray;
use arrow::datatypes::{Float32Type, Int64Type, UInt64Type};
use arrow_array::{BooleanArray, Float32Array, RecordBatch, UInt64Array};
use arrow_array::{StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use byte_unit::{Byte, UnitType};
use leptos::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

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
            HashMap::<String, u32>::new(),
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
                *aggregated_column_info[i]
                    .3
                    .entry(format!("{encoding_it:?}"))
                    .or_insert(0) += 1;
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
            Field::new("Compressed", DataType::UInt64, false),
            Field::new("Uncompressed", DataType::UInt64, false),
            Field::new("Compression ratio", DataType::Float32, false),
            Field::new("Null count", DataType::UInt32, false),
            Field::new("All encodings", DataType::Utf8, false), // String
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
        let compression_ratio =
            Float32Array::from_iter_values(aggregated_column_info.iter().map(|col| {
                if col.1 > 0 {
                    col.0 as f32 / col.1 as f32
                } else {
                    0.0
                }
            }));

        let null_count =
            UInt32Array::from_iter_values(aggregated_column_info.iter().map(|col| col.2 as u32));

        // a hashmap of all encoding types
        let all_encoding_types =
            StringArray::from_iter_values(aggregated_column_info.iter().map(|col| {
                let total_encoding_count: u32 = col.3.values().sum(); // this is a multiple of number of row groups
                assert_ne!(
                    total_encoding_count, 0,
                    "The total number of encodings cannot be zero"
                );
                col.3
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "{} [{:.0}%]",
                            k,
                            *v as f32 * 100.0 / total_encoding_count as f32
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(", ")
            }));

        // a hashmap of all compression types
        let all_compression_types =
            StringArray::from_iter_values(aggregated_column_info.iter().map(|col| {
                col.4
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "{} [{:.0}%]",
                            k,
                            *v as f32 * 100.0 / metadata.row_groups().len() as f32
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(", ")
            }));

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id),
                Arc::new(name),
                Arc::new(data_type),
                Arc::new(compressed),
                Arc::new(uncompressed),
                Arc::new(compression_ratio),
                Arc::new(null_count),
                Arc::new(all_encoding_types),
                Arc::new(all_compression_types),
            ],
        )
        .unwrap()
    });

    // parquet_formatter must match with the defined parquet_columns
    let parquet_formatter: Vec<Option<RecordFormatter>> = vec![
        None,                                  // id
        None,                                  // name
        None,                                  // data_type
        Some(Box::new(format_u64_size)),       // compressed
        Some(Box::new(format_u64_size)),       // uncompressed
        Some(Box::new(format_f32_percentage)), // compression_ratio
        None,                                  // null_count
        None,                                  // all_encoding_types
        None,                                  // all_compression_types
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
            <RecordBatchTable data=parquet_columns.get() formatter=parquet_formatter />

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
    format!(
        "{:.2}",
        Byte::from_u64(size).get_appropriate_unit(UnitType::Binary)
    )
    .into_any()
}

fn format_f32_percentage(val: &RecordBatch, (col_idx, row_idx): (usize, usize)) -> AnyView {
    let col = val.column(col_idx).as_primitive::<Float32Type>();
    let percentage = col.value(row_idx);
    format!("{:.2}%", percentage * 100.0).into_any()
}
