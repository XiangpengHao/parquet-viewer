use crate::components::RecordBatchTable;
use crate::{ParquetResolved, utils::format_arrow_type};
use arrow_array::{BooleanArray, Float32Array, RecordBatch, UInt64Array};
use arrow_array::{StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::scalar::ScalarValue;
use leptos::prelude::*;
use std::sync::Arc;

#[component]
pub fn SchemaSection(parquet_reader: Arc<ParquetResolved>) -> impl IntoView {
    let parquet_info = parquet_reader.metadata().clone();
    let schema = parquet_info.schema.clone();

    let metadata = parquet_info.metadata.clone();

    let column_count = metadata
        .row_groups()
        .first()
        .map(|rg| rg.columns().len())
        .unwrap_or(0);
    let mut aggregated_column_info = vec![(0, 0, None, 0); column_count];
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

    let parquet_columns = Memo::new(move |_| {
        let schema = Schema::new(vec![
            Field::new("ID", DataType::UInt32, false),
            Field::new("Name", DataType::Utf8, false),
            Field::new("Type", DataType::Utf8, false),
            Field::new("Compressed", DataType::UInt64, false),
            Field::new("Uncompressed", DataType::UInt64, false),
            Field::new("Compression ratio", DataType::Float32, false),
            Field::new("Null count", DataType::UInt32, false),
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
                let compression_ratio = if col.1 > 0 {
                    col.0 as f32 / col.1 as f32
                } else {
                    0.0
                };
                compression_ratio
            }));

        let null_count =
            UInt32Array::from_iter_values(aggregated_column_info.iter().map(|col| col.3 as u32));
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
            ],
        )
        .unwrap()
    });
    let parquet_formatter: Vec<Option<Box<dyn Fn(ScalarValue) -> String + Send + Sync + 'static>>> = vec![
        None,
        None,
        None,
        Some(Box::new(format_u64_size)),
        Some(Box::new(format_u64_size)),
        Some(Box::new(format_f32_percentage)),
        None,
    ];

    let schema_clone = schema.clone();
    let arrow_schema_table = Memo::new(move |_| {
        let display_schema = Schema::new(vec![
            Field::new("ID", DataType::UInt32, false),
            Field::new("Field Name", DataType::Utf8, false),
            Field::new("Data Type", DataType::Utf8, false),
            Field::new("Nullable", DataType::Boolean, false),
            Field::new("Distinct Count", DataType::UInt32, false),
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
        let distinct_count =
            UInt32Array::from_iter_values(schema_clone.fields().iter().map(|_col| 0));
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

    let mut schema_formatter = vec![];
    for _ in 0..arrow_schema_table.get().num_columns() {
        schema_formatter.push(None);
    }

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-6 flex-1 overflow-auto">
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

fn format_u64_size(val: ScalarValue) -> String {
    let size = match val {
        ScalarValue::UInt64(Some(v)) => v,
        _ => return format!("{:?}", val),
    };
    if size > 1_048_576 {
        // 1MB
        format!("{:.2} MB", size as f64 / 1_048_576.0)
    } else if size > 1024 {
        // 1KB
        format!("{:.2} KB", size as f64 / 1024.0)
    } else {
        format!("{size} B")
    }
}

fn format_f32_percentage(val: ScalarValue) -> String {
    let percentage = match val {
        ScalarValue::Float32(Some(v)) => v,
        _ => return format!("{:?}", val),
    };
    format!("{:.2}%", percentage * 100.0)
}
