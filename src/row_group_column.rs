use std::sync::Arc;

use bytes::{Buf, Bytes};
use leptos::prelude::*;
use parquet::{
    arrow::async_reader::AsyncFileReader,
    basic::{Compression, Encoding, PageType},
    errors::ParquetError,
    file::{
        reader::{ChunkReader, Length, SerializedPageReader},
        statistics::Statistics,
    },
};

use crate::{ParquetTable, format_rows};

fn stats_to_string(stats: Option<Statistics>) -> String {
    match stats {
        Some(stats) => {
            let mut parts = Vec::new();
            match &stats {
                Statistics::Int32(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {}", max));
                    }
                }
                Statistics::Int64(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {}", max));
                    }
                }
                Statistics::Int96(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {}", max));
                    }
                }
                Statistics::Boolean(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {}", max));
                    }
                }
                Statistics::Float(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {:.2}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {:.2}", max));
                    }
                }
                Statistics::Double(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {:.2}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {:.2}", max));
                    }
                }
                Statistics::ByteArray(s) => {
                    if let Some(min) = s.min_opt() {
                        if let Ok(min_utf8) = min.as_utf8() {
                            parts.push(format!("min: {:?}", min_utf8));
                        }
                    }
                    if let Some(max) = s.max_opt() {
                        if let Ok(max_utf8) = max.as_utf8() {
                            parts.push(format!("max: {:?}", max_utf8));
                        }
                    }
                }
                Statistics::FixedLenByteArray(s) => {
                    if let Some(min) = s.min_opt() {
                        if let Ok(min_utf8) = min.as_utf8() {
                            parts.push(format!("min: {:?}", min_utf8));
                        }
                    }
                    if let Some(max) = s.max_opt() {
                        if let Ok(max_utf8) = max.as_utf8() {
                            parts.push(format!("max: {:?}", max_utf8));
                        }
                    }
                }
            }

            if let Some(null_count) = stats.null_count_opt() {
                parts.push(format!("nulls: {}", format_rows(null_count)));
            }

            if let Some(distinct_count) = stats.distinct_count_opt() {
                parts.push(format!("distinct: {}", format_rows(distinct_count)));
            }

            if parts.is_empty() {
                "✗".to_string()
            } else {
                parts.join(" / ")
            }
        }
        None => "✗".to_string(),
    }
}

#[derive(Clone)]
struct ColumnInfo {
    compressed_size: f64,
    uncompressed_size: f64,
    compression: Compression,
    statistics: Option<Statistics>,
    page_info: Vec<(PageType, f64, u32, Encoding)>,
}

struct ColumnChunk {
    data: Bytes,
    byte_range: (u64, u64),
}

impl Length for ColumnChunk {
    fn len(&self) -> u64 {
        self.byte_range.1 - self.byte_range.0
    }
}

impl ChunkReader for ColumnChunk {
    type T = bytes::buf::Reader<Bytes>;
    fn get_read(&self, offset: u64) -> Result<Self::T, ParquetError> {
        let start = offset - self.byte_range.0;
        Ok(self.data.slice(start as usize..).reader())
    }

    fn get_bytes(&self, offset: u64, length: usize) -> Result<Bytes, ParquetError> {
        let start = offset - self.byte_range.0;
        Ok(self.data.slice(start as usize..(start as usize + length)))
    }
}

#[component]
pub fn RowGroupColumn(parquet_reader: Arc<ParquetTable>) -> impl IntoView {
    let display_info = &parquet_reader.display_info;
    let (selected_row_group, set_selected_row_group) = signal(0);
    let (selected_column, set_selected_column) = signal(0);

    let metadata = display_info.metadata.clone();
    let row_group_info = move || {
        let rg = metadata.row_group(selected_row_group.get());
        let compressed_size = rg.compressed_size() as f64 / 1_048_576.0;
        let uncompressed_size = rg.total_byte_size() as f64 / 1_048_576.0;
        let num_rows = rg.num_rows() as u64;
        (compressed_size, uncompressed_size, num_rows)
    };

    let sorted_fields = {
        let mut fields = display_info
            .schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (i, f.name()))
            .collect::<Vec<_>>();

        fields.sort_by(|a, b| a.1.cmp(b.1));
        fields
    };

    let metadata = display_info.metadata.clone();
    let column_byte_range = move || {
        let rg = metadata.row_group(selected_row_group.get());
        let col = rg.column(selected_column.get());
        col.byte_range()
    };

    let (column_info, set_column_info) = signal(None::<ColumnInfo>);

    let metadata = display_info.metadata.clone();
    let reader = parquet_reader.reader.clone();
    Effect::watch(
        column_byte_range,
        move |byte_range, _, _| {
            let byte_range = *byte_range;
            let metadata = metadata.clone();
            let mut reader = reader.clone();
            leptos::task::spawn_local(async move {
                let range = byte_range.0 as usize..(byte_range.0 + byte_range.1) as usize;
                let bytes = reader.get_bytes(range).await.unwrap();
                let chunk = ColumnChunk {
                    data: bytes,
                    byte_range,
                };

                let rg = metadata.row_group(selected_row_group.get());
                let col = rg.column(selected_column.get());
                let row_count = rg.num_rows();
                let compressed_size = col.compressed_size() as f64 / 1_048_576.0;
                let uncompressed_size = col.uncompressed_size() as f64 / 1_048_576.0;
                let compression = col.compression();
                let statistics = col.statistics().cloned();

                let page_reader =
                    SerializedPageReader::new(Arc::new(chunk), col, row_count as usize, None)
                        .unwrap();

                let mut page_info = Vec::new();
                for page in page_reader.flatten() {
                    let page_type = page.page_type();
                    let page_size = page.buffer().len() as f64 / 1024.0;
                    let num_values = page.num_values();
                    page_info.push((page_type, page_size, num_values, page.encoding()));
                }

                set_column_info.set(Some(ColumnInfo {
                    compressed_size,
                    uncompressed_size,
                    compression,
                    statistics,
                    page_info,
                }));
            });
        },
        true,
    );

    view! {
        <div class="space-y-8">
            // Row Group Selection
            <div class="flex flex-col space-y-2">
                <div class="flex items-center">
                    <label for="row-group-select" class="text-sm font-medium text-gray-700 w-32">
                        "Row Group"
                    </label>
                    <select
                        id="row-group-select"
                        class="w-full bg-white text-gray-700 text-sm font-medium rounded-lg border border-gray-200 px-4 py-2.5 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer bg-[url('data:image/svg+xml;charset=US-ASCII,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%20width%3D%2224%22%20height%3D%2224%22%20viewBox%3D%220%200%2024%2024%22%20fill%3D%22none%22%20stroke%3D%22%23666%22%20stroke-width%3D%222%22%20stroke-linecap%3D%22round%22%20stroke-linejoin%3D%22round%22%3E%3Cpolyline%20points%3D%226%209%2012%2015%2018%209%22%3E%3C%2Fpolyline%3E%3C%2Fsvg%3E')] bg-[length:1.5em] bg-[right_0.5em_center] bg-no-repeat"
                        on:change=move |ev| {
                            set_selected_row_group
                                .set(event_target_value(&ev).parse::<usize>().unwrap_or(0))
                        }
                    >
                        {(0..display_info.row_group_count)
                            .map(|i| {
                                view! {
                                    <option value=i.to_string() class="py-2">
                                        {format!("{}", i)}
                                    </option>
                                }
                            })
                            .collect::<Vec<_>>()}
                    </select>
                </div>

                {move || {
                    let (compressed_size, uncompressed_size, num_rows) = row_group_info();
                    view! {
                        <div class="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-md">
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Compressed"</div>
                                <div class="font-medium">
                                    {format!("{:.2} MB", compressed_size)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Uncompressed"</div>
                                <div class="font-medium">
                                    {format!("{:.2} MB", uncompressed_size)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Compression ratio"</div>
                                <div class="font-medium">
                                    {format!("{:.1}%", compressed_size / uncompressed_size * 100.0)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Rows"</div>
                                <div class="font-medium">{format_rows(num_rows)}</div>
                            </div>
                        </div>
                    }
                }}
            </div>

            // Column Selection
            <div class="flex flex-col space-y-2">
                <div class="flex items-center">
                    <label for="column-select" class="text-sm font-medium text-gray-700 w-32">
                        "Column"
                    </label>
                    <select
                        id="column-select"
                        class="w-full bg-white text-gray-700 text-sm font-medium rounded-lg border border-gray-200 px-4 py-2.5 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer bg-[url('data:image/svg+xml;charset=US-ASCII,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%20width%3D%2224%22%20height%3D%2224%22%20viewBox%3D%220%200%2024%2024%22%20fill%3D%22none%22%20stroke%3D%22%23666%22%20stroke-width%3D%222%22%20stroke-linecap%3D%22round%22%20stroke-linejoin%3D%22round%22%3E%3Cpolyline%20points%3D%226%209%2012%2015%2018%209%22%3E%3C%2Fpolyline%3E%3C%2Fsvg%3E')] bg-[length:1.5em] bg-[right_0.5em_center] bg-no-repeat"
                        on:change=move |ev| {
                            set_selected_column
                                .set(event_target_value(&ev).parse::<usize>().unwrap_or(0))
                        }
                    >
                        {sorted_fields
                            .iter()
                            .map(|(i, field)| {
                                view! {
                                    <option value=i.to_string() class="py-2">
                                        {field.to_string()}
                                    </option>
                                }
                            })
                            .collect::<Vec<_>>()}
                    </select>
                </div>

                {move || {
                    {
                        if let Some(column_info) = column_info.get() {
                            view! {
                                <div class="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-md">
                                    <div class="space-y-1">
                                        <div class="text-sm text-gray-500">"Compressed"</div>
                                        <div class="font-medium">
                                            {format!("{:.2} MB", column_info.compressed_size)}
                                        </div>
                                    </div>
                                    <div class="space-y-1">
                                        <div class="text-sm text-gray-500">"Uncompressed"</div>
                                        <div class="font-medium">
                                            {format!("{:.2} MB", column_info.uncompressed_size)}
                                        </div>
                                    </div>
                                    <div class="space-y-1">
                                        <div class="text-sm text-gray-500">"Compression ratio"</div>
                                        <div class="font-medium">
                                            {format!(
                                                "{:.1}%",
                                                column_info.compressed_size / column_info.uncompressed_size
                                                    * 100.0,
                                            )}
                                        </div>
                                    </div>
                                    <div class="space-y-1">
                                        <div class="text-sm text-gray-500">"Compression Type"</div>
                                        <div class="font-medium">
                                            {format!("{:?}", column_info.compression)}
                                        </div>
                                    </div>
                                    <div class="col-span-2 space-y-1">
                                        <div class="text-sm text-gray-500">"Statistics"</div>
                                        <div class="font-medium text-sm">
                                            {stats_to_string(column_info.statistics)}
                                        </div>
                                    </div>
                                    <div class="col-span-2 space-y-1">
                                        <div class="space-y-0.5">
                                            <div class="flex gap-4 text-sm text-gray-500">
                                                <span class="w-4">"#"</span>
                                                <span class="w-32">"Type"</span>
                                                <span class="w-16">"Size"</span>
                                                <span class="w-16">"Rows"</span>
                                                <span>"Encoding"</span>
                                            </div>
                                            <div class="max-h-[250px] overflow-y-auto pr-2">
                                                {column_info
                                                    .page_info
                                                    .into_iter()
                                                    .enumerate()
                                                    .map(|(i, (page_type, size, values, encoding))| {
                                                        view! {
                                                            <div class="flex gap-4 text-sm">
                                                                <span class="w-4">{format!("{}", i)}</span>
                                                                <span class="w-32">{format!("{:?}", page_type)}</span>
                                                                <span class="w-16">
                                                                    {format!("{} KB", size.round() as i64)}
                                                                </span>
                                                                <span class="w-16">{format_rows(values as u64)}</span>
                                                                <span>{format!("{:?}", encoding)}</span>
                                                            </div>
                                                        }
                                                    })
                                                    .collect::<Vec<_>>()}
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            }
                                .into_any()
                        } else {
                            view! {
                                <div class="flex justify-center items-center h-full">
                                    <div class="text-sm text-gray-500">"No column selected"</div>
                                </div>
                            }
                                .into_any()
                        }
                    }
                }}
            </div>
        </div>
    }
}
