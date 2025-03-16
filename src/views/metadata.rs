use crate::ParquetResolved;
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
use std::sync::Arc;

use crate::utils::format_rows;

#[component]
pub fn MetadataSection(parquet_reader: Arc<ParquetResolved>) -> impl IntoView {
    let metadata_display = parquet_reader.display_info.clone();
    let created_by = metadata_display
        .metadata
        .file_metadata()
        .created_by()
        .unwrap_or("Unknown")
        .to_string();
    let version = metadata_display.metadata.file_metadata().version();
    let has_bloom_filter = metadata_display.has_bloom_filter;
    let has_page_index = metadata_display.has_page_index;
    let has_column_index = metadata_display.has_column_index;
    let has_row_group_stats = metadata_display.has_row_group_stats;

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-6 text-sm">
            <div class="grid grid-cols-2 gap-6">
                <div>
                    <h2 class="font-semibold text-normal mb-4">"Metadata"</h2>
                    <div class="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-md mb-8">
                        <div class="space-y-2">
                            <span class="text-gray-400">"File size"</span>
                            <span class="block">
                                {format!(
                                    "{:.2} MB",
                                    metadata_display.file_size as f64 / 1_048_576.0,
                                )}
                            </span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-400">"Metadata size"</span>
                            <span class="block">
                                {format!("{:.2} KB", metadata_display.metadata_len as f64 / 1024.0)}
                            </span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-400">"Uncompressed size"</span>
                            <span class="block">
                                {format!(
                                    "{:.2} MB",
                                    metadata_display.uncompressed_size as f64 / 1_048_576.0,
                                )}
                            </span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-400">"Compression ratio"</span>
                            <span class="block">
                                {format!("{:.2}%", metadata_display.compression_ratio * 100.0)}
                            </span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-400">"Row groups"</span>
                            <span class="block">{metadata_display.row_group_count}</span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-400">"Total rows"</span>
                            <span class="block">{format_rows(metadata_display.row_count)}</span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-400">"Columns"</span>
                            <span class="block">{metadata_display.columns}</span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-400">"Created by"</span>
                            <span class="block">{created_by}</span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-400">"Version"</span>
                            <span class="block">{version}</span>
                        </div>
                    </div>
                    <div class="grid grid-cols-2 gap-2">
                        <div class="p-2 rounded ".to_owned()
                            + if has_row_group_stats {
                                "bg-green-100 text-green-800"
                            } else {
                                "bg-gray-100 text-gray-800"
                            }>
                            {if has_row_group_stats { "✓" } else { "✗" }}
                            " Row Group Statistics"
                        </div>
                        <div class="p-2 rounded ".to_owned()
                            + if has_column_index {
                                "bg-green-100 text-green-800"
                            } else {
                                "bg-gray-100 text-gray-800"
                            }>{if has_column_index { "✓" } else { "✗" }} " Column Index"</div>
                        <div class="p-2 rounded ".to_owned()
                            + if has_page_index {
                                "bg-green-100 text-green-800"
                            } else {
                                "bg-gray-100 text-gray-800"
                            }>{if has_page_index { "✓" } else { "✗" }} " Page Index"</div>
                        <div class="p-2 rounded ".to_owned()
                            + if has_bloom_filter {
                                "bg-green-100 text-green-800"
                            } else {
                                "bg-gray-100 text-gray-800"
                            }>{if has_bloom_filter { "✓" } else { "✗" }} " Bloom Filter"</div>
                    </div>
                </div>

                {move || {
                    if metadata_display.row_group_count > 0 {
                        Some(
                            view! {
                                <div>
                                    <RowGroupColumn parquet_reader=parquet_reader.clone() />
                                </div>
                            },
                        )
                    } else {
                        None
                    }
                }}
            </div>
        </div>
    }
}

fn stats_to_string(stats: &Option<Statistics>) -> String {
    match stats {
        Some(stats) => {
            let mut parts = Vec::new();
            match stats {
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
pub fn RowGroupColumn(parquet_reader: Arc<ParquetResolved>) -> impl IntoView {
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

    let column_reader = parquet_reader.reader.clone();
    let metadata = display_info.metadata.clone();
    let column_info = LocalResource::new(move || {
        let byte_range = column_byte_range();
        let mut reader = column_reader.clone();
        let metadata = metadata.clone();
        async move {
            let bytes = reader
                .get_bytes(byte_range.0 as usize..(byte_range.0 + byte_range.1) as usize)
                .await
                .unwrap();
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
                SerializedPageReader::new(Arc::new(chunk), col, row_count as usize, None).unwrap();

            let mut page_info = Vec::new();
            for page in page_reader.flatten() {
                let page_type = page.page_type();
                let page_size = page.buffer().len() as f64 / 1024.0;
                let num_values = page.num_values();
                page_info.push((page_type, page_size, num_values, page.encoding()));
            }

            ColumnInfo {
                compressed_size,
                uncompressed_size,
                compression,
                statistics,
                page_info,
            }
        }
    });

    view! {
        <div class="space-y-8">
            <div class="flex flex-col space-y-2">
                <div class="flex items-center">
                    <label for="row-group-select" class="text-sm text-gray-700 w-32">
                        "Row Group"
                    </label>
                    <select
                        id="row-group-select"
                        class="w-full bg-white text-gray-700 text-sm rounded-lg border border-gray-200 px-4 py-2.5 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer"
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
                                <div>{format!("{:.2} MB", compressed_size)}</div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Uncompressed"</div>
                                <div>{format!("{:.2} MB", uncompressed_size)}</div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Compression ratio"</div>
                                <div>
                                    {format!("{:.1}%", compressed_size / uncompressed_size * 100.0)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Rows"</div>
                                <div>{format_rows(num_rows)}</div>
                            </div>
                        </div>
                    }
                }}
            </div>

            // Column Selection
            <div class="flex flex-col space-y-2">
                <div class="flex items-center">
                    <label for="column-select" class="text-sm text-gray-700 w-32">
                        "Column"
                    </label>
                    <select
                        id="column-select"
                        class="w-full bg-white text-gray-700 text-sm rounded-lg border border-gray-200 px-4 py-2.5 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer "
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
                <Suspense fallback=move || {
                    view! {
                        <div class="flex justify-center items-center h-full">
                            <div class="text-sm text-gray-500">"Loading column info..."</div>
                        </div>
                    }
                }>
                    {move || Suspend::new(async move {
                        let column_info = column_info.await;
                        view! {
                            <div class="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-md">
                                <div class="space-y-1">
                                    <div class="text-sm text-gray-500">"Compressed"</div>
                                    <div>{format!("{:.2} MB", column_info.compressed_size)}</div>
                                </div>
                                <div class="space-y-1">
                                    <div class="text-sm text-gray-500">"Uncompressed"</div>
                                    <div>{format!("{:.2} MB", column_info.uncompressed_size)}</div>
                                </div>
                                <div class="space-y-1">
                                    <div class="text-sm text-gray-500">"Compression ratio"</div>
                                    <div>
                                        {format!(
                                            "{:.1}%",
                                            column_info.compressed_size / column_info.uncompressed_size
                                                * 100.0,
                                        )}
                                    </div>
                                </div>
                                <div class="space-y-1">
                                    <div class="text-sm text-gray-500">"Compression Type"</div>
                                    <div>{format!("{:?}", column_info.compression)}</div>
                                </div>
                                <div class="col-span-2 space-y-1">
                                    <div class="text-sm text-gray-500">"Statistics"</div>
                                    <div class="text-sm">
                                        {stats_to_string(&column_info.statistics)}
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
                                                .iter()
                                                .enumerate()
                                                .map(|(i, (page_type, size, values, encoding))| {
                                                    view! {
                                                        <div class="flex gap-4 text-sm">
                                                            <span class="w-4">{format!("{}", i)}</span>
                                                            <span class="w-32">{format!("{:?}", page_type)}</span>
                                                            <span class="w-16">
                                                                {format!("{} KB", size.round() as i64)}
                                                            </span>
                                                            <span class="w-16">{format_rows(*values as u64)}</span>
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
                    })}
                </Suspense>

            </div>
        </div>
    }
}
