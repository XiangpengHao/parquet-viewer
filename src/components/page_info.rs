use std::sync::Arc;

use byte_unit::{Byte, UnitType};
use bytes::{Buf, Bytes};
use leptos::prelude::*;
use parquet::{
    arrow::async_reader::AsyncFileReader,
    errors::ParquetError,
    file::{
        page_index::index::Index,
        reader::{ChunkReader, Length, SerializedPageReader},
    },
};

use crate::{parquet_ctx::ParquetResolved, utils::format_rows};

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
fn IndexDisplay(index: Index) -> impl IntoView {
    match index {
        Index::NONE => view! { <div class="text-gray-500">"No page index available"</div> }
        .into_any(),
        Index::BOOLEAN(native_index) => view! { <IndexTable indexes=native_index.indexes format_value=|v: &bool| v.to_string() /> }
        .into_any(),
        Index::INT32(native_index) => view! { <IndexTable indexes=native_index.indexes format_value=|v: &i32| v.to_string() /> }
        .into_any(),
        Index::INT64(native_index) => view! { <IndexTable indexes=native_index.indexes format_value=|v: &i64| v.to_string() /> }
        .into_any(),
        Index::INT96(native_index) => view! {
            <IndexTable
                indexes=native_index.indexes
                format_value=|v: &parquet::data_type::Int96| format!("{v:?}")
            />
        }
        .into_any(),
        Index::FLOAT(native_index) => view! { <IndexTable indexes=native_index.indexes format_value=|v: &f32| format!("{v:.6}") /> }
        .into_any(),
        Index::DOUBLE(native_index) => view! { <IndexTable indexes=native_index.indexes format_value=|v: &f64| format!("{v:.6}") /> }
        .into_any(),
        Index::BYTE_ARRAY(native_index) => view! {
            <IndexTable
                indexes=native_index.indexes
                format_value=|v: &parquet::data_type::ByteArray| {
                    String::from_utf8_lossy(v.data()).to_string()
                }
            />
        }
        .into_any(),
        Index::FIXED_LEN_BYTE_ARRAY(native_index) => view! {
            <IndexTable
                indexes=native_index.indexes
                format_value=|v: &parquet::data_type::FixedLenByteArray| {
                    String::from_utf8_lossy(v.data()).to_string()
                }
            />
        }
        .into_any(),
    }
}

#[component]
fn IndexTable<T, F>(
    indexes: Vec<parquet::file::page_index::index::PageIndex<T>>,
    format_value: F,
) -> impl IntoView
where
    T: Clone + 'static,
    F: Fn(&T) -> String + Copy + 'static,
{
    view! {
        <div class="space-y-2">
            {if !indexes.is_empty() {
                view! {
                    <div class="border border-gray-100 p-2">
                        <div class="grid grid-cols-[auto_1fr_1fr_auto] gap-4 text-gray-600">
                            <div>"#"</div>
                            <div>"Min"</div>
                            <div>"Max"</div>
                            <div>"Nulls"</div>
                        </div>
                        <div class="max-h-32 overflow-y-auto">
                            {indexes
                                .into_iter()
                                .enumerate()
                                .map(|(i, page_index)| {
                                    let min_str = page_index
                                        .min()
                                        .map(format_value)
                                        .unwrap_or_else(|| "-".to_string());
                                    let max_str = page_index
                                        .max()
                                        .map(format_value)
                                        .unwrap_or_else(|| "-".to_string());
                                    let null_count_str = page_index
                                        .null_count()
                                        .map(|n| n.to_string())
                                        .unwrap_or_else(|| "-".to_string());

                                    view! {
                                        <div class="py-1.5 last:border-b-0 hover:bg-gray-50">
                                            <div class="grid grid-cols-[auto_1fr_1fr_auto] gap-4">
                                                <div class="font-mono">{i + 1}</div>
                                                <div class="font-mono text-gray-700 break-all">
                                                    {min_str}
                                                </div>
                                                <div class="font-mono text-gray-700 break-all">
                                                    {max_str}
                                                </div>
                                                <div class="font-mono text-gray-600">{null_count_str}</div>
                                            </div>
                                        </div>
                                    }
                                })
                                .collect::<Vec<_>>()}
                        </div>
                    </div>
                }
                    .into_any()
            } else {
                view! { <div class="text-gray-500">"No page data available"</div> }.into_any()
            }}
        </div>
    }
}

#[component]
pub fn PageInfo(
    parquet_reader: Arc<ParquetResolved>,
    row_group_id: usize,
    column_id: usize,
) -> impl IntoView {
    let metadata = parquet_reader.metadata().metadata.clone();
    let byte_range = {
        let rg = metadata.row_group(row_group_id);
        let col = rg.column(column_id);
        col.byte_range()
    };
    let page_index = metadata
        .column_index()
        .and_then(|v| v.get(row_group_id).map(|v| v.get(column_id)))
        .flatten()
        .cloned();

    let page_info = LocalResource::new(move || {
        let mut column_reader = parquet_reader.reader().clone();
        let metadata = metadata.clone();
        async move {
            let bytes = column_reader
                .get_bytes(byte_range.0..(byte_range.0 + byte_range.1))
                .await
                .unwrap();

            let chunk = ColumnChunk {
                data: bytes,
                byte_range,
            };
            let rg = metadata.row_group(row_group_id);
            let col = rg.column(column_id);

            let page_reader =
                SerializedPageReader::new(Arc::new(chunk), col, rg.num_rows() as usize, None)
                    .unwrap();

            let mut page_info = Vec::new();
            for page in page_reader.flatten() {
                let page_type = page.page_type();
                let page_size = page.buffer().len() as u64;
                let num_values = page.num_values();
                page_info.push((page_type, page_size, num_values, page.encoding()));
            }

            page_info
        }
    });

    view! {
        <div class="col-span-2 space-y-4">
            <div class="space-y-2">
                <h4 class="text-gray-900">"Page info"</h4>
                <div class="border border-gray-100 p-2">
                    <div class="grid grid-cols-[1rem_7rem_4rem_4rem_1fr] gap-3 text-gray-600 mb-2">
                        <span>"#"</span>
                        <span>"Type"</span>
                        <span>"Size"</span>
                        <span>"Rows"</span>
                        <span>"Encoding"</span>
                    </div>
                    <Suspense fallback=move || {
                        view! {
                            <div class="flex justify-center items-center py-4">
                                <div class="text-gray-500">"Loading page info..."</div>
                            </div>
                        }
                    }>
                        <div class="max-h-32 overflow-y-auto space-y-1">
                            {move || Suspend::new(async move {
                                let page_info = page_info.await;
                                page_info
                                    .iter()
                                    .enumerate()
                                    .map(|(i, (page_type, size, values, encoding))| {
                                        view! {
                                            <div class="grid grid-cols-[1rem_7rem_4rem_4rem_1fr] gap-3 hover:bg-gray-50">
                                                <span>{format!("{i}")}</span>
                                                <span>{format!("{page_type:?}")}</span>
                                                <span>{format!(
                                                    "{:.0}",
                                                    Byte::from_u64(*size).get_appropriate_unit(UnitType::Binary)
                                                )}</span>
                                                <span>{format_rows(*values as u64)}</span>
                                                <span>{format!("{encoding:?}")}</span>
                                            </div>
                                        }
                                    })
                                    .collect::<Vec<_>>()
                            })}
                        </div>
                    </Suspense>
                </div>
            </div>

            <div class="space-y-2 ">
                <h4 class="text-gray-900">"Page stats"</h4>
                <div>
                    {if let Some(index) = page_index {
                        view! { <IndexDisplay index=index /> }.into_any()
                    } else {
                        ().into_any()
                    }}
                </div>
            </div>
        </div>
    }
}
