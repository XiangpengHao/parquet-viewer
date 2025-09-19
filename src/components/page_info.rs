use std::sync::Arc;

use byte_unit::{Byte, UnitType};
use leptos::prelude::*;
use parquet::file::page_index::index::Index;

use crate::{
    parquet_ctx::ParquetResolved,
    utils::{format_rows, get_column_chunk_page_info},
};
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
    let page_index = metadata
        .column_index()
        .and_then(|v| v.get(row_group_id).map(|v| v.get(column_id)))
        .flatten()
        .cloned();

    let page_info = LocalResource::new(move || {
        let mut column_reader = parquet_reader.reader().clone();
        let metadata = metadata.clone();
        async move {
            match get_column_chunk_page_info(&mut column_reader, &metadata, row_group_id, column_id).await {
                Ok(pages) => pages,
                Err(_) => Vec::new(),
            }
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
                                let pages = page_info.await;
                                pages
                                    .iter()
                                    .enumerate()
                                    .map(|(i, page)| {
                                        view! {
                                            <div class="grid grid-cols-[1rem_7rem_4rem_4rem_1fr] gap-3 hover:bg-gray-50">
                                                <span>{format!("{i}")}</span>
                                                <span>{format!("{:?}", page.page_type)}</span>
                                                <span>{format!(
                                                    "{:.0}",
                                                    Byte::from_u64(page.size_bytes).get_appropriate_unit(UnitType::Binary)
                                                )}</span>
                                                <span>{format_rows(page.num_values as u64)}</span>
                                                <span>{format!("{:?}", page.encoding)}</span>
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
