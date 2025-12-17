use std::sync::Arc;

use byte_unit::{Byte, UnitType};
use dioxus::prelude::*;
use parquet::file::page_index::index::Index;

use crate::{
    parquet_ctx::ParquetResolved,
    utils::{format_rows, get_column_chunk_page_info},
};
fn index_display(index: Index) -> Element {
    match index {
        Index::NONE => rsx! { div { class: "opacity-60", "No page index available" } },
        Index::BOOLEAN(native_index) => index_table(native_index.indexes, |v: &bool| v.to_string()),
        Index::INT32(native_index) => index_table(native_index.indexes, |v: &i32| v.to_string()),
        Index::INT64(native_index) => index_table(native_index.indexes, |v: &i64| v.to_string()),
        Index::INT96(native_index) => {
            index_table(native_index.indexes, |v: &parquet::data_type::Int96| {
                format!("{v:?}")
            })
        }
        Index::FLOAT(native_index) => {
            index_table(native_index.indexes, |v: &f32| format!("{v:.6}"))
        }
        Index::DOUBLE(native_index) => {
            index_table(native_index.indexes, |v: &f64| format!("{v:.6}"))
        }
        Index::BYTE_ARRAY(native_index) => {
            index_table(native_index.indexes, |v: &parquet::data_type::ByteArray| {
                String::from_utf8_lossy(v.data()).to_string()
            })
        }
        Index::FIXED_LEN_BYTE_ARRAY(native_index) => index_table(
            native_index.indexes,
            |v: &parquet::data_type::FixedLenByteArray| {
                String::from_utf8_lossy(v.data()).to_string()
            },
        ),
    }
}

fn index_table<T, F>(
    indexes: Vec<parquet::file::page_index::index::PageIndex<T>>,
    format_value: F,
) -> Element
where
    T: Clone + 'static,
    F: Fn(&T) -> String + Copy + 'static,
{
    rsx! {
        div { class: "space-y-2",
            if !indexes.is_empty() {
                div { class: "border border-gray-100 p-2",
                    div { class: "grid grid-cols-[auto_1fr_1fr_auto] gap-4 opacity-75",
                        div { "#" }
                        div { "Min" }
                        div { "Max" }
                        div { "Nulls" }
                    }
                    div { class: "max-h-32 overflow-y-auto",
                        for (i, page_index) in indexes.into_iter().enumerate() {
                            {
                                let min_str = page_index.min().map(format_value).unwrap_or_else(|| "-".to_string());
                                let max_str = page_index.max().map(format_value).unwrap_or_else(|| "-".to_string());
                                let null_count_str = page_index.null_count().map(|n| n.to_string()).unwrap_or_else(|| "-".to_string());
                                rsx! {
                                    div { class: "py-1.5 last:border-b-0 hover:bg-base-200",
                                        div { class: "grid grid-cols-[auto_1fr_1fr_auto] gap-4",
                                            div { class: "font-mono", "{i + 1}" }
                                            div { class: "font-mono text-base-content break-all", "{min_str}" }
                                            div { class: "font-mono text-base-content break-all", "{max_str}" }
                                            div { class: "font-mono opacity-75", "{null_count_str}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                div { class: "opacity-60", "No page data available" }
            }
        }
    }
}

#[component]
pub fn PageInfo(
    parquet_reader: Arc<ParquetResolved>,
    row_group_id: usize,
    column_id: usize,
) -> Element {
    let metadata = parquet_reader.metadata().metadata.clone();
    let page_index = metadata
        .column_index()
        .and_then(|v| v.get(row_group_id).map(|v| v.get(column_id)))
        .flatten()
        .cloned();

    let page_info = use_resource(move || {
        let mut column_reader = parquet_reader.reader().clone();
        let metadata = metadata.clone();
        async move {
            get_column_chunk_page_info(&mut column_reader, &metadata, row_group_id, column_id)
                .await
                .unwrap_or_default()
        }
    });

    rsx! {
        div { class: "col-span-2 space-y-4",
            div { class: "space-y-2",
                h4 { class: "font-semibold", "Page info" }
                div { class: "border border-gray-100 p-2",
                    div { class: "grid grid-cols-[1rem_7rem_4rem_4rem_1fr] gap-3 opacity-75 mb-2",
                        span { "#" }
                        span { "Type" }
                        span { "Size" }
                        span { "Rows" }
                        span { "Encoding" }
                    }
                    div { class: "max-h-32 overflow-y-auto space-y-1",
                        match (page_info.value())() {
                            Some(pages) => rsx! {
                                for (i, page) in pages.iter().enumerate() {
                                    div { class: "grid grid-cols-[1rem_7rem_4rem_4rem_1fr] gap-3 hover:bg-base-200",
                                        span { "{i}" }
                                        span { "{page.page_type:?}" }
                                        {
                                            let size = format!(
                                                "{:.0}",
                                                Byte::from_u64(page.size_bytes)
                                                    .get_appropriate_unit(UnitType::Binary)
                                            );
                                            rsx!(span { "{size}" })
                                        }
                                        span { "{format_rows(page.num_values as u64)}" }
                                        span { "{page.encoding:?}" }
                                    }
                                }
                            },
                            None => rsx! {
                                div { class: "flex justify-center items-center py-4",
                                    div { class: "opacity-60", "Loading page info..." }
                                }
                            },
                        }
                    }
                }
            }
            div { class: "space-y-2",
                h4 { class: "font-semibold", "Page stats" }
                if let Some(index) = page_index {
                    {index_display(index)}
                }
            }
        }
    }
}
