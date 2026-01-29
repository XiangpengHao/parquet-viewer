use std::sync::Arc;

use byte_unit::{Byte, UnitType};
use dioxus::prelude::*;
use parquet::file::page_index::column_index::{
    ByteArrayColumnIndex, ColumnIndexMetaData, PrimitiveColumnIndex,
};

use crate::{
    parquet_ctx::ParquetResolved,
    utils::{format_rows, get_column_chunk_page_info},
};
fn index_display(index: ColumnIndexMetaData) -> Element {
    match index {
        ColumnIndexMetaData::NONE => rsx! {
            div { class: "opacity-60", "No page index available" }
        },
        ColumnIndexMetaData::BOOLEAN(native_index) => {
            primitive_index_table(native_index, |v: &bool| v.to_string())
        }
        ColumnIndexMetaData::INT32(native_index) => {
            primitive_index_table(native_index, |v: &i32| v.to_string())
        }
        ColumnIndexMetaData::INT64(native_index) => {
            primitive_index_table(native_index, |v: &i64| v.to_string())
        }
        ColumnIndexMetaData::INT96(native_index) => {
            primitive_index_table(native_index, |v: &parquet::data_type::Int96| {
                format!("{v:?}")
            })
        }
        ColumnIndexMetaData::FLOAT(native_index) => {
            primitive_index_table(native_index, |v: &f32| format!("{v:.6}"))
        }
        ColumnIndexMetaData::DOUBLE(native_index) => {
            primitive_index_table(native_index, |v: &f64| format!("{v:.6}"))
        }
        ColumnIndexMetaData::BYTE_ARRAY(native_index) => byte_array_index_table(native_index),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(native_index) => {
            byte_array_index_table(native_index)
        }
    }
}

fn primitive_index_table<T, F>(index: PrimitiveColumnIndex<T>, format_value: F) -> Element
where
    T: Clone + 'static,
    F: Fn(&T) -> String + Copy + 'static,
{
    let num_pages = index.num_pages() as usize;
    let min_values: Vec<_> = index.min_values_iter().collect();
    let max_values: Vec<_> = index.max_values_iter().collect();

    rsx! {
        div { class: "space-y-2",
            if num_pages > 0 {
                div { class: "border border-gray-100 p-2",
                    div { class: "grid grid-cols-[auto_1fr_1fr_auto] gap-4 opacity-75",
                        div { "#" }
                        div { "Min" }
                        div { "Max" }
                        div { "Nulls" }
                    }
                    div { class: "max-h-32 overflow-y-auto",
                        for i in 0..num_pages {
                            {
                                let min_str = min_values[i].map(format_value).unwrap_or_else(|| "-".to_string());
                                let max_str = max_values[i].map(format_value).unwrap_or_else(|| "-".to_string());
                                let null_count_str = index
                                    .null_count(i)
                                    .map(|n| n.to_string())
                                    .unwrap_or_else(|| "-".to_string());
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

fn byte_array_index_table(index: ByteArrayColumnIndex) -> Element {
    let num_pages = index.num_pages() as usize;

    rsx! {
        div { class: "space-y-2",
            if num_pages > 0 {
                div { class: "border border-gray-100 p-2",
                    div { class: "grid grid-cols-[auto_1fr_1fr_auto] gap-4 opacity-75",
                        div { "#" }
                        div { "Min" }
                        div { "Max" }
                        div { "Nulls" }
                    }
                    div { class: "max-h-32 overflow-y-auto",
                        for i in 0..num_pages {
                            {
                                let min_str = index
                                    .min_value(i)
                                    .map(|v| String::from_utf8_lossy(v).to_string())
                                    .unwrap_or_else(|| "-".to_string());
                                let max_str = index
                                    .max_value(i)
                                    .map(|v| String::from_utf8_lossy(v).to_string())
                                    .unwrap_or_else(|| "-".to_string());
                                let null_count_str = index
                                    .null_count(i)
                                    .map(|n| n.to_string())
                                    .unwrap_or_else(|| "-".to_string());
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
    row_group_id: ReadSignal<usize>,
    column_id: ReadSignal<usize>,
) -> Element {
    let metadata = parquet_reader.metadata().metadata.clone();
    let row_group_id_value = row_group_id();
    let column_id_value = column_id();
    let page_index = metadata
        .column_index()
        .and_then(|v| v.get(row_group_id_value).map(|v| v.get(column_id_value)))
        .flatten()
        .cloned();

    let page_info = use_resource(move || {
        let mut column_reader = parquet_reader.reader().clone();
        let metadata = metadata.clone();
        async move {
            get_column_chunk_page_info(
                &mut column_reader,
                &metadata,
                row_group_id(),
                column_id(),
            )
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
                                for (i , page) in pages.iter().enumerate() {
                                    div { class: "grid grid-cols-[1rem_7rem_4rem_4rem_1fr] gap-3 hover:bg-base-200",
                                        span { "{i}" }
                                        span { "{page.page_type:?}" }
                                        {
                                            let size = format!(
                                                "{:.0}",
                                                Byte::from_u64(page.size_bytes).get_appropriate_unit(UnitType::Binary),
                                            );
                                            rsx! {
                                                span { "{size}" }
                                            }
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
