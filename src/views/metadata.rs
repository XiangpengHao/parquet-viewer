use crate::{
    ParquetResolved,
    components::{
        FileLevelInfo, PageInfo, StatisticsDisplay,
        ui::{Panel, SectionHeader},
    },
    utils::count_column_chunk_pages,
};
use byte_unit::{Byte, UnitType};
use dioxus::prelude::*;
use parquet::{basic::Compression, file::metadata::ParquetMetaData};
use std::sync::Arc;

use crate::utils::format_rows;

/// Mirror `Compression::codec_to_string` from `arrow-rs` so we can keep parity with the
/// formatting used by upstream metadata printing helpers.
trait CompressionExt {
    fn codec_to_string(self) -> &'static str;
}

impl CompressionExt for Compression {
    fn codec_to_string(self) -> &'static str {
        match self {
            Compression::UNCOMPRESSED => "UNCOMPRESSED",
            Compression::SNAPPY => "SNAPPY",
            Compression::GZIP(_) => "GZIP",
            Compression::LZO => "LZO",
            Compression::BROTLI(_) => "BROTLI",
            Compression::LZ4 => "LZ4",
            Compression::ZSTD(_) => "ZSTD",
            Compression::LZ4_RAW => "LZ4_RAW",
        }
    }
}

#[component]
pub fn MetadataView(parquet_reader: Arc<ParquetResolved>) -> Element {
    let metadata_display = parquet_reader.metadata().clone();
    let row_group_count = metadata_display.row_group_count;
    let mut selected_row_group = use_signal(|| 0usize);
    let mut selected_column = use_signal(|| 0usize);

    let sorted_fields = {
        let mut fields = metadata_display
            .schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (i, f.name().to_string()))
            .collect::<Vec<_>>();

        fields.sort_by(|a, b| a.1.as_str().cmp(b.1.as_str()));
        fields
    };

    let metadata_for_col = metadata_display.metadata.clone();
    let column_stats = move || {
        let rg = metadata_for_col.row_group(selected_row_group());
        let col = rg.column(selected_column());
        col.statistics().cloned()
    };

    let reader_for_column_info = parquet_reader.clone();
    let reader_for_page_info = parquet_reader.clone();

    rsx! {
        Panel { class: Some("rounded-lg p-3 text-xs".to_string()),
            SectionHeader {
                title: "Metadata".to_string(),
                subtitle: None,
                class: Some("mb-1".to_string()),
                trailing: Some(rsx! {
                    a {
                        href: "https://parquet.apache.org/docs/file-format/metadata/",
                        target: "_blank",
                        class: "text-blue-500 hover:text-blue-700 text-xs ml-1",
                        title: "Parquet Metadata Documentation",
                        "(doc)"
                    }
                })
            }
            div { class: "grid gap-6 lg:grid-cols-2",
                div {
                    FileLevelInfo { metadata_display: metadata_display.clone() }
                    if row_group_count > 0 {
                        div { class: "mt-2 flex flex-col gap-4 md:flex-row md:justify-between",
                            div {
                                div { class: "flex items-center mb-2",
                                    label { r#for: "row-group-select", class: "text-gray-700 w-32", "Row Group" }
                                    select {
                                        id: "row-group-select",
                                        class: "w-full bg-white text-gray-700 rounded-lg border border-gray-200 px-2 py-1 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer",
                                        onchange: move |ev| selected_row_group.set(ev.value().parse::<usize>().unwrap_or(0)),
                                        for i in 0..row_group_count {
                                            option { value: "{i}", class: "py-2", "{i}" }
                                        }
                                    }
                                }
                                RowGroupInfo { metadata: metadata_display.metadata.clone(), row_group_id: selected_row_group() }
                            }
                            div {
                                div { class: "flex items-center mb-2",
                                    label { r#for: "column-select", class: "text-gray-700 w-32", "Column" }
                                    select {
                                        id: "column-select",
                                        class: "w-full bg-white text-gray-700 rounded-lg border border-gray-200 px-2 py-1 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer",
                                        onchange: move |ev| selected_column.set(ev.value().parse::<usize>().unwrap_or(0)),
                                        for (i, field) in sorted_fields.iter() {
                                            option { value: "{i}", class: "py-2", "{field}" }
                                        }
                                    }
                                }
                                ColumnInfo { parquet_reader: reader_for_column_info.clone(), row_group_id: selected_row_group(), column_id: selected_column() }
                            }
                        }
                    }
                }
                if row_group_count > 0 {
                    div { class: "flex flex-col space-y-2",
                        div {
                            div { class: "text-gray-900", "Row Group stats" }
                            StatisticsDisplay { statistics: column_stats() }
                        }
                        PageInfo { parquet_reader: reader_for_page_info.clone(), row_group_id: selected_row_group(), column_id: selected_column() }
                    }
                }
            }
        }
    }
}

#[component]
fn RowGroupInfo(metadata: Arc<ParquetMetaData>, row_group_id: usize) -> Element {
    let row_group_info = move || {
        let rg = metadata.row_group(row_group_id);
        let compressed_size = rg.compressed_size() as u64;
        let uncompressed_size = rg.total_byte_size() as u64;
        let num_rows = rg.num_rows() as u64;
        (compressed_size, uncompressed_size, num_rows)
    };

    let (compressed_size, uncompressed_size, num_rows) = row_group_info();
    rsx! {
        div { class: "grid grid-cols-2 gap-2 bg-gray-50 p-2 rounded-md",
            div { class: "space-y-1",
                div { class: "text-gray-500", "Compressed" }
                div { "{Byte::from_u64(compressed_size).get_appropriate_unit(UnitType::Binary):.2}" }
            }
            div { class: "space-y-1",
                div { class: "text-gray-500", "Uncompressed" }
                div { "{Byte::from_u64(uncompressed_size).get_appropriate_unit(UnitType::Binary):.2}" }
            }
            div { class: "space-y-1",
                div { class: "text-gray-500", "Compression%" }
                div { "{compressed_size as f64 / uncompressed_size as f64 * 100.0:.1}%" }
            }
            div { class: "space-y-1",
                div { class: "text-gray-500", "Rows" }
                div { "{format_rows(num_rows)}" }
            }
        }
    }
}

#[derive(Clone)]
struct ColumnInfoData {
    compressed_size: u64,
    uncompressed_size: u64,
    compression: Compression,
}

#[component]
pub fn ColumnInfo(
    parquet_reader: Arc<ParquetResolved>,
    row_group_id: usize,
    column_id: usize,
) -> Element {
    let metadata = parquet_reader.metadata().metadata.clone();

    let column_info = {
        let rg = metadata.row_group(row_group_id);
        let col = rg.column(column_id);
        let compressed_size = col.compressed_size() as u64;
        let uncompressed_size = col.uncompressed_size() as u64;
        let compression = col.compression();

        ColumnInfoData {
            compressed_size,
            uncompressed_size,
            compression,
        }
    };

    let page_count = use_resource(move || {
        let mut column_reader = parquet_reader.reader().clone();
        let metadata = metadata.clone();
        async move {
            count_column_chunk_pages(&mut column_reader, &metadata, row_group_id, column_id)
                .await
                .unwrap_or_default()
        }
    });

    let page_count_text = match (page_count.value())() {
        Some(value) => value.to_string(),
        None => "...".to_string(),
    };

    rsx! {
        div { class: "space-y-8",
            div { class: "flex flex-col space-y-2",
                div { class: "grid grid-cols-3 gap-2 bg-gray-50 p-2 rounded-md",
                    div { class: "space-y-1",
                        div { class: "text-gray-500", "Compressed" }
                        div { "{Byte::from_u64(column_info.compressed_size).get_appropriate_unit(UnitType::Binary):.2}" }
                    }
                    div { class: "space-y-1",
                        div { class: "text-gray-500", "Uncompressed" }
                        div { "{Byte::from_u64(column_info.uncompressed_size).get_appropriate_unit(UnitType::Binary):.2}" }
                    }
                    div { class: "space-y-1",
                        div { class: "text-gray-500", "Compression%" }
                        div { "{column_info.compressed_size as f64 / column_info.uncompressed_size as f64 * 100.0:.1}%" }
                    }
                    div { class: "space-y-1",
                        div { class: "text-gray-500", "CompressionType" }
                        div { "{column_info.compression.codec_to_string()}" }
                    }
                    div { class: "space-y-1",
                        div { class: "text-gray-500", "Pages" }
                        div { class: "text-gray-700", "{page_count_text}" }
                    }
                }
            }
        }
    }
}
