use dioxus::prelude::*;

use crate::parquet_ctx::MetadataSummary;
use crate::utils::format_rows;
use byte_unit::{Byte, UnitType};

#[component]
pub fn FileLevelInfo(metadata_summary: MetadataSummary) -> Element {
    let created_by = metadata_summary
        .metadata
        .file_metadata()
        .created_by()
        .unwrap_or("Unknown")
        .to_string();
    let version = metadata_summary.metadata.file_metadata().version();
    let has_bloom_filter = metadata_summary.has_bloom_filter;
    let has_offset_index = metadata_summary.has_offset_index;
    let has_column_index = metadata_summary.has_column_index;
    let has_row_group_stats = metadata_summary.has_row_group_stats;

    let file_size = format!(
        "{:.2}",
        Byte::from_u64(metadata_summary.file_size).get_appropriate_unit(UnitType::Binary)
    );
    let compressed_row_groups = format!(
        "{:.2}",
        Byte::from_u64(metadata_summary.compressed_row_group_size)
            .get_appropriate_unit(UnitType::Binary)
    );
    let footer_size = format!(
        "{:.2}",
        Byte::from_u64(metadata_summary.footer_size).get_appropriate_unit(UnitType::Binary)
    );
    let metadata_memory_size = format!(
        "{:.2}",
        Byte::from_u64(metadata_summary.metadata_memory_size)
            .get_appropriate_unit(UnitType::Binary)
    );
    let bloom_filter_size = format!(
        "{:.2}",
        Byte::from_u64(metadata_summary.total_bloom_filter_size)
            .get_appropriate_unit(UnitType::Binary)
    );
    let uncompressed_size = format!(
        "{:.2}",
        Byte::from_u64(metadata_summary.uncompressed_size).get_appropriate_unit(UnitType::Binary)
    );
    let compression_pct = format!("{:.2}%", metadata_summary.compression_ratio * 100.0);

    let stats_class = if has_row_group_stats {
        "border-green-200 text-green-700"
    } else {
        "border-gray-200 text-gray-600"
    };
    let page_stats_class = if has_column_index {
        "border-green-200 text-green-700"
    } else {
        "border-gray-200 text-gray-600"
    };
    let page_offsets_class = if has_offset_index {
        "border-green-200 text-green-700"
    } else {
        "border-gray-200 text-gray-600"
    };
    let bloom_class = if has_bloom_filter {
        "border-green-200 text-green-700"
    } else {
        "border-gray-200 text-gray-600"
    };

    rsx! {
        div { class: "mb-6",
            div { class: "grid grid-cols-4 gap-x-6 gap-y-3 bg-gray-50 p-2 rounded-md mb-2",
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "File size" }
                    span { class: "block", "{file_size}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Compressed row groups" }
                    span { class: "block", "{compressed_row_groups}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Metadata size" }
                    span { class: "block", "{footer_size}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Metadata in memory size" }
                    span { class: "block", "{metadata_memory_size}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Bloom filter size" }
                    span { class: "block", "{bloom_filter_size}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Uncompressed" }
                    span { class: "block", "{uncompressed_size}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Compression%" }
                    span { class: "block", "{compression_pct}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Row groups" }
                    span { class: "block", "{metadata_summary.row_group_count}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Total rows" }
                    span { class: "block", "{format_rows(metadata_summary.row_count)}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Columns" }
                    span { class: "block", "{metadata_summary.columns}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Created by" }
                    span { class: "block", "{created_by}" }
                }
                div { class: "space-y-1",
                    span { class: "text-gray-400 text-xs", "Version" }
                    span { class: "block", "{version}" }
                }
            }

            div { class: "grid grid-cols-4 gap-2 text-xs",
                div { class: "p-1 rounded border {stats_class}",
                    if has_row_group_stats {
                        "✓"
                    } else {
                        "✗"
                    }
                    " Stats"
                }
                div { class: "p-1 rounded border {page_stats_class}",
                    if has_column_index {
                        "✓"
                    } else {
                        "✗"
                    }
                    " Page stats"
                }
                div { class: "p-1 rounded border {page_offsets_class}",
                    if has_offset_index {
                        "✓"
                    } else {
                        "✗"
                    }
                    " Page offsets"
                }
                div { class: "p-1 rounded border {bloom_class}",
                    if has_bloom_filter {
                        "✓"
                    } else {
                        "✗"
                    }
                    " Bloom Filter"
                }
            }
        }
    }
}
