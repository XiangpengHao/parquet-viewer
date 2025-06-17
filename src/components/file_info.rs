use leptos::prelude::*;

use crate::utils::format_rows;

#[component]
pub fn FileLevelInfo(metadata_display: crate::parquet_ctx::MetadataDisplay) -> impl IntoView {
    let created_by = metadata_display
        .metadata
        .file_metadata()
        .created_by()
        .unwrap_or("Unknown")
        .to_string();
    let version = metadata_display.metadata.file_metadata().version();
    let has_bloom_filter = metadata_display.has_bloom_filter;
    let has_offset_index = metadata_display.has_offset_index;
    let has_column_index = metadata_display.has_column_index;
    let has_row_group_stats = metadata_display.has_row_group_stats;

    view! {
        <div class="mb-6">
            <div class="grid grid-cols-4 gap-x-6 gap-y-3 bg-gray-50 p-2 rounded-md mb-2">
                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"File size"</span>
                    <span class="block">
                        {format!("{:.2} MB", metadata_display.file_size as f64 / 1_048_576.0)}
                    </span>
                </div>
                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Compressed row groups"</span>
                    <span class="block">
                        {format!("{:.2} MB", metadata_display.compressed_row_group_size as f64 / 1_048_576.0)}
                    </span>
                </div>
                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Metadata size"</span>
                    <span class="block">
                        {format!("{:.2} KB", metadata_display.footer_size as f64 / 1024.0)}
                    </span>
                </div>
                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Metadata in memory size"</span>
                    <span class="block">
                        {format!("{:.2} KB", metadata_display.metadata_memory_size as f64 / 1024.0)}
                    </span>
                </div>
                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Uncompressed"</span>
                    <span class="block">
                        {format!(
                            "{:.2} MB",
                            metadata_display.uncompressed_size as f64 / 1_048_576.0,
                        )}
                    </span>
                </div>

                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Compression%"</span>
                    <span class="block">
                        {format!("{:.2}%", metadata_display.compression_ratio * 100.0)}
                    </span>
                </div>
                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Row groups"</span>
                    <span class="block">{metadata_display.row_group_count}</span>
                </div>
                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Total rows"</span>
                    <span class="block">{format_rows(metadata_display.row_count)}</span>
                </div>

                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Columns"</span>
                    <span class="block">{metadata_display.columns}</span>
                </div>
                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Created by"</span>
                    <span class="block">{created_by}</span>
                </div>
                <div class="space-y-1">
                    <span class="text-gray-400 text-xs">"Version"</span>
                    <span class="block">{version}</span>
                </div>
            </div>
            <div class="grid grid-cols-4 gap-2 text-xs">
                <div class="p-1 rounded border ".to_owned()
                    + if has_row_group_stats {
                        "border-green-200 text-green-700"
                    } else {
                        "border-gray-200 text-gray-600"
                    }>{if has_row_group_stats { "✓" } else { "✗" }} " Stats"</div>
                <div class="p-1 rounded border ".to_owned()
                    + if has_column_index {
                        "border-green-200 text-green-700"
                    } else {
                        "border-gray-200 text-gray-600"
                    }>{if has_column_index { "✓" } else { "✗" }} " Page stats"</div>
                <div class="p-1 rounded border ".to_owned()
                    + if has_offset_index {
                        "border-green-200 text-green-700"
                    } else {
                        "border-gray-200 text-gray-600"
                    }>{if has_offset_index { "✓" } else { "✗" }} " Page offsets"</div>
                <div class="p-1 rounded border ".to_owned()
                    + if has_bloom_filter {
                        "border-green-200 text-green-700"
                    } else {
                        "border-gray-200 text-gray-600"
                    }>{if has_bloom_filter { "✓" } else { "✗" }} " Bloom Filter"</div>
            </div>
        </div>
    }
}
