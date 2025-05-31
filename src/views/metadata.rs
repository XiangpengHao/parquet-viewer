use crate::{
    ParquetResolved,
    components::{FileLevelInfo, PageInfo, StatisticsDisplay},
};
use leptos::prelude::*;
use parquet::{basic::Compression, file::metadata::ParquetMetaData};
use std::sync::Arc;

use crate::utils::format_rows;

#[component]
pub fn MetadataView(parquet_reader: Arc<ParquetResolved>) -> impl IntoView {
    let metadata_display = parquet_reader.metadata().clone();
    let row_group_count = metadata_display.row_group_count;
    let (selected_row_group, set_selected_row_group) = signal(0);
    let (selected_column, set_selected_column) = signal(0);

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
        let rg = metadata_for_col.row_group(selected_row_group.get());
        let col = rg.column(selected_column.get());
        col.statistics().cloned()
    };
    let metadata_for_col = metadata_display.metadata.clone();

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-3 text-xs">
            <div class="flex items-center mb-2">
                <h2 class="text-gray-900">"Metadata"</h2>
                <a
                    href="https://parquet.apache.org/docs/file-format/metadata/"
                    target="_blank"
                    class="text-blue-500 hover:text-blue-700 text-xs ml-1"
                    title="Parquet Metadata Documentation"
                >
                    "(doc)"
                </a>
            </div>
            <div class="grid grid-cols-2 gap-6">
                <div>
                    <FileLevelInfo metadata_display=metadata_display.clone() />
                    {move || {
                        if row_group_count > 0u64 {
                            view! {
                                <div class="flex flex-row justify-between mt-2">
                                    <div>
                                        <div class="flex items-center mb-2">
                                            <label for="row-group-select" class="text-gray-700 w-32">
                                                "Row Group"
                                            </label>
                                            <select
                                                id="row-group-select"
                                                class="w-full bg-white text-gray-700 rounded-lg border border-gray-200 px-2 py-1 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer"
                                                on:change=move |ev| {
                                                    set_selected_row_group
                                                        .set(event_target_value(&ev).parse::<usize>().unwrap_or(0))
                                                }
                                            >
                                                {(0..row_group_count)
                                                    .map(|i| {
                                                        view! {
                                                            <option value=i.to_string() class="py-2">
                                                                {format!("{i}")}
                                                            </option>
                                                        }
                                                    })
                                                    .collect::<Vec<_>>()}
                                            </select>
                                        </div>
                                        <RowGroupInfo
                                            metadata=metadata_display.metadata.clone()
                                            row_group_id=selected_row_group.get()
                                        />
                                    </div>
                                    <div>
                                        <div class="flex items-center mb-2">
                                            <label for="column-select" class="text-gray-700 w-32">
                                                "Column"
                                            </label>
                                            <select
                                                id="column-select"
                                                class="w-full bg-white text-gray-700 rounded-lg border border-gray-200 px-2 py-1 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer "
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

                                        <ColumnInfo
                                            metadata=metadata_for_col.clone()
                                            row_group_id=selected_row_group.get()
                                            column_id=selected_column.get()
                                        />
                                    </div>
                                </div>
                            }
                                .into_any()
                        } else {
                            ().into_any()
                        }
                    }}

                </div>

                {move || {
                    if row_group_count > 0u64 {
                        view! {
                            <div class="flex flex-col space-y-2">
                                <div>
                                    <div class="text-gray-900">"Row Group stats"</div>
                                    <div>
                                        <StatisticsDisplay statistics=column_stats() />
                                    </div>
                                </div>
                                <div>
                                    <PageInfo
                                        parquet_reader=parquet_reader.clone()
                                        row_group_id=selected_row_group.get()
                                        column_id=selected_column.get()
                                    />
                                </div>
                            </div>
                        }
                            .into_any()
                    } else {
                        ().into_any()
                    }
                }}
            </div>
        </div>
    }
}

#[component]
fn RowGroupInfo(metadata: Arc<ParquetMetaData>, row_group_id: usize) -> impl IntoView {
    let row_group_info = move || {
        let rg = metadata.row_group(row_group_id);
        let compressed_size = rg.compressed_size() as f64 / 1_048_576.0;
        let uncompressed_size = rg.total_byte_size() as f64 / 1_048_576.0;
        let num_rows = rg.num_rows() as u64;
        (compressed_size, uncompressed_size, num_rows)
    };

    let (compressed_size, uncompressed_size, num_rows) = row_group_info();
    view! {
        <div class="grid grid-cols-2 gap-2 bg-gray-50 p-2 rounded-md">
            <div class="space-y-1">
                <div class="text-gray-500">"Compressed"</div>
                <div>{format!("{compressed_size:.2} MB")}</div>
            </div>
            <div class="space-y-1">
                <div class="text-gray-500">"Uncompressed"</div>
                <div>{format!("{uncompressed_size:.2} MB")}</div>
            </div>
            <div class="space-y-1">
                <div class="text-gray-500">"Compression%"</div>
                <div>{format!("{:.1}%", compressed_size / uncompressed_size * 100.0)}</div>
            </div>
            <div class="space-y-1">
                <div class="text-gray-500">"Rows"</div>
                <div>{format_rows(num_rows)}</div>
            </div>
        </div>
    }
}

#[derive(Clone)]
struct ColumnInfo {
    compressed_size: f64,
    uncompressed_size: f64,
    compression: Compression,
}

#[component]
pub fn ColumnInfo(
    metadata: Arc<ParquetMetaData>,
    row_group_id: usize,
    column_id: usize,
) -> impl IntoView {
    let column_info = {
        let metadata = metadata.clone();

        let rg = metadata.row_group(row_group_id);
        let col = rg.column(column_id);
        let compressed_size = col.compressed_size() as f64 / 1_048_576.0;
        let uncompressed_size = col.uncompressed_size() as f64 / 1_048_576.0;
        let compression = col.compression();

        ColumnInfo {
            compressed_size,
            uncompressed_size,
            compression,
        }
    };

    view! {
        <div class="space-y-8">
            // Column Selection
            <div class="flex flex-col space-y-2">

                <div class="grid grid-cols-2 gap-2 bg-gray-50 p-2 rounded-md">
                    <div class="space-y-1">
                        <div class="text-gray-500">"Compressed"</div>
                        <div>{format!("{:.2} MB", column_info.compressed_size)}</div>
                    </div>
                    <div class="space-y-1">
                        <div class="text-gray-500">"Uncompressed"</div>
                        <div>{format!("{:.2} MB", column_info.uncompressed_size)}</div>
                    </div>
                    <div class="space-y-1">
                        <div class="text-gray-500">"Compression%"</div>
                        <div>
                            {format!(
                                "{:.1}%",
                                column_info.compressed_size / column_info.uncompressed_size * 100.0,
                            )}
                        </div>
                    </div>
                    <div class="space-y-1">
                        <div class="text-gray-500">"Compression Type"</div>
                        <div>{format!("{:?}", column_info.compression)}</div>
                    </div>
                </div>

            </div>
        </div>
    }
}
