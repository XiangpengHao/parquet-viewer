use arrow::array::PrimitiveArray;
use arrow::compute::{SortOptions, sort_to_indices};
use arrow::datatypes::UInt32Type;
use arrow_array::RecordBatch;
use leptos::prelude::*;

use crate::views::query_results::ArrayExt;

pub type RecordFormatter = Box<dyn Fn(&RecordBatch, (usize, usize)) -> AnyView + Send + Sync>;

#[component]
pub fn RecordBatchTable(
    data: RecordBatch,
    formatter: Vec<Option<RecordFormatter>>,
) -> impl IntoView {
    let column_names = data
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect::<Vec<_>>();

    let (sort_column, set_sort_column) = signal(None::<usize>);
    let (sort_ascending, set_sort_ascending) = signal(true);

    let data_clone = data.clone();
    let sorted_data = Memo::new(move |_| {
        if let Some(col_idx) = sort_column.get() {
            if col_idx < data_clone.num_columns() {
                let sort_col = data_clone.column(col_idx);

                let options = SortOptions {
                    descending: !sort_ascending.get(),
                    nulls_first: false,
                };

                let indices = sort_to_indices(sort_col.as_ref(), Some(options), None)
                    .unwrap_or_else(|_| {
                        PrimitiveArray::<UInt32Type>::from_iter_values(
                            0..data_clone.num_rows() as u32,
                        )
                    });

                return indices;
            }
        }

        PrimitiveArray::<UInt32Type>::from_iter_values(0..data_clone.num_rows() as u32)
    });

    let handle_column_click = move |col_idx: usize| {
        set_sort_column.update(|current| {
            if *current == Some(col_idx) {
                set_sort_ascending.update(|asc| *asc = !*asc);
            } else {
                set_sort_ascending.set(true);
            }
            *current = Some(col_idx);
        });
    };

    let data_clone = data.clone();

    view! {
        <table class="w-full border-collapse text-sm">
            <thead>
                <tr>
                    {move || {
                        column_names
                            .iter()
                            .enumerate()
                            .map(|(i, name)| {
                                let is_sorted = sort_column.get() == Some(i);
                                let direction_icon = if is_sorted {
                                    if sort_ascending.get() { "↑" } else { "↓" }
                                } else {
                                    ""
                                };

                                view! {
                                    <th
                                        class="text-left px-3 py-1 border-b border-gray-200 bg-gray-50 cursor-pointer hover:bg-gray-100"
                                        on:click=move |_| handle_column_click(i)
                                    >
                                        <span>{name.clone()}</span>
                                        <span class="ml-1">{direction_icon}</span>
                                    </th>
                                }
                            })
                            .collect::<Vec<_>>()
                    }}
                </tr>
            </thead>
            <tbody>
                {move || {
                    let indices = sorted_data.get();
                    (0..indices.len())
                        .map(|idx| {
                            let row_idx = indices.value(idx) as usize;

                            view! {
                                <tr class="hover:bg-gray-50 border-b border-gray-100">
                                    {(0..data_clone.num_columns())
                                        .zip(formatter.iter())
                                        .map(|(col_idx, formatter)| {
                                            let col = data_clone.column(col_idx);
                                            match formatter {
                                                Some(formatter) => {
                                                    let cell_value = formatter(&data_clone, (col_idx, row_idx));
                                                    view! { <td class="px-3 py-1">{cell_value}</td> }.into_any()
                                                }
                                                None => {
                                                    let cell_value = col.as_ref().value_to_string(row_idx);
                                                    view! { <td class="px-3 py-1">{cell_value}</td> }.into_any()
                                                }
                                            }
                                        })
                                        .collect::<Vec<_>>()}
                                </tr>
                            }
                        })
                        .collect::<Vec<_>>()
                }}
            </tbody>
        </table>
    }
}
