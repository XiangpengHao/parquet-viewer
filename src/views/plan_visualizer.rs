use std::sync::Arc;

use datafusion::physical_plan::{accept, display::DisplayableExecutionPlan, DisplayFormatType, ExecutionPlan, ExecutionPlanVisitor};
use leptos::{logging, prelude::*};

#[derive(Debug, Clone)]
struct PlanNode {
    _id: usize,
    name: String,
    label: String,
    metrics: Option<String>,
    children: Vec<PlanNode>,
}

struct TreeBuilder {
    next_id: usize,
    current_path: Vec<PlanNode>,
}

struct DisplayPlan<'a> {
    plan: &'a dyn ExecutionPlan,
}

impl std::fmt::Display for DisplayPlan<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.plan.fmt_as(DisplayFormatType::Default, f)
    }
}

impl ExecutionPlanVisitor for TreeBuilder {
    type Error = std::fmt::Error;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        let name = plan.name().to_string();
        let label = format!("{}", DisplayPlan { plan });

        let metrics = plan.metrics().map(|m| {
            let metrics = m
                .aggregate_by_name()
                .sorted_for_display()
                .timestamps_removed();
            format!("{metrics}")
        });

        let node = PlanNode {
            _id: self.next_id,
            name,
            label,
            metrics,
            children: vec![],
        };

        self.next_id += 1;
        self.current_path.push(node);
        Ok(true)
    }

    fn post_visit(&mut self, _: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        if self.current_path.len() >= 2 {
            let child = self.current_path.pop().unwrap();
            self.current_path.last_mut().unwrap().children.push(child);
        }
        Ok(true)
    }
}

#[component]
fn PlanNode(node: PlanNode) -> impl IntoView {
    view! {
        <div class="relative">
            <div class="flex flex-col items-center">
                <div class="p-4 border rounded-lg bg-white shadow-sm hover:shadow-md transition-shadow">
                    <div class="font-medium">{node.name}</div>
                    <div class="text-sm text-gray-700 mt-1 font-mono">{node.label}</div>
                    {node
                        .metrics
                        .map(|m| {
                            view! { <div class="text-sm text-blue-600 mt-1 italic">{m}</div> }
                        })}
                </div>

                {(!node.children.is_empty())
                    .then(|| {
                        view! {
                            <div class="relative pt-4">
                                <svg
                                    class="absolute top-0 left-1/2 -translate-x-[0.5px] h-4 w-1 z-10"
                                    overflow="visible"
                                >
                                    <line
                                        x1="0.5"
                                        y1="16"
                                        x2="0.5"
                                        y2="0"
                                        stroke="#D1D5DB"
                                        stroke-width="1"
                                        marker-end="url(#global-arrowhead)"
                                    />
                                </svg>

                                <div class="relative flex items-center justify-center">
                                    {(node.children.len() > 1)
                                        .then(|| {
                                            view! {
                                                <svg
                                                    class="absolute top-0 h-[1px]"
                                                    style="left: 25%; width: 50%;"
                                                    overflow="visible"
                                                >
                                                    <line
                                                        x1="0"
                                                        y1="0.5"
                                                        x2="100%"
                                                        y2="0.5"
                                                        stroke="#D1D5DB"
                                                        stroke-width="1"
                                                    />
                                                </svg>
                                            }
                                        })}
                                </div>

                                <div class="flex gap-8">
                                    {node
                                        .children
                                        .into_iter()
                                        .map(|child| view! { <PlanNode node=child /> })
                                        .collect::<Vec<_>>()}
                                </div>
                            </div>
                        }
                    })}
            </div>
        </div>
    }
    .into_any()
}

#[component]
pub fn PhysicalPlan(physical_plan: Arc<dyn ExecutionPlan>) -> impl IntoView {
    let mut builder = TreeBuilder {
        next_id: 0,
        current_path: vec![],
    };
    let displayable_plan = DisplayableExecutionPlan::with_metrics(physical_plan.as_ref());
    accept(physical_plan.as_ref(), &mut builder).unwrap();
    let root = builder.current_path.pop().unwrap();
    logging::log!("{}", displayable_plan.indent(true).to_string());

    view! {
        <div class="relative">
            <svg class="absolute" width="0" height="0">
                <defs>
                    <marker
                        id="global-arrowhead"
                        markerWidth="10"
                        markerHeight="7"
                        refX="9"
                        refY="3.5"
                        orient="auto"
                    >
                        <polygon points="0 0, 10 3.5, 0 7" fill="#D1D5DB" />
                    </marker>
                </defs>
            </svg>

            <div class="p-8 overflow-auto">
                <PlanNode node=root />
            </div>
        </div>
    }
}
