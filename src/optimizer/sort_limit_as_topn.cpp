#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto copy_plan = plan->CloneWithChildren(children);
  auto child = [&]() -> std::optional<AbstractPlanNodeRef> {
    if (copy_plan->GetChildren().size() == 1) {
      return copy_plan->GetChildAt(0);
    }
    return std::nullopt;
  }();
  if (child.has_value() && plan->GetType() == PlanType::Limit && child.value()->GetType() == PlanType::Sort) {
    auto new_child = child.value()->GetChildAt(0);
    auto limit_plan = dynamic_cast<const LimitPlanNode &>(*copy_plan);
    auto sort_plan = dynamic_cast<const SortPlanNode &>(*child.value());
    auto schema = std::make_shared<Schema>(child.value()->OutputSchema());
    TopNPlanNode new_plan(schema, new_child, sort_plan.order_bys_, limit_plan.limit_);
    return std::make_shared<TopNPlanNode>(new_plan);
  }
  return copy_plan;
}

}  // namespace bustub
