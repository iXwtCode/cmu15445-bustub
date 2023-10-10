#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
void getExpression(std::vector<AbstractExpressionRef> &left_expr,  std::vector<AbstractExpressionRef> &right_expr
                   , AbstractExpressionRef expr) {
  auto ex = dynamic_cast<ComparisonExpression *>(expr.get());
  if (ex != nullptr) {
    for (const auto& e: ex->GetChildren()) {
      auto col_expr = dynamic_cast<ColumnValueExpression *>(e.get());
      if (col_expr->GetTupleIdx() == 0) {
        left_expr.emplace_back(e);
      } else {
        right_expr.emplace_back(e);
      }
    }
  } else {
    for (const auto& child : expr->GetChildren()) {
      getExpression(left_expr, right_expr, child);
    }
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto copy_plan = plan->CloneWithChildren(children);
  if (copy_plan->GetType() == PlanType::NestedLoopJoin) {
    auto nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*copy_plan);
    auto left_plan = nlj_plan.GetLeftPlan();
    auto right_plan = nlj_plan.GetRightPlan();
    auto left_schema = left_plan->OutputSchema();
    auto right_schema = right_plan->OutputSchema();
    std::vector<AbstractExpressionRef> left_expr;
    std::vector<AbstractExpressionRef> right_expr;
    getExpression(left_expr, right_expr, nlj_plan.predicate_);

    auto schema = std::make_shared<Schema>(nlj_plan.OutputSchema());
    auto opti_plan = HashJoinPlanNode(schema, nlj_plan.GetChildAt(0), nlj_plan.GetChildAt(1)
                                                                          , left_expr, right_expr, nlj_plan.GetJoinType());
    return std::make_shared<HashJoinPlanNode>(opti_plan);
  }
  return copy_plan;
}

}  // namespace bustub
