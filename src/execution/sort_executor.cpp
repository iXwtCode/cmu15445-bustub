#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  Tuple tup;
  RID rid;
  auto schema = plan->OutputSchema();
  child_executor->Init();
  while (child_executor->Next(&tup, &rid)) {
    out_tups_.emplace_back(tup);
  }

  std::function<bool(Tuple, Tuple)> cmpr = [&](const Tuple &lhs, const Tuple &rhs) -> bool {
    for (const auto &[type, expr] : plan->order_bys_) {
      auto left_val = expr->Evaluate(&lhs, plan->OutputSchema());
      auto right_val = expr->Evaluate(&rhs, plan->OutputSchema());
      if (left_val.CompareEquals(right_val) == CmpBool::CmpFalse) {
        if (type == OrderByType::ASC || type == OrderByType::DEFAULT) {
          return left_val.CompareLessThan(right_val) == CmpBool::CmpTrue;
        }
        if (type == OrderByType::DESC) {
          return left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue;
        }
      }
    }
    return false;
  };
  std::sort(out_tups_.begin(), out_tups_.end(), cmpr);
}

void SortExecutor::Init() { out_line_cnt_ = 0; }

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_line_cnt_ != out_tups_.size()) {
    *tuple = out_tups_[out_line_cnt_];
    *rid = tuple->GetRid();
    out_line_cnt_ += 1;
    return true;
  }
  return false;
}

}  // namespace bustub
