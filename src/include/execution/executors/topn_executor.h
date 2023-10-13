//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;
  //  static const TopNPlanNode *plan_;
 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::function<bool(Tuple, Tuple)> cmpr_ = [&](const Tuple &lhs, const Tuple &rhs) -> bool {
    for (const auto &[type, expr] : plan_->order_bys_) {
      auto left_val = expr->Evaluate(&lhs, plan_->OutputSchema());
      auto right_val = expr->Evaluate(&rhs, plan_->OutputSchema());
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
  std::function<bool(Tuple, Tuple)> cmpr1_ = [&](const Tuple &lhs, const Tuple &rhs) -> bool {
    for (const auto &[type, expr] : plan_->order_bys_) {
      auto left_val = expr->Evaluate(&lhs, plan_->OutputSchema());
      auto right_val = expr->Evaluate(&rhs, plan_->OutputSchema());
      if (left_val.CompareEquals(right_val) == CmpBool::CmpFalse) {
        if (type == OrderByType::ASC || type == OrderByType::DEFAULT) {
          return left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue;
        }
        if (type == OrderByType::DESC) {
          return left_val.CompareLessThan(right_val) == CmpBool::CmpTrue;
        }
      }
    }
    return true;
  };
  std::vector<Tuple> as_heap_;
  size_t out_cnt_{0};
};
}  // namespace bustub

namespace std {}
