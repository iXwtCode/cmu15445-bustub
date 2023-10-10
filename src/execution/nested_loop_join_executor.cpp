//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_ex_(std::move(left_executor)), right_ex_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_ex_->Init();
  right_ex_->Init();
  inner_status_ = false;
  has_any_item_ = true;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {

  auto left_schema = left_ex_->GetOutputSchema();
  auto right_schema = right_ex_->GetOutputSchema();
  auto out_schema = NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());

  while (true) {
    if (!inner_status_) {
      if (plan_->GetJoinType() == JoinType::LEFT && !has_any_item_) {
        has_any_item_ = true;
        std::vector<Value> out_vec;
        // 构建输出 tuple
        for(uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
          out_vec.emplace_back(left_tup_.GetValue(&left_schema, i));
        }
        for(uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
          auto type = right_schema.GetColumn(i).GetType();
          out_vec.emplace_back(ValueFactory::GetNullValueByType(type));
        }
        *tuple = Tuple(out_vec, &out_schema);
        *rid = tuple->GetRid();
        return true;
      }
      outer_status_ = left_ex_->Next(&left_tup_, &left_rid_);
      if(!outer_status_) { break; }
      right_ex_->Init();
      has_any_item_ = false;
    }

    inner_status_ = right_ex_->Next(&right_tup_, &right_rid_);
    if (!inner_status_) { continue; }

    auto status = plan_->predicate_->EvaluateJoin(&left_tup_, left_schema, &right_tup_, right_schema);
    if (status.GetAs<bool>()) {
      has_any_item_ = true;
      std::vector<Value> out_vec;
      // 构建输出 tuple
      for(uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        out_vec.emplace_back(left_tup_.GetValue(&left_schema, i));
      }
      for(uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        out_vec.emplace_back(right_tup_.GetValue(&right_schema, i));
      }
      *tuple = Tuple(out_vec, &out_schema);
      *rid = tuple->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
