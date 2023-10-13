//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  Tuple tup;
  RID rid;
  child_executor_->Init();
  AggregateKey agg_key{{ValueFactory::GetIntegerValue(0)}};
  AggregateValue agg_val{{ValueFactory::GetIntegerValue(0)}};
  while (child_executor_->Next(&tup, &rid)) {
    agg_key = MakeAggregateKey(&tup);
    agg_val = MakeAggregateValue(&tup);
    aht_.InsertCombine(agg_key, agg_val);
  }
}

void AggregationExecutor::Init() {
  aht_iterator_ = aht_.Begin();
  terminated_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto schema = AggregationPlanNode::InferAggSchema(plan_->group_bys_, plan_->aggregates_, plan_->agg_types_);
  AggregateKey key;
  AggregateValue val;
  if (aht_.IsEmpty() && !plan_->group_bys_.empty()) {
    return false;
  }

  if (aht_.IsEmpty() && !terminated_) {
    std::vector<Value> out_vec;
    key = MakeAggregateNullKey();
    val = aht_.GenerateInitialAggregateValue();
    out_vec.reserve(key.group_bys_.size() + val.aggregates_.size());
    for (const auto &out : key.group_bys_) {
      out_vec.emplace_back(out);
    }
    for (const auto &out : val.aggregates_) {
      out_vec.emplace_back(out);
    }
    terminated_ = true;
    *tuple = Tuple(out_vec, &schema);
    *rid = tuple->GetRid();
    return true;
  }

  if (aht_iterator_ != aht_.End()) {
    //    std::cout << "not empty!\n";
    key = aht_iterator_.Key();
    val = aht_iterator_.Val();
    ++aht_iterator_;

    // 构建输出 tuple
    std::vector<Value> out_vec;
    out_vec.reserve(key.group_bys_.size() + val.aggregates_.size());
    for (const auto &out : key.group_bys_) {
      out_vec.emplace_back(out);
    }
    for (const auto &out : val.aggregates_) {
      out_vec.emplace_back(out);
    }

    *tuple = Tuple(out_vec, &schema);
    *rid = tuple->GetRid();
    return true;
  }

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
