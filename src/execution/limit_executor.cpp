//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  out_cnt_ = 0;
  child_executor_->Init();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_executor_->Next(tuple, rid) && out_cnt_ < plan_->limit_) {
    out_cnt_ += 1;
    return true;
  }
  return false;
}
}  // namespace bustub
