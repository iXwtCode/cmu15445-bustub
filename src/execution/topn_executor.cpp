#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  Tuple tup;
  RID rid;
  std::vector<std::pair<OrderByType, Value>> vec;
  child_executor_->Init();
  while (child_executor_->Next(&tup, &rid)) {
    if (GetNumInHeap() < plan->n_) {
      as_heap_.emplace_back(tup);
      std::push_heap(as_heap_.begin(), as_heap_.end(), cmpr_);
    } else if (cmpr_(tup, as_heap_[0])) {
      as_heap_[0] = tup;
      std::make_heap(as_heap_.begin(), as_heap_.end(), cmpr_);
    }
  }
}

void TopNExecutor::Init() {
  out_cnt_ = 0;
  std::make_heap(as_heap_.begin(), as_heap_.end(), cmpr1_);
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto limit = std::min(as_heap_.size(), plan_->n_);
  if (out_cnt_ < limit) {
    std::pop_heap(as_heap_.begin(), as_heap_.end() - out_cnt_, cmpr1_);
    *tuple = as_heap_[as_heap_.size() - out_cnt_ - 1];
    *rid = tuple->GetRid();
    out_cnt_ += 1;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return as_heap_.size(); }

}  // namespace bustub
// namespace bustub
