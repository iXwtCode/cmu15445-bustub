//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_oid = plan_->table_oid_;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(table_oid);
  p_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!p_iterator_->IsEnd()) {
    auto [meta, tup] = p_iterator_->GetTuple();
    p_iterator_->operator++();
    if (!meta.is_deleted_) {
      *tuple = tup;
      *rid = tup.GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
