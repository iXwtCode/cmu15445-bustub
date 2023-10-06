//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan)
      , index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_))
      , table_info_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_))
      , tree_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get()))
      , iterator_(tree_->GetBeginIterator()) {}


void IndexScanExecutor::Init() {
  iterator_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iterator_.IsEnd()) {
    auto id = (*iterator_).second;
    auto [meta, tup] = table_info_->table_->GetTuple(id);
    ++iterator_;
    if (!meta.is_deleted_) {
      *tuple = tup;
      *rid = id;
      return true;
    }
  }
  return false;
}
}  // namespace bustub
