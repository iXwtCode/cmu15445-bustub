//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))
      ,table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)) {}

void DeleteExecutor::Init() {
  Tuple tup;
  RID rid;
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  child_executor_->Init();
  flag_ = false;
  delete_cnt_ = 0;
  while (child_executor_->Next(&tup, &rid)) {
    // 从 table 中删除 tuple
    TupleMeta meta {INVALID_TXN_ID, INVALID_TXN_ID, true};
    table_info_->table_->UpdateTupleMeta(meta, rid);
    delete_cnt_ += 1;

    // 删除对应索引
    for(auto index : table_indexes) {
      index->index_->DeleteEntry(
          tup.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          tup.GetRid(),
          exec_ctx_->GetTransaction()
      );
    }
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!flag_) {
    std::vector<Value> vec;
    vec.emplace_back(TypeId::INTEGER, delete_cnt_);
    *tuple = Tuple(vec, &GetOutputSchema());
    *rid = tuple->GetRid();
    flag_ = true;
    return true;
  }
  return false;
}
}  // namespace bustub
