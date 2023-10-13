//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)) {}

void InsertExecutor::Init() {
  Tuple tup;
  RID rid;
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  num_inserted_ = 0;
  flag_ = false;
  child_executor_->Init();
  while (child_executor_->Next(&tup, &rid)) {
    TupleMeta meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    auto rid_insert = table_info_->table_->InsertTuple(meta, tup);

    // 更新 index
    if (rid_insert.has_value()) {
      num_inserted_ += 1;
      //      std::cout << num_inserted_ << std::endl;
      for (auto index : table_indexes) {
        index->index_->InsertEntry(
            tup.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
            rid_insert.value(), exec_ctx_->GetTransaction());
      }
    }
  }
  //  std::cout << "end" << std::endl;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (flag_) {
    return false;
  }
  std::vector<Value> vec;
  vec.emplace_back(TypeId::INTEGER, num_inserted_);
  *tuple = Tuple{vec, &GetOutputSchema()};
  *rid = tuple->GetRid();
  flag_ = true;
  return true;
}

}  // namespace bustub
