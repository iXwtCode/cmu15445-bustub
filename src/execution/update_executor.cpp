//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)),
      child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  Tuple tup;
  RID rid;
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  child_executor_->Init();
  flag_ = false;
  updated_cnt_ = 0;
  while (child_executor_->Next(&tup, &rid)) {
    // 从 table 中删除 tuple
    TupleMeta meta {INVALID_TXN_ID, INVALID_TXN_ID, true};
    table_info_->table_->UpdateTupleMeta(meta, rid);
    updated_cnt_ += 1;

    // 计算新 tuple
    std::vector<Value> vec;
    for(const auto& expr : plan_->target_expressions_) {
      auto val = expr->Evaluate(&tup, table_info_->schema_);
      vec.emplace_back(val);
    }
    Tuple insert_tup(vec, &table_info_->schema_);

    // 新 tuple 插入 table
    TupleMeta new_meta {INVALID_TXN_ID, INVALID_TXN_ID, false};
    table_info_->table_->InsertTuple(new_meta, insert_tup);

    // 从索引中删除旧值，插入新值
    for(auto index : table_indexes) {
      index->index_->DeleteEntry(
          tup.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          tup.GetRid(),
          exec_ctx_->GetTransaction()
      );
      index->index_->InsertEntry(
          insert_tup.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          insert_tup.GetRid(),
          exec_ctx_->GetTransaction()
      );
    }
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!flag_) {
    std::vector<Value> vec;
    vec.emplace_back(TypeId::INTEGER, updated_cnt_);
    *tuple = Tuple(vec, &GetOutputSchema());
    *rid = tuple->GetRid();
    flag_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
