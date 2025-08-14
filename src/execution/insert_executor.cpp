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
<<<<<<< HEAD
  child_executor_->Init();
  flag_ = false;
  num_inserted_ = 0;
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  try {
    //    std::cout << "insert init try!" << std::endl;
    if (!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)) {
      throw ExecutionException("error: LockTale retrun fasle!\n");
=======
  num_inserted_ = 0;
  flag_ = false;
  child_executor_->Init();
  //  std::cout << "end" << std::endl;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (flag_) {
    return false;
  }
  Tuple tup;
  RID id;
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(&tup, &id)) {
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
>>>>>>> p4t2
    }
  } catch (TransactionAbortException &e) {
    //    std::cout << "insert: " << e.GetInfo() << std::endl;
    throw ExecutionException("failed to lock\n" + e.GetInfo());
  }
<<<<<<< HEAD
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (flag_) {
    return false;
  }

  Tuple tup;
  RID id;
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  auto write_set = txn->GetWriteSet();
  while (child_executor_->Next(&tup, &id)) {
    //    try {
    ////      locked_rids_.insert(id);
    //      lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, id);
    //    } catch (TransactionAbortException &e) {
    //      throw ExecutionException("error in insert excutor next\n" + e.GetInfo());
    //    }
    TupleMeta meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    auto rid_insert = table_info_->table_->InsertTuple(meta, tup, lock_manager, txn, table_info_->oid_);
    // 更新 index
    if (rid_insert.has_value()) {
      num_inserted_ += 1;
      TableWriteRecord record{table_info_->oid_, rid_insert.value(), table_info_->table_.get()};
      record.wtype_ = WType::INSERT;
      txn->AppendTableWriteRecord(record);
      for (auto index : table_indexes) {
        index->index_->InsertEntry(
            tup.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
            rid_insert.value(), exec_ctx_->GetTransaction());
        txn->AppendIndexWriteRecord(IndexWriteRecord{rid_insert.value(), table_info_->oid_, WType::INSERT, tup,
                                                     index->index_oid_, exec_ctx_->GetCatalog()});
      }
    }
  }

=======
>>>>>>> p4t2
  std::vector<Value> vec;
  vec.emplace_back(TypeId::INTEGER, num_inserted_);
  *tuple = Tuple{vec, &GetOutputSchema()};
  *rid = tuple->GetRid();
  flag_ = true;

  return true;
}

}  // namespace bustub
