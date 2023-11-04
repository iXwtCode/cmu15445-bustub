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
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  auto table_oid = plan_->table_oid_;
  finished_ = false;
  std::cout << "seq init" << std::endl;
  try {
    if (exec_ctx_->IsDelete()) {
      std::cout << "IX table lock!" << std::endl;
      if(!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid)) {
        throw ExecutionException("seq_scan_executor: LockTable return false!\n");
      }
    } else if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED
               && !txn->IsTableIntentionExclusiveLocked(table_oid)) {
      std::cout << "IS table lock!" << std::endl;
      if (!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_oid)) {
        throw ExecutionException("seq_scan_executor: LockTable return false!\n");
      }
    }

  } catch (TransactionAbortException &e) {
    std::cout << "seq init" << std::endl;
    throw ExecutionException("seq_scan lock error \n" + e.GetInfo());
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(table_oid);
  p_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (finished_) { return false; }
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  auto table_oid = plan_->GetTableOid();
  while (!p_iterator_->IsEnd()) {
    auto id = p_iterator_->GetRID();
    try {
      if (exec_ctx_->IsDelete()) {
        if (!lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_oid, id)) {
          throw ExecutionException("seq_scan_executor: LockRow return false!\n");
        }
      } else if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED
                 && !txn->IsRowExclusiveLocked(table_oid, id)) {
        if(!lock_manager->LockRow(txn, LockManager::LockMode::SHARED, table_oid, id)) {
          throw ExecutionException("seq_scan_executor: LockRow return false!\n");
        }
      }

      auto [meta, tup] = p_iterator_->GetTuple();
      if (meta.is_deleted_) {
        lock_manager->UnlockRow(txn, table_oid, tup.GetRid(), true);
      } else {
        *tuple = tup;
        *rid = tup.GetRid();
        p_iterator_->operator++();
        if (!exec_ctx_->IsDelete()&& txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED
            && !txn->IsRowExclusiveLocked(table_oid, *rid) ) {
          lock_manager->UnlockRow(txn, table_oid, *rid);
        }
        return true;
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("seq_scan_executor: in next" + e.GetInfo());
    }
    p_iterator_->operator++();
  }
  finished_ = true;
  return false;
}

}  // namespace bustub
