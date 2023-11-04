//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"

namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
  txn->UnlockTxn();
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto table_write_set = txn->GetWriteSet();
  auto index_write_set = txn->GetIndexWriteSet();
  for (auto &[tid, rid, table_heap, wtype] : *table_write_set) {
    if (wtype == WType::INSERT) {
      TupleMeta new_meta {INVALID_TXN_ID, INVALID_TXN_ID, false};
      table_heap->UpdateTupleMeta(new_meta, rid);
    } else if (wtype == WType::DELETE) {
      TupleMeta new_meta {INVALID_TXN_ID, INVALID_TXN_ID, true};
      table_heap->UpdateTupleMeta(new_meta, rid);
    }
  }
  table_write_set->clear();

  for (auto [rid, table_oid, wtype, tuple, old_tuple, index_oid, catalog] : *index_write_set) {
    auto index = catalog->GetIndex(index_oid);
    if (wtype == WType::DELETE) {
      index->index_->InsertEntry(tuple, rid, txn);
    } else if (wtype == WType::INSERT) {
      index->index_->DeleteEntry(tuple, rid, txn);
    }
  }
  index_write_set->clear();
  txn->SetState(TransactionState::ABORTED);
  txn->UnlockTxn();
  ReleaseLocks(txn);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
