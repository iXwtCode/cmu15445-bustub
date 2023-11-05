//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  CheckCanTxnTakeLock(txn, lock_mode);
  // 检查是否符合 Isolation level 的限制，如果不符合就抛出异常
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto request_que = table_lock_map_[oid];
  request_que->latch_.lock();
  table_lock_map_latch_.unlock();

  auto try_lock_mode = TryGetLockMode(txn, oid);
  if (try_lock_mode.has_value()) {             // 如果事务已经对table加锁
    if (try_lock_mode.value() == lock_mode) {  // 锁类型相同
      request_que->latch_.unlock();
      return true;
    }
    if (CanLockUpgrade(try_lock_mode.value(), lock_mode)) {  // 检查锁升级是否兼容，如果兼容就升级锁，否则抛出异常
      if (request_que->upgrading_ !=
          INVALID_TXN_ID) {  // 其他事务锁升级提交但还未允许，此时不能提交新的锁升级，抛出异常
        txn->SetState(TransactionState::ABORTED);
        request_que->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      return UpgradeLockTable(txn, lock_mode, try_lock_mode.value(), oid);
    }
    // 这里锁升级不兼容， 抛出异常
    txn->SetState(TransactionState::ABORTED);
    request_que->latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }

  // 当前加锁是获取新锁，而不是升级锁
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  request_que->request_queue_.push_back(new_request);
  std::unique_lock<std::mutex> lock(request_que->latch_, std::adopt_lock);
  while (!CanGrantLock(request_que, new_request)) {
    request_que->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      request_que->request_queue_.remove(new_request);
      request_que->cv_.notify_all();
      return false;
    }
  }

  AddToLockSet(txn, lock_mode, oid);
  new_request->granted_ = true;
  if (lock_mode != LockMode::EXCLUSIVE) {
    request_que->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  auto request_que = table_lock_map_[oid];
  request_que->latch_.lock();
  table_lock_map_latch_.unlock();
  // 检查txn是否持有oid的锁，若没有抛出异常
  auto lock_mode = TryGetLockMode(txn, oid);
  if (!lock_mode.has_value()) {
    txn->SetState(TransactionState::ABORTED);
    request_que->latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 检查是否持有行锁，若持有行锁抛出异常
  if (!txn->GetExclusiveRowLockSet()->operator[](oid).empty() || !txn->GetSharedRowLockSet()->operator[](oid).empty()) {
    txn->SetState(TransactionState::ABORTED);
    request_que->latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  request_que->Remove(txn);
//  for (auto it = request_que->request_queue_.begin(); it != request_que->request_queue_.end(); ++it) {
//    if ((*it)->txn_id_ == txn->GetTransactionId()) {
//      request_que->request_queue_.erase(it);
//      break;
//    }
//  }

  // 根据隔离级别 更新事务状态
  auto isolation_level = txn->GetIsolationLevel();
  if (isolation_level == IsolationLevel::REPEATABLE_READ &&
      (txn->IsTableSharedLocked(oid) || txn->IsTableExclusiveLocked(oid))) {
    txn->SetState(TransactionState::SHRINKING);
  } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED && txn->IsTableExclusiveLocked(oid)) {
    txn->SetState(TransactionState::SHRINKING);
  } else if (isolation_level == IsolationLevel::READ_COMMITTED && txn->IsTableExclusiveLocked(oid)) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // 更新txn的lockset
  RemoveFromLockSet(txn, lock_mode.value(), oid);
  request_que->cv_.notify_all();
  request_que->latch_.unlock();  //  临界区结束。保证请求队列和lockset的更新同步

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  CheckCanTxnTakeLock(txn, lock_mode);  // 检查是否符合 Isolation level 的限制，如果不符合就抛出异常
  if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  // 获取当前请求队列
  auto request_que = row_lock_map_[rid];
  request_que->latch_.lock();
  row_lock_map_latch_.unlock();

  // 检查table的是否持有合适的锁
  auto parent_lock_mode = TryGetLockMode(txn, oid);
  if (parent_lock_mode.has_value()) {  // 父节点有锁
    if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  auto try_lock_mod = TryGetLockMode(txn, oid, rid);
  if (try_lock_mod.has_value()) {
    if (try_lock_mod.value() == lock_mode) {  // 锁类型相同
      request_que->latch_.unlock();
      return true;
    }
    if (CanLockUpgrade(try_lock_mod.value(), lock_mode)) {  // 检查锁升级是否兼容，如果兼容就升级锁，否则抛出异常
      if (request_que->upgrading_ !=
          INVALID_TXN_ID) {  // 其他事务锁升级提交但还未允许，此时不能提交新的锁升级，抛出异常
        txn->SetState(TransactionState::ABORTED);
        request_que->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      return UpgradeLockRow(txn, lock_mode, try_lock_mod.value(), oid, rid);
    }
    // 这里锁升级不兼容， 抛出异常
    txn->SetState(TransactionState::ABORTED);
    request_que->latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }

  // 当前加锁是获取新锁，而不是升级锁
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  request_que->request_queue_.push_back(new_request);
  std::unique_lock<std::mutex> lock(request_que->latch_, std::adopt_lock);
  while (!CanGrantLock(request_que, new_request)) {
    request_que->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      request_que->request_queue_.remove(new_request);
      request_que->cv_.notify_all();
      return false;
    }
  }

  AddToLockSet(txn, lock_mode, oid, rid);
  new_request->granted_ = true;
  if (lock_mode != LockMode::EXCLUSIVE) {
    request_que->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  auto request_que = row_lock_map_[rid];
  request_que->latch_.lock();
  row_lock_map_latch_.unlock();

  auto lock_mode = TryGetLockMode(txn, oid, rid);
  if (!lock_mode.has_value()) {
    txn->SetState(TransactionState::ABORTED);
    request_que->latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if (force) {
    request_que->Remove(txn);
//    for (auto it = request_que->request_queue_.begin(); it != request_que->request_queue_.end(); ++it) {
//      if ((*it)->txn_id_ == txn->GetTransactionId()) {
//        request_que->request_queue_.erase(it);
//        break;
//      }
//    }
    RemoveFromLockSet(txn, lock_mode.value(), oid, rid);
    request_que->cv_.notify_all();
    request_que->latch_.unlock();  //  临界区结束。保证请求队列和lockset的更新同步
    return true;
  }



  for (auto it = request_que->request_queue_.begin(); it != request_que->request_queue_.end(); ++it) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      request_que->request_queue_.erase(it);
      break;
    }
  }

  // 根据隔离级别 更新事务状态
  auto isolation_level = txn->GetIsolationLevel();
  if (isolation_level == IsolationLevel::REPEATABLE_READ &&
      (txn->IsRowSharedLocked(oid, rid) || txn->IsRowExclusiveLocked(oid, rid))) {
    txn->SetState(TransactionState::SHRINKING);
  } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED && txn->IsRowExclusiveLocked(oid, rid)) {
    txn->SetState(TransactionState::SHRINKING);
  } else if (isolation_level == IsolationLevel::READ_COMMITTED && txn->IsRowExclusiveLocked(oid, rid)) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // 更新txn的lockset
  RemoveFromLockSet(txn, lock_mode.value(), oid, rid);
  request_que->cv_.notify_all();
  request_que->latch_.unlock();  //  临界区结束。保证请求队列和lockset的更新同步

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here./
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  tid_set_.insert(t1);
  tid_set_.insert(t2);
  waits_for_[t1].insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(t2);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_set<txn_id_t> visited;
  for (auto it = waits_for_.rbegin(); it != waits_for_.rend(); ++it){
    auto tid = it->first;
    auto s = it->second;
    std::vector<txn_id_t> path;
    std::unordered_set<txn_id_t> on_path;
    LOG_DEBUG("find circle with %d", tid);
    for (auto id : s) {
      *txn_id = id;
      path.push_back(tid);
      on_path.insert(tid);
      if (FindCycle(tid, path, on_path, visited, txn_id)) {
        return true;
      }
    }
  }

  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto &[t1, t2s] : waits_for_) {
    for (const auto &t2 : t2s) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      LOG_DEBUG("\nSTART DEAD LOCK DETECTION!!!");
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      LOG_DEBUG("start build wait_for_map");
      BuildWaitForGraph();
      LOG_DEBUG("end build wait_for_map");
      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();
      txn_id_t tid { };
      while (HasCycle(&tid)) {
        LOG_DEBUG("find a cicle: %d", tid);
        auto txn = txn_manager_->GetTransaction(tid);
        txn->SetState(TransactionState::ABORTED);
        DeleteNode(tid);
        LOG_DEBUG("delete node %d", tid);
        if (tid_oid_map_.count(tid) > 0) {
          LOG_DEBUG("reach hear");
          table_lock_map_[tid_oid_map_[tid]]->latch_.lock();
          table_lock_map_[tid_oid_map_[tid]]->cv_.notify_all();
          table_lock_map_[tid_oid_map_[tid]]->latch_.unlock();
          LOG_DEBUG("notify a table request queue");
        }

        if (tid_rid_map_.count(tid) > 0) {
          LOG_DEBUG("reach hear");
          row_lock_map_[tid_rid_map_[tid]]->latch_.lock();
          row_lock_map_[tid_rid_map_[tid]]->cv_.notify_all();
          row_lock_map_[tid_rid_map_[tid]]->latch_.unlock();
          LOG_DEBUG("notify a row request queue");
        }
      }
      waits_for_.clear();
      tid_set_.clear();
      aborted_txn_set_.clear();
      tid_oid_map_.clear();
      tid_rid_map_.clear();
    }
  }
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockManager::LockMode lock_mode, LockMode old_lock_mode,
                                   const table_oid_t &oid) -> bool {
  auto request_que = table_lock_map_[oid];     // 获取请求队列
  request_que->Remove(txn);                    // 从请队列中删除旧请求
  RemoveFromLockSet(txn, old_lock_mode, oid);  // 在txn持有的锁中删除

  auto upgraded_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);  // 构建新请求
  auto it = request_que->request_queue_.begin();
  for (; it != request_que->request_queue_.end(); ++it) {
    if (!(*it)->granted_) {
      break;
    }
  }
  request_que->request_queue_.insert(it, upgraded_request);  // 新请求插入队列
  request_que->upgrading_ = txn->GetTransactionId();
  std::unique_lock<std::mutex> lock(request_que->latch_, std::adopt_lock);
  while (!CanGrantLock(request_que, upgraded_request)) {
    request_que->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      request_que->upgrading_ = INVALID_TXN_ID;
      request_que->request_queue_.remove(upgraded_request);
      request_que->cv_.notify_all();
      return false;
    }
  }

  upgraded_request->granted_ = true;
  request_que->upgrading_ = INVALID_TXN_ID;
  AddToLockSet(txn, lock_mode, oid);

  if (lock_mode != LockMode::EXCLUSIVE) {
    request_que->cv_.notify_all();
  }
  return true;
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockManager::LockMode lock_mode, LockMode old_lock_mode,
                                 const table_oid_t &oid, const RID &rid) -> bool {
  auto request_que = row_lock_map_[rid];            // 获取请求队列
  request_que->Remove(txn);                         // 从请队列中删除旧请求
  RemoveFromLockSet(txn, old_lock_mode, oid, rid);  // 在txn持有的锁中删除
  auto upgraded_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);  // 构建新请求
  auto it = request_que->request_queue_.begin();
  for (; it != request_que->request_queue_.end(); ++it) {   // 新请求加入请求队列
    if (!(*it)->granted_) {
      break;
    }
  }
  request_que->request_queue_.insert(it, upgraded_request);  // 新请求插入队列
  request_que->upgrading_ = txn->GetTransactionId();
  std::unique_lock<std::mutex> lock(request_que->latch_, std::adopt_lock);
  while (!CanGrantLock(request_que, upgraded_request)) {
    request_que->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      request_que->request_queue_.remove(upgraded_request);
      request_que->cv_.notify_all();
      return false;
    }
  }

  upgraded_request->granted_ = true;
  request_que->upgrading_ = INVALID_TXN_ID;
  AddToLockSet(txn, lock_mode, oid, rid);

  if (lock_mode != LockMode::EXCLUSIVE) {
    request_que->cv_.notify_all();
  }
  return true;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  switch (curr_lock_mode) {
    case LockMode::INTENTION_SHARED:
      return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
             requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::SHARED:
      return requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || requested_lock_mode == LockMode::EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE;
    case LockMode::EXCLUSIVE:
      return false;
  }
}

// 检查事务隔离级别是否允许获得该类型的锁，如果不允许抛出异常
void LockManager::CheckCanTxnTakeLock(Transaction *txn, LockMode lock_mode) {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 可重复读不允许在收缩阶段加任何锁
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
}

auto LockManager::AreLocksCompatible(LockManager::LockMode curr, LockManager::LockMode requested_lock_mode) -> bool {
  switch (curr) {
    case LockMode::INTENTION_SHARED:
      return requested_lock_mode != LockMode::EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::INTENTION_SHARED || requested_lock_mode == LockMode::INTENTION_EXCLUSIVE;
    case LockMode::SHARED:
      return requested_lock_mode == LockMode::INTENTION_SHARED || requested_lock_mode == LockMode::SHARED;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::INTENTION_SHARED;
    case LockMode::EXCLUSIVE:
      return false;
  }
}

auto LockManager::GetTableLockSet(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_set<table_oid_t>> {
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet();
    case LockMode::SHARED:
      return txn->GetSharedTableLockSet();
    case LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet();
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet();
    case LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet();
  }
}
auto LockManager::GetRowLockSet(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
  if (lock_mode == LockMode::EXCLUSIVE) {
    return txn->GetExclusiveRowLockSet();
  }
  return txn->GetSharedRowLockSet();
}

auto LockManager::TryGetLockMode(Transaction *txn, const table_oid_t &oid) -> std::optional<LockMode> {
  if (txn->IsTableExclusiveLocked(oid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsTableSharedLocked(oid)) {
    return LockMode::SHARED;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    return LockMode::INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return LockMode::INTENTION_SHARED;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    return LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  return std::nullopt;
}

auto LockManager::TryGetLockMode(Transaction *txn, const table_oid_t &oid, const RID &rid) -> std::optional<LockMode> {
  if (txn->IsRowSharedLocked(oid, rid)) {
    return LockMode::SHARED;
  }
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    return LockMode::EXCLUSIVE;
  }
  return std::nullopt;
}

void LockManager::AddToLockSet(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  GetTableLockSet(txn, lock_mode)->insert(oid);
}
void LockManager::AddToLockSet(Transaction *txn, LockMode lock_mode, table_oid_t oid, const RID &rid) {
  GetRowLockSet(txn, lock_mode)->operator[](oid).insert(rid);
}
void LockManager::RemoveFromLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  GetTableLockSet(txn, lock_mode)->erase(oid);
}
void LockManager::RemoveFromLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  GetRowLockSet(txn, lock_mode)->operator[](oid).erase(rid);
}
auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  auto parent_lock_mode = TryGetLockMode(txn, oid).value();  // 这里可以保证一定有值
  if (row_lock_mode == LockMode::EXCLUSIVE) {
    return parent_lock_mode == LockMode::INTENTION_EXCLUSIVE || parent_lock_mode == LockMode::EXCLUSIVE ||
           parent_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  return true;
}

auto LockManager::CanGrantLock(const std::shared_ptr<LockRequestQueue>& que
                               , std::shared_ptr<LockRequest> lock_request) -> bool {
  if (que->request_queue_.empty()) {
    return true;
  }
  for (const auto &quest : que->request_queue_) {
    if (quest->granted_) {
      if (!AreLocksCompatible(quest->lock_mode_, lock_request->lock_mode_)) {
        return false;
      }
    } else if (quest.get() != lock_request.get()){
      return false;
    } else {
      return true;
    }
  }
  return false;
}

auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  if (*abort_txn_id == source_txn) {  // 找到一个环
    return true;
  }
  if (on_path.find(*abort_txn_id) != on_path.end()) {
    return false;
  }
//  if (visited.find(*abort_txn_id) != visited.end()) {
//    return false;
//  }
  path.push_back(*abort_txn_id);
  on_path.insert(*abort_txn_id);
  visited.insert(*abort_txn_id);
  for (auto tid : waits_for_[*abort_txn_id]) {
    *abort_txn_id = tid;
    if (FindCycle(source_txn, path, on_path, visited, abort_txn_id)) {
      return true;
    }
    path.pop_back();
    on_path.erase(*abort_txn_id);
  }
  return false;
}

void LockManager::BuildWaitForGraph() {
  //  printf("table_lock_map_ size: %zu\n", table_lock_map_.size());
  //  printf("row_lock_map_ size: %zu\n", row_lock_map_.size());
  for (auto &[oid, request_que] : table_lock_map_) {
    request_que->latch_.lock();
    std::vector<txn_id_t> granted_tids;
    for (const auto &request : request_que->request_queue_) {
      if (request->granted_) {
        granted_tids.push_back(request->txn_id_);
      } else {
        tid_oid_map_.emplace(request->txn_id_, request->oid_);
        for (auto tid : granted_tids) {
          AddEdge(request->txn_id_, tid);
          LOG_DEBUG("add edge  %d->%d", tid, request->txn_id_);
        }
      }
    }
    request_que->latch_.unlock();
  }

  for (auto &[rid, request_que] : row_lock_map_) {
    request_que->latch_.lock();
    std::vector<txn_id_t> granted_tids;
    for (const auto &request : request_que->request_queue_) {
      if (request->granted_) {
        granted_tids.push_back(request->txn_id_);
      } else {
        tid_rid_map_.emplace(request->txn_id_, rid);
        for (auto tid : granted_tids) {
          AddEdge(request->txn_id_, tid);
          LOG_DEBUG("add edge  %d->%d", tid, request->txn_id_);
        }
      }
    }
    request_que->latch_.unlock();
  }
}

auto LockManager::DeleteNode(txn_id_t txn_id) -> void {
  waits_for_.erase(txn_id);
  tid_set_.erase(txn_id);
  for (auto a_txn_id : tid_set_) {
    LOG_DEBUG("remove edge %d->%d", a_txn_id, txn_id);
    RemoveEdge(a_txn_id, txn_id);
  }
}
}  // namespace bustub
