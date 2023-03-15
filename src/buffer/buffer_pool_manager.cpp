//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //    "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //  "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  if (!free_list_.empty()) {
    frame_id_t fid = free_list_.back();
    free_list_.pop_back();
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);

    *page_id = AllocatePage();
    page_table_[*page_id] = fid;

    auto p = &pages_[fid];
    latch_.unlock();

    p->page_id_ = *page_id;
    p->is_dirty_ = true;
    p->pin_count_ = 0;
    p->WLatch();
    p->ResetMemory();
    p->WUnlatch();
    p->pin_count_ = 1;
    return p;
  }

  if (frame_id_t fid; replacer_->Evict(&fid)) {
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    *page_id = AllocatePage();
    page_table_[*page_id] = fid;

    auto p = &pages_[fid];
    page_table_.erase(p->GetPageId());
    latch_.unlock();

    if (p->IsDirty()) {
      p->RLatch();
      disk_manager_->WritePage(p->GetPageId(), p->GetData());
      p->RUnlatch();
    }
    p->page_id_ = *page_id;
    p->is_dirty_ = true;
    p->pin_count_ = 0;
    p->WLatch();
    p->ResetMemory();
    p->WUnlatch();
    latch_.unlock();
    p->pin_count_ = 1;
    return p;
  }
  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
  if (page_table_.count(page_id) != 0) {
    pages_[page_table_[page_id]].pin_count_ += 1;
    latch_.unlock();
    return &pages_[page_table_[page_id]];
  }

  // 从freelist选取一个新frame，或者淘汰一个frame
  if (!free_list_.empty()) {
    frame_id_t fid = free_list_.back();
    free_list_.pop_back();
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    page_table_[page_id] = fid;
    latch_.unlock();

    auto p = &pages_[fid];
    ++(p->pin_count_);
    p->page_id_ = page_id;
    p->is_dirty_ = true;
    return p;
  }

  if (frame_id_t fid; replacer_->Evict(&fid)) {
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    page_table_[page_id] = fid;

    auto p = &pages_[fid];
    page_table_.erase(p->GetPageId());
    latch_.unlock();
    if (p->IsDirty()) {
      p->RLatch();
      disk_manager_->WritePage(p->GetPageId(), p->GetData());
      p->RUnlatch();
    }
    p->pin_count_ += 1;
    p->page_id_ = page_id;
    p->is_dirty_ = true;

    p->WLatch();
    p->ResetMemory();
    disk_manager_->ReadPage(page_id, p->data_);
    p->WUnlatch();
    return p;
  }
  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto fid = page_table_[page_id];
  auto p = &pages_[fid];
  latch_.unlock();

  if (p->pin_count_ == 0) {
    return false;
  }

  --(p->pin_count_);
  if (p->pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }
  if (is_dirty) {
    p->is_dirty_ = true;
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto fid = page_table_[page_id];
  auto p = &pages_[fid];

  p->RLatch();
  disk_manager_->WritePage(p->GetPageId(), p->GetData());
  p->RUnlatch();

  p->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto [pid, fid] : page_table_) {
    FlushPage(pid);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto fid = page_table_[page_id];
  auto p = &pages_[page_table_[page_id]];
  if (p->GetPinCount() != 0) {
    return false;
  }

  DeallocatePage(page_id);
  replacer_->Remove(fid);
  free_list_.push_front(fid);
  page_table_.erase(page_id);
  p->ResetMemory();
  p->page_id_ = INVALID_PAGE_ID;
  p->pin_count_ = 0;

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
