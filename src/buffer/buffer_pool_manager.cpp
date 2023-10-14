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
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
  } else {
    if (replacer_->Evict(&fid)) {
      if (pages_[fid].IsDirty()) {
        disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
      }
      page_table_.erase(pages_[fid].page_id_);
    } else {
      return nullptr;
    }
  }

  *page_id = AllocatePage();
  auto p = &pages_[fid];
  p->ResetMemory();
  p->page_id_ = *page_id;
  p->pin_count_ = 1;
  p->is_dirty_ = false;
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  page_table_[*page_id] = fid;
  return p;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_table_.count(page_id) != 0) {
    auto p = &pages_[page_table_[page_id]];
    p->pin_count_ += 1;
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    return p;
  }

  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
  } else {
    if (replacer_->Evict(&fid)) {
      if (pages_[fid].IsDirty()) {
        disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
      }
      page_table_.erase(pages_[fid].page_id_);
    } else {
      return nullptr;
    }
  }

  auto p = &pages_[fid];
  p->pin_count_ = 1;
  p->is_dirty_ = false;
  p->page_id_ = page_id;
  p->ResetMemory();
  disk_manager_->ReadPage(p->page_id_, p->data_);
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  page_table_[page_id] = fid;
  return p;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto fid = page_table_[page_id];
  auto p = &pages_[fid];

  if (p->pin_count_ <= 0) {
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
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto p = &pages_[page_table_[page_id]];
  disk_manager_->WritePage(p->GetPageId(), p->GetData());
  p->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto [pid, fid] : page_table_) {
    FlushPage(pid);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }

  auto fid = page_table_[page_id];
  auto p = &pages_[page_table_[page_id]];
  if (p->GetPinCount() > 0) {
    return false;
  }

  replacer_->Remove(fid);
  free_list_.emplace_front(fid);
  page_table_.erase(page_id);
  p->ResetMemory();
  p->page_id_ = INVALID_PAGE_ID;
  p->pin_count_ = 0;
  p->is_dirty_ = false;
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  if (!id_can_reuse_.empty()) {
    auto res = id_can_reuse_.back();
    id_can_reuse_.pop_back();
    return res;
  }
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto p = FetchPage(page_id);
  p->RLatch();
  return {this, p};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto p = FetchPage(page_id);
  p->WLatch();
  return {this, p};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }
}  // namespace bustub
