//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (curr_size_ == 0) {
    latch_.unlock();
    return false;
  }
  if (!l_fid_e_.empty()) {
    for (auto it = l_fid_e_.begin(); it != l_fid_e_.end(); ++it) {
      if (node_store_[*it].IsEvictable()) {
        *frame_id = *it;
        l_fid_e_.erase(fid2iter_e_[*frame_id]);
        fid2iter_e_.erase(*frame_id);
        node_store_.erase(*frame_id);
        --curr_size_;
        latch_.unlock();
        return true;
      }
    }
  }

  if (!l_fid_k_.empty()) {
    for (auto it = l_fid_k_.begin(); it != l_fid_k_.end(); ++it) {
      if (node_store_[*it].IsEvictable()) {
        *frame_id = *it;
        l_fid_k_.erase(fid2iter_k_[*frame_id]);
        fid2iter_k_.erase(*frame_id);
        node_store_.erase(*frame_id);
        --curr_size_;
        latch_.unlock();
        return true;
      }
    }
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
    if(static_cast<size_t>(frame_id) > replacer_size_) {
      latch_.unlock();
      throw Exception("invalid frame id");
    }

  if (node_store_.count(frame_id) == 0) {
    node_store_[frame_id] = LRUKNode(k_);
  }
  auto &node = node_store_[frame_id];
  node.AddHistory(current_timestamp_++);
  node.IcreaseTimes();

  if (node.GetTimes() >= k_) {
    if (node.GetTimes() == k_) {
      l_fid_e_.erase(fid2iter_e_[frame_id]);
      fid2iter_e_.erase(frame_id);
      AddToList(frame_id, node.GetPreKthTime());
    } else {
      l_fid_k_.erase(fid2iter_k_[frame_id]);
      AddToList(frame_id, node.GetPreKthTime());
    }
  } else if (node.GetTimes() == 1) {
    l_fid_e_.push_back(frame_id);
    fid2iter_e_[frame_id] = --l_fid_e_.end();
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    latch_.unlock();
    throw Exception("invalid frame id");
  }
  if (node_store_.count(frame_id) == 0) {
    latch_.unlock();
    throw Exception("invalid frame id");
  }

  auto &node = node_store_[frame_id];
  if (node.IsEvictable() && !set_evictable) {
    --curr_size_;
    node.SetEvictable(false);
  }
  if (!node.IsEvictable() && set_evictable) {
    ++curr_size_;
    node.SetEvictable(true);
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (fid2iter_e_.count(frame_id) != 0) {
    l_fid_e_.erase(fid2iter_e_[frame_id]);
    fid2iter_e_.erase(frame_id);
    node_store_.erase(frame_id);
    --curr_size_;
    latch_.unlock();
    return;
  }
  if (fid2iter_k_.count(frame_id) != 0) {
    l_fid_k_.erase(fid2iter_k_[frame_id]);
    fid2iter_k_.erase(frame_id);
    node_store_.erase(frame_id);
    --curr_size_;
    latch_.unlock();
    return;
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  int res = curr_size_;
  latch_.unlock();
  return res;
}
void LRUKReplacer::AddToList(frame_id_t fid, size_t target) {
  auto it = l_fid_k_.begin();
  for (; it != l_fid_k_.end(); ++it) {
    if (node_store_[*it].GetPreKthTime() >= target) {
      l_fid_k_.insert(it, fid);
      fid2iter_k_[fid] = --it;
      return ;
    }
  }

  if (it == l_fid_k_.end()) {
    l_fid_k_.emplace_back(fid);
    fid2iter_k_[fid] = --it;
  }
}

void LRUKNode::AddHistory(size_t time) {
  history_.emplace_back(time);
  if (history_.size() == K_) {
    last_kth_pre_time = history_.begin();
  }
  if (history_.size() > K_) {
    ++last_kth_pre_time;
  }
}

auto LRUKNode::GetPreKthTime() const -> size_t {
  if (history_.size() >= K_) {
    return *last_kth_pre_time;
  }
  return 0;
}
}  // namespace bustub
