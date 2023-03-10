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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {

}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if(curr_size_ == 0) {
    latch_.unlock();
    return false;
  }

  if(!l_fid_e_.empty()) {
    *frame_id = l_fid_e_.front();
    l_fid_e_.pop_front();
    fid2iter_e_.erase(*frame_id);
  }
  else {
    *frame_id= l_fid_k_.front();
    l_fid_k_.pop_front();
    fid2iter_k_.erase(*frame_id);
  }
  node_store_[*frame_id].history_.clear();
  node_store_[*frame_id].is_evictable_ = false;
  --curr_size_;
  ++current_timestamp_;
  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  if(static_cast<size_t>(frame_id) > replacer_size_) {
    latch_.unlock();
    throw Exception("Frame_id you give is larger than replacer_size");
  }
  if(node_store_.count(frame_id) == 0) {
    node_store_[frame_id] = LRUKNode{{}, 0, frame_id};
    node_store_[frame_id].history_.push_back(current_timestamp_++);
  }
  else {
    node_store_[frame_id].history_.push_back(current_timestamp_++);
  }

  auto &node = node_store_[frame_id];
  auto &history = node.history_;
  //如果访问超过k_次，更新frame的k_
  if(history.size() >= k_) {
    auto pre = history.rbegin();
    for(size_t i = 0; i < k_; ++i) {
      ++pre;
    }
    node.k_ = history.back() - *pre;
  }

  if(node.is_evictable_) {
    if(fid2iter_e_.count(frame_id) != 0) {
      if(node_store_[frame_id].k_ > 0) {
        l_fid_e_.erase(fid2iter_e_[frame_id]);
        fid2iter_e_.erase(frame_id);
        AddListk(frame_id);
      }
      else {
        l_fid_e_.push_back(*fid2iter_e_[frame_id]);
        l_fid_e_.erase(fid2iter_e_[frame_id]);
        fid2iter_e_[frame_id] = std::next(l_fid_e_.end(), -1);
      }
    }
    else {
      auto temp = fid2iter_k_[frame_id];
      l_fid_k_.erase(temp);
      AddListk(frame_id);
    }
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if(static_cast<size_t>(frame_id) > replacer_size_) {
    latch_.unlock();
    throw Exception("frame_id is invalid");
  }
  if(node_store_.count(frame_id) == 0) {
    latch_.unlock();
    return ;
  }

  auto &node = node_store_[frame_id];
  if(set_evictable && !node.is_evictable_) {
    node.is_evictable_ = true;
    ++curr_size_;
    //加入队列
    if(node.k_ >= k_) {
      AddListk(frame_id);
    }
    else {
      AddListe(frame_id);
    }
  }

  if(!set_evictable && node.is_evictable_) {
    node.is_evictable_ = false;
    --curr_size_;
    //从队列删除
    if(fid2iter_e_.count(frame_id) != 0) {
      l_fid_e_.erase(fid2iter_e_[frame_id]);
      fid2iter_e_.erase(frame_id);
    }
    else {
      l_fid_k_.erase(fid2iter_k_[frame_id]);
      fid2iter_k_.erase(frame_id);
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if(node_store_.count(frame_id) == 0) { return ; }
  if(!node_store_[frame_id].is_evictable_) {
    throw Exception("frame is non-evitable");
  }

  --curr_size_;
  if(fid2iter_e_.count(frame_id) != 0) {
    l_fid_e_.erase(fid2iter_e_[frame_id]);
    fid2iter_e_.erase(frame_id);
  }
  else {
    l_fid_k_.erase(fid2iter_k_[frame_id]);
    fid2iter_k_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  int res = curr_size_;
  latch_.unlock();
  return res;
}
void LRUKReplacer::AddListe(frame_id_t frame_id) {
  auto it = l_fid_e_.begin();
  auto node = node_store_[frame_id];
  for(; it != l_fid_e_.end(); ++it) {
    if(node_store_[*it].history_.back() >= node.history_.back()) {
      l_fid_e_.insert(it, frame_id);
      fid2iter_e_[frame_id] = std::move(it);
      break ;
    }
  }
  if(it == l_fid_e_.end()) {
    l_fid_e_.push_back(frame_id);
    fid2iter_e_[frame_id] = std::next(l_fid_e_.end(), -1);
  }
}
void LRUKReplacer::AddListk(frame_id_t frame_id) {
  auto it = l_fid_k_.begin();
  auto k = node_store_[frame_id].k_;
  for(; it != l_fid_k_.end(); ++it) {
    if(node_store_[*it].k_ >= k) {
      l_fid_k_.insert(it, frame_id);
      fid2iter_k_[frame_id] = it;
      break ;
    }
  }
  if(it == l_fid_k_.end()) {
    l_fid_k_.push_back(frame_id);
    fid2iter_k_[frame_id] = std::next(l_fid_k_.end(), -1);
  }
}
}  // namespace bustub
