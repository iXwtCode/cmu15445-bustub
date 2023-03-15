#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
}

void BasicPageGuard::Drop() { bpm_->UnpinPage(page_->GetPageId(), is_dirty_); }

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { bpm_->UnpinPage(page_->GetPageId(), is_dirty_); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) { guard_.page_->RLatch(); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_ = std::move(that.guard_);
  guard_.page_->RUnlatch();
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { guard_.page_->RUnlatch(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
  guard_.page_->WLatch();
  guard_.is_dirty_ = true;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  guard_.page_->WLatch();
  guard_.is_dirty_ = true;
  return *this;
}

void WritePageGuard::Drop() {
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { guard_.page_->WUnlatch(); }  // NOLINT

}  // namespace bustub
