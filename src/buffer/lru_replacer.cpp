//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_page_(num_pages) {
  // dummy node
  size_ = 0;
  fst_ = std::make_unique<Node>();
  fst_->next_ = std::make_unique<Node>(0, fst_.get());
  lst_ = fst_->next_.get();
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  auto lock = std::lock_guard(mutex_);
  if (size_ > 0) {
    auto victim = lst_->prev_;
    auto pre = victim->prev_;
    *frame_id = victim->frame_;
    hash_map_.erase(victim->frame_);
    pre->next_ = std::move(victim->next_);
    pre->next_->prev_ = pre;
    size_--;
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  auto lock = std::lock_guard(mutex_);
  auto iter = hash_map_.find(frame_id);
  if (iter != hash_map_.end()) {
    auto node = iter->second;
    hash_map_.erase(node->frame_);
    auto cur = std::move(node->prev_->next_);
    auto pre = node->prev_;
    pre->next_ = std::move(cur->next_);
    pre->next_->prev_ = pre;
    size_--;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  auto lock = std::lock_guard(mutex_);
  auto iter = hash_map_.find(frame_id);
  if (iter == hash_map_.end() && size_ < num_page_) {
    auto new_node = std::make_unique<Node>(frame_id, fst_.get());
    hash_map_.insert(std::pair<frame_id_t, Node *>(frame_id, new_node.get()));
    new_node->next_ = std::move(fst_->next_);
    new_node->next_->prev_ = new_node.get();
    fst_->next_ = std::move(new_node);
    size_++;
  }
}

auto LRUReplacer::Size() -> size_t {
  auto lock = std::lock_guard(mutex_);
  return size_;
}

}  // namespace bustub
