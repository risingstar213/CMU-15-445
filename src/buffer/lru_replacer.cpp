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


LRUReplacer::LRUReplacer(size_t num_pages) : num_pages(num_pages) {
  // dummy node
  size = 0;
  fst = std::make_unique<Node>();
  fst->next = std::make_unique<Node>(0, fst.get());
  lst = fst->next.get();
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  auto lock = std::lock_guard(mutex);
  if (size > 0) {
    auto victim = lst->prev;
    auto pre = victim->prev;
    *frame_id = victim->frame;
    hashMap.erase(victim->frame);
    pre->next = std::move(victim->next);
    pre->next->prev = pre;
    size --;
    return true;
  }
  return false; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  auto lock = std::lock_guard(mutex);
  auto iter = hashMap.find(frame_id);
  if (iter != hashMap.end()) {
    auto node = iter->second;
    hashMap.erase(node->frame);
    auto cur = std::move(node->prev->next);
    auto pre = node->prev;
    pre->next = std::move(cur->next);
    pre->next->prev = pre;
    size --;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  auto lock = std::lock_guard(mutex);
  auto iter = hashMap.find(frame_id);
  if (iter == hashMap.end()) {
    auto newNode = std::make_unique<Node>(frame_id, fst.get());
    hashMap.insert(std::pair<frame_id_t, Node *>(frame_id, newNode.get()));
    newNode->next = std::move(fst->next);
    newNode->next->prev = newNode.get();
    fst->next = std::move(newNode);
    size ++;
  }
}

auto LRUReplacer::Size() -> size_t { 
  auto lock = std::lock_guard(mutex);
  return size;
}

}  // namespace bustub
