//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  struct Node {
    frame_id_t frame;
    Node *prev = nullptr;
    std::unique_ptr<Node> next;
    Node() = default;
    explicit Node(frame_id_t frame, Node *prev) : frame(frame), prev(prev) {}
  };
  std::mutex mutex;
  size_t num_pages;
  size_t size;
  std::unique_ptr<Node> fst;
  Node *lst;
  std::unordered_map<frame_id_t, Node *> hashMap;
};

}  // namespace bustub
