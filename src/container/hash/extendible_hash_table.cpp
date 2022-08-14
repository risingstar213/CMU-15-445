//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto page0 = buffer_pool_manager->NewPage(&directory_page_id_);
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(page0->GetData());
  dir_page->SetPageId(directory_page_id_);

  page_id_t bucket_page;
  buffer_pool_manager->NewPage(&bucket_page);

  // initial depth = 1
  dir_page->SetBucketPageId(0, bucket_page);
  dir_page->SetLocalDepth(0, 0);

  buffer_pool_manager->UnpinPage(directory_page_id_, true);
  buffer_pool_manager->UnpinPage(bucket_page, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> std::pair<Page *, HASH_TABLE_BUCKET_TYPE *> {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  return std::pair<Page *, HASH_TABLE_BUCKET_TYPE *>(page, bucket_page);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  // LOG_INFO("# Search key");

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto [raw_page, bucket_page] = FetchBucketPage(bucket_page_id);
  raw_page->RLatch();
  bool success = bucket_page->GetValue(key, comparator_, result);

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);

  raw_page->RUnlatch();
  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  // LOG_INFO("# Insert key");

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto [raw_page, bucket_page] = FetchBucketPage(bucket_page_id);
  raw_page->WLatch();

  if (bucket_page->IsFull()) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    raw_page->WUnlatch();
    table_latch_.RUnlock();
    return SplitInsert(transaction, key, value);
  }

  bool success = bucket_page->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  raw_page->WUnlatch();

  table_latch_.RUnlock();
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  // LOG_INFO("# SplitInsert key");

  bool success = false;
  bool inserted = false;
  uint32_t i;
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  while (!inserted) {
    uint32_t global_depth = dir_page->GetGlobalDepth();
    uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
    uint32_t bucket_page_id = KeyToPageId(key, dir_page);
    auto bucket_page = FetchBucketPage(bucket_page_id).second;
    // raw_page->WLatch();

    if (bucket_page->IsFull()) {
      if (dir_page->GetLocalDepth(bucket_idx) == global_depth) {
        dir_page->IncrGlobalDepth();
      }

      dir_page->IncrLocalDepth(bucket_idx);

      uint32_t num_readable = bucket_page->NumReadable();
      uint32_t num_read = 0;
      MappingType *tmp_array = new MappingType[num_readable];
      for (i = 0; i < BUCKET_ARRAY_SIZE; i++) {
        if (num_readable == num_read) {
          break;
        }
        if (!bucket_page->IsReadable(i)) {
          continue;
        }
        tmp_array[num_read] = MappingType(bucket_page->KeyAt(i), bucket_page->ValueAt(i));
        num_read += 1;
      }
      bucket_page->Clear();

      uint32_t split_image_bucket_idx = dir_page->GetSplitImageIndex(bucket_idx);
      page_id_t split_image_bucket_page_id;
      HASH_TABLE_BUCKET_TYPE *split_image_bucket_page =
          reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&split_image_bucket_page_id));

      uint32_t diff = 1 << dir_page->GetLocalDepth(bucket_idx);
      uint32_t dir_size = dir_page->Size();
      uint32_t bucket_page_local_depth = dir_page->GetLocalDepth(bucket_idx);
      for (i = bucket_idx;; i -= diff) {
        dir_page->SetBucketPageId(i, bucket_page_id);
        dir_page->SetLocalDepth(i, bucket_page_local_depth);
        if (i < diff) {
          break;
        }
      }
      for (i = bucket_idx; i < dir_size; i += diff) {
        dir_page->SetBucketPageId(i, bucket_page_id);
        dir_page->SetLocalDepth(i, bucket_page_local_depth);
      }

      for (i = split_image_bucket_idx;; i -= diff) {
        dir_page->SetBucketPageId(i, split_image_bucket_page_id);
        dir_page->SetLocalDepth(i, bucket_page_local_depth);
        if (i < diff) {
          break;
        }
      }
      for (i = split_image_bucket_idx; i < dir_size; i += diff) {
        dir_page->SetBucketPageId(i, split_image_bucket_page_id);
        dir_page->SetLocalDepth(i, bucket_page_local_depth);
      }

      uint32_t local_bucket_idx = bucket_idx & dir_page->GetLocalDepthMask(bucket_idx);

      for (i = 0; i < num_readable; i++) {
        auto [tmp_key, tmp_value] = tmp_array[i];
        uint32_t tmp_bucket_idx = Hash(tmp_key) & dir_page->GetLocalDepthMask(bucket_idx);
        if (tmp_bucket_idx != local_bucket_idx) {
          split_image_bucket_page->Insert(tmp_key, tmp_value, comparator_);
        } else {
          bucket_page->Insert(tmp_key, tmp_value, comparator_);
        }
      }

      delete[] tmp_array;
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
      buffer_pool_manager_->UnpinPage(split_image_bucket_page_id, true);
    } else {
      success = bucket_page->Insert(key, value, comparator_);
      inserted = true;
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    }
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  table_latch_.WUnlock();
  return success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  // LOG_INFO("# Remove key");

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto [raw_page, bucket_page] = FetchBucketPage(bucket_page_id);
  raw_page->WLatch();
  bool success = bucket_page->Remove(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(bucket_page_id, success);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  raw_page->WUnlatch();
  table_latch_.RUnlock();
  // attempt to merge
  Merge(transaction, key, value);
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  // LOG_INFO("# Merge key");
  uint32_t i;
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  if (dir_page->GetLocalDepth(bucket_idx) == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return;
  }
  uint32_t split_image_bucket_idx = dir_page->GetSplitImageIndex(bucket_idx);
  if (dir_page->GetLocalDepth(bucket_idx) != dir_page->GetLocalDepth(split_image_bucket_idx)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return;
  }

  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id).second;
  if (!static_cast<bool>(bucket_page->IsEmpty())) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return;
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->DeletePage(bucket_page_id);

  page_id_t split_image_bucket_page_id = dir_page->GetBucketPageId(split_image_bucket_idx);

  dir_page->DecrLocalDepth(bucket_idx);
  dir_page->DecrLocalDepth(split_image_bucket_idx);
  uint32_t diff = 1 << dir_page->GetLocalDepth(bucket_idx);
  uint32_t dir_size = dir_page->Size();
  uint32_t bucket_page_local_depth = dir_page->GetLocalDepth(bucket_idx);
  for (i = bucket_idx;; i -= diff) {
    dir_page->SetBucketPageId(i, split_image_bucket_page_id);
    dir_page->SetLocalDepth(i, bucket_page_local_depth);
    if (i < diff) {
      break;
    }
  }
  for (i = bucket_idx; i < dir_size; i += diff) {
    dir_page->SetBucketPageId(i, split_image_bucket_page_id);
    dir_page->SetLocalDepth(i, bucket_page_local_depth);
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
