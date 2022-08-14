//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetLocation(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx / 8;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetMask(uint32_t bucket_idx) const -> char {
  uint32_t idx = bucket_idx % 8;
  return static_cast<char>(1 << idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  uint32_t i;
  for (i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (!IsReadable(i)) {
      continue;
    }
    if (cmp(key, KeyAt(i)) == 0) {
      result->push_back(ValueAt(i));
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  // if (IsFull()) {
  //   return false;
  // }
  std::vector<ValueType> result;
  GetValue(key, cmp, &result);
  if (std::find(result.cbegin(), result.cend(), value) != result.cend()) {
    return false;
  }
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    SetOccupied(i);
    if (!IsReadable(i)) {
      array_[i] = MappingType(key, value);
      SetReadable(i);
      break;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  bool success = false;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (!IsReadable(i)) {
      continue;
    }
    if (cmp(key, KeyAt(i)) == 0 && ValueAt(i) == value) {
      SetUnreadable(i);
      success = true;
      break;
    }
  }
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  SetUnreadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  uint32_t idx = GetLocation(bucket_idx);
  char mask = GetMask(bucket_idx);
  bool occupied = false;
  if ((occupied_[idx] & mask) != 0) {
    occupied = true;
  }
  return occupied;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t idx = GetLocation(bucket_idx);
  char mask = GetMask(bucket_idx);
  occupied_[idx] = occupied_[idx] | mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  uint32_t idx = GetLocation(bucket_idx);
  char mask = GetMask(bucket_idx);
  bool readable = false;
  if ((readable_[idx] & mask) != 0) {
    readable = true;
  }
  return readable;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t idx = GetLocation(bucket_idx);
  char mask = GetMask(bucket_idx);
  readable_[idx] = readable_[idx] | mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnreadable(uint32_t bucket_idx) {
  uint32_t idx = GetLocation(bucket_idx);
  char mask = GetMask(bucket_idx);
  readable_[idx] = readable_[idx] & (~mask);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  return (NumReadable() == BUCKET_ARRAY_SIZE);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t num_readable = 0;
  uint32_t i;
  for (i = 0; i < (BUCKET_ARRAY_SIZE - 1) / 8 + 1; i++) {
    uint8_t readable = readable_[i];
    while (readable != 0) {
      readable = readable & (readable - 1);
      num_readable += 1;
    }
  }
  return num_readable;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  return (NumReadable() == 0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType *HASH_TABLE_BUCKET_TYPE::GetArrayCopy() {
  uint32_t num = NumReadable();
  MappingType *copy = new MappingType[num];
  for (uint32_t i = 0, index = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      copy[index++] = array_[i];
    }
  }
  return copy;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HashTableBucketPage<KeyType, ValueType, KeyComparator>::Clear() {
  // LOG_DEBUG("clear");
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  memset(array_, 0, sizeof(array_));
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
