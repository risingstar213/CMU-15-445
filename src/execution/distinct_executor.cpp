//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() { child_executor_->Init(); }

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  uint32_t i;
  while (true) {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }

    std::vector<Value> values;
    for (i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      values.emplace_back(tuple->GetValue(plan_->OutputSchema(), i));
    }

    auto key = DistinctKey{values};
    if (hash_set_.count(key) > 0) {
      continue;
    }
    hash_set_.insert(std::move(key));
    return true;
  }
}

}  // namespace bustub
