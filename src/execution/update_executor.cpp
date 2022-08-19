//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      catalog_(exec_ctx->GetCatalog()),
      table_info_(catalog_->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  if (txn->IsSharedLocked(*rid)) {
    if (!lock_mgr->LockUpgrade(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  } else {
    if (!lock_mgr->LockExclusive(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  auto new_tuple = GenerateUpdatedTuple(*tuple);
  if (!table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  for (auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    auto old_key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
    index->index_->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());
  }
  if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
    if (!lock_mgr->Unlock(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  return Next(tuple, rid);
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
