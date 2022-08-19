//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      catalog_(exec_ctx->GetCatalog()),
      table_info_(catalog_->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  Transaction *txn = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();

  if (txn->IsSharedLocked(*rid)) {
    if (!lock_mgr->LockUpgrade(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  } else {
    if (!lock_mgr->LockExclusive(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  for (auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    auto key = tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
    index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    txn->GetIndexWriteSet()->emplace_back(
        IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
  }

  if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
    if (!lock_mgr->Unlock(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  return Next(tuple, rid);
}

}  // namespace bustub
