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
  if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  for (auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    auto key = tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
    index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
  }
  return Next(tuple, rid);
}

}  // namespace bustub
