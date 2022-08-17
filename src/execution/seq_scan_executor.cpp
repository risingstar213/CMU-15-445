//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_heap_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_.get()),
      schema_(&exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->schema_),
      iter_(table_heap_->Begin(exec_ctx->GetTransaction())) {}

void SeqScanExecutor::Init() { iter_ = table_heap_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  bool get_result = false;
  bool result_got = true;
  uint32_t i;
  while (!get_result) {
    if (iter_ == table_heap_->End()) {
      result_got = false;
      break;
    }
    *tuple = *iter_;
    *rid = iter_->GetRid();

    std::vector<Value> values;
    for (i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      values.emplace_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(tuple, schema_));
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    iter_++;
    auto predicate = plan_->GetPredicate();
    if (predicate != nullptr && !predicate->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
      continue;
    }
    get_result = true;
    result_got = true;
  }
  return result_got;
}

}  // namespace bustub
