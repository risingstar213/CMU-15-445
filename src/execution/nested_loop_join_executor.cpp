//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  is_left_selected_ = left_executor_->Next(&left_tuple_, &left_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto predicate = plan_->Predicate();
  std::vector<Value> values;
  if (!is_left_selected_) {
    return false;
  }
  while (true) {
    while (right_executor_->Next(&right_tuple_, &right_rid_)) {
      if (predicate == nullptr || predicate
                                      ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                                     right_executor_->GetOutputSchema())
                                      .GetAs<bool>()) {
        for (auto &column : GetOutputSchema()->GetColumns()) {
          values.emplace_back(column.GetExpr()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                             &right_tuple_, right_executor_->GetOutputSchema()));
        }
        *tuple = Tuple(values, plan_->OutputSchema());
        *rid = left_rid_;
        return true;
      }
    }
    if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
      return false;
    }
    right_executor_->Init();
  }
}

}  // namespace bustub
