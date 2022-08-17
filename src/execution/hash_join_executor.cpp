//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  uint32_t i;
  left_executor_->Init();
  while (left_executor_->Next(&tuple, &rid)) {
    Value key_value = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, plan_->GetLeftPlan()->OutputSchema());
    std::vector<Value> values;
    for (i = 0; i < plan_->GetLeftPlan()->OutputSchema()->GetColumnCount(); i++) {
      values.emplace_back(tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), i));
    }
    auto key = HashJoinKey{key_value};
    if (hash_map_.count(key) == 0) {
      hash_map_.insert({key, {values}});
    } else {
      hash_map_[key].emplace_back(std::move(values));
    }
  }

  right_executor_->Init();
  left_idx_ = 0;
  left_vectors_.clear();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  uint32_t i;
  while (true) {
    if (left_idx_ < left_vectors_.size()) {
      break;
    }
    if (!right_executor_->Next(tuple, rid)) {
      return false;
    }
    Value key_value = plan_->RightJoinKeyExpression()->Evaluate(tuple, plan_->GetRightPlan()->OutputSchema());
    auto key = HashJoinKey{key_value};
    if (hash_map_.find(key) != hash_map_.end()) {
      right_vector_.clear();
      for (i = 0; i < plan_->GetRightPlan()->OutputSchema()->GetColumnCount(); i++) {
        right_vector_.emplace_back(tuple->GetValue(plan_->GetRightPlan()->OutputSchema(), i));
      }
      left_idx_ = 0;
      left_vectors_ = hash_map_[key];
      break;
    }
  }
  std::vector<Value> values;
  auto left_tuple = Tuple(left_vectors_[left_idx_], plan_->GetLeftPlan()->OutputSchema());
  auto right_tuple = Tuple(right_vector_, plan_->GetRightPlan()->OutputSchema());
  for (auto &column : GetOutputSchema()->GetColumns()) {
    values.emplace_back(column.GetExpr()->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                                       plan_->GetRightPlan()->OutputSchema()));
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  left_idx_ += 1;
  return true;
}

}  // namespace bustub
