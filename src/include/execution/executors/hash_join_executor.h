//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {
struct JoinKey {
  /** The join keys */
  std::vector<Value> join_keys_;

  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.join_keys_.size(); i++) {
      if (join_keys_[i].CompareEquals(other.join_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct JoinVal {
  /** The join val */
  std::vector<Value> col_vals_;
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.join_keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {
/*
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto GetJoinKey(Tuple *tup, const std::vector<AbstractExpressionRef> &key_exprs,
                  const AbstractPlanNodeRef &plan) const -> JoinKey {
    std::vector<Value> res;
    res.reserve(key_exprs.size());
    auto &schema = plan->OutputSchema();
    for (const auto &expr : key_exprs) {
      res.emplace_back(expr->Evaluate(tup, schema));
    }
    return {res};
  }

  /** @return left join key */
  auto GetLeftJoinKey(Tuple *tup) const -> JoinKey {
    return GetJoinKey(tup, plan_->left_key_expressions_, plan_->GetLeftPlan());
  }

  /** @return right join key */
  auto GetRightJoinKey(Tuple *tup) const -> JoinKey {
    return GetJoinKey(tup, plan_->right_key_expressions_, plan_->GetRightPlan());
  }

  auto GetJoinValue(Tuple *tup) const -> JoinVal {
    std::vector<Value> res;
    auto schema = plan_->GetChildAt(0)->OutputSchema();
    for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
      res.emplace_back(tup->GetValue(&schema, i));
    }
    return {res};
  }

  struct JoinKeyValWraper {
    bustub::JoinKey join_key_;
    bustub::JoinVal join_val_;
    bool has_visited_{false};
  };

  auto TryGetJoinValueByJoinKeyFromht(const JoinKey &join_key)
      -> std::optional<std::vector<std::vector<JoinKeyValWraper>::iterator>>;

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::unordered_map<JoinKey, std::vector<JoinKeyValWraper>> ht_{};
  std::vector<std::vector<JoinKeyValWraper>::iterator> cur_matched_{};
  decltype(cur_matched_)::iterator cur_matched_it_;
  decltype(ht_)::iterator map_it_{ht_.begin()};
  std::vector<JoinKeyValWraper>::iterator vec_it_{};
  Tuple tup_{};
  RID id_{};
};

}  // namespace bustub
