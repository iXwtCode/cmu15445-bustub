//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_plan.h
//
// Identification: src/include/execution/plans/hash_join_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "binder/table_ref/bound_join_ref.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

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


/**
 * Hash join performs a JOIN operation with a hash table.
 */
class HashJoinPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new HashJoinPlanNode instance.
   * @param output_schema The output schema for the JOIN
   * @param children The child plans from which tuples are obtained
   * @param left_key_expression The expression for the left JOIN key
   * @param right_key_expression The expression for the right JOIN key
   */
  HashJoinPlanNode(SchemaRef output_schema, AbstractPlanNodeRef left, AbstractPlanNodeRef right,
                   std::vector<AbstractExpressionRef> left_key_expressions,
                   std::vector<AbstractExpressionRef> right_key_expressions, JoinType join_type)
      : AbstractPlanNode(output_schema, {left, right}),
        left_key_expressions_{left_key_expressions},
        right_key_expressions_{right_key_expressions},
        join_type_(join_type) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::HashJoin; }

  /** @return The expression to compute the left join key */
  auto LeftJoinKeyExpressions() const -> const std::vector<AbstractExpressionRef> & { return left_key_expressions_; }

  /** @return The expression to compute the right join key */
  auto RightJoinKeyExpressions() const -> const std::vector<AbstractExpressionRef> & { return right_key_expressions_; }

  /** @return The left plan node of the hash join */
  auto GetLeftPlan() const -> AbstractPlanNodeRef  {
    BUSTUB_ASSERT(GetChildren().size() == 2, "Hash joins should have exactly two children plans.");
    return GetChildAt(0);
  }

  /** @return The right plan node of the hash join */
  auto GetRightPlan() const -> AbstractPlanNodeRef  {
    BUSTUB_ASSERT(GetChildren().size() == 2, "Hash joins should have exactly two children plans.");
    return GetChildAt(1);
  }

  /** @return The join type used in the hash join */
  auto GetJoinType() const -> JoinType { return join_type_; };

  /** @return left join key */
  auto GetLeftJoinKey(Tuple *tup) const -> JoinKey {
      return GetJoinKey(tup, left_key_expressions_, GetLeftPlan());
  }

  /** @return right join key */
  auto GetRightJoinKey(Tuple *tup) const -> JoinKey {
    return GetJoinKey(tup, right_key_expressions_, GetRightPlan());
  }

  auto GetJoinValue(Tuple *tup) const -> JoinVal {
    std::vector<Value> res;
    auto schema = children_[0]->OutputSchema();
    for(uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
      res.emplace_back(tup->GetValue(&schema, i));
    }
    return {res};
  }


  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(HashJoinPlanNode);

  /** The expression to compute the left JOIN key */
  std::vector<AbstractExpressionRef> left_key_expressions_;
  /** The expression to compute the right JOIN key */
  std::vector<AbstractExpressionRef> right_key_expressions_;

  /** The join type */
  JoinType join_type_;
 private:
  auto GetJoinKey(Tuple *tup, const std::vector<AbstractExpressionRef>& key_exprs, const AbstractPlanNodeRef& plan) const -> JoinKey {
    std::vector<Value> res;
    auto &schema = plan->OutputSchema();
    for (const auto& expr: key_exprs) {
      res.emplace_back(expr->Evaluate(tup, schema));
    }
    return {res};
  }

 protected:
  auto PlanNodeToString() const -> std::string override;
};
}  // namespace bustub

namespace  std {
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
}
