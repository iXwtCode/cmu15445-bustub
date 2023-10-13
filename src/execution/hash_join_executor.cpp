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
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  left_child_->Init();
  while (left_child_->Next(&tup_, &id_)) {
    auto join_key = GetLeftJoinKey(&tup_);
    auto join_val = GetJoinValue(&tup_);
    ht_[join_key].emplace_back(JoinKeyValWraper{join_key, join_val, false});
  }
  std::cout << ht_.size() << "\n";
}

void HashJoinExecutor::Init() {
  right_child_->Init();
  map_it_ = ht_.begin();
  if (map_it_ != ht_.end()) {
    vec_it_ = map_it_->second.begin();
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto right_schema = plan_->GetRightPlan()->OutputSchema();
  auto out_schema = GetOutputSchema();
  while (true) {
    if (cur_matched_it_ == cur_matched_.end()) {  // 当前 match 的 key 的 tup_ 输出完了，得到下一个 match key 的所有tup
      std::optional<std::vector<std::vector<JoinKeyValWraper>::iterator>> opt;
      while (right_child_->Next(&tup_, &id_)) {
        auto right_join_key = GetRightJoinKey(&tup_);
        opt = TryGetJoinValueByJoinKeyFromht(right_join_key);
        if (opt.has_value()) {
          break;
        }
      }
      if (!opt.has_value()) {
        break;
      }
      cur_matched_ = opt.value();
      cur_matched_it_ = cur_matched_.begin();
    }

    while (cur_matched_it_ != cur_matched_.end()) {
      (*cur_matched_it_)->has_visited_ = true;
      auto left_join_val = (*cur_matched_it_)->join_val_;
      std::vector<Value> out_vec;
      for (auto val : left_join_val.col_vals_) {
        out_vec.emplace_back(val);
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        out_vec.emplace_back(tup_.GetValue(&right_schema, i));
      }
      *tuple = Tuple(out_vec, &out_schema);
      *rid = tuple->GetRid();
      ++cur_matched_it_;
      return true;
    }
  }

  if (!right_child_->Next(&tup_, &id_) && plan_->GetJoinType() == JoinType::LEFT) {
    while (true) {
      //      std::cout << "enter left\n";
      if (map_it_ == ht_.end()) {
        break;
      }
      if (vec_it_ == map_it_->second.end()) {
        std::cout << "next map_it_\n";
        if (++map_it_ == ht_.end()) {
          break;
        }
        vec_it_ = map_it_->second.begin();
      }
      while (vec_it_ != map_it_->second.end()) {
        if (!vec_it_->has_visited_) {
          std::cout << "unvisited\n";
          auto left_join_val = vec_it_->join_val_;
          std::vector<Value> out_vec;
          // 构建输出 tuple
          for (auto val : left_join_val.col_vals_) {
            out_vec.emplace_back(val);
          }
          for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
            auto type = right_schema.GetColumn(i).GetType();
            out_vec.emplace_back(ValueFactory::GetNullValueByType(type));
          }
          *tuple = Tuple(out_vec, &out_schema);
          *rid = tuple->GetRid();
          ++vec_it_;
          return true;
        }
        ++vec_it_;
      }
    }
  }
  return false;
}

auto HashJoinExecutor::TryGetJoinValueByJoinKeyFromht(const JoinKey &join_key)
    -> std::optional<std::vector<std::vector<JoinKeyValWraper>::iterator>> {
  using ret_type = std::vector<std::vector<JoinKeyValWraper>::iterator>;
  ret_type res;
  if (ht_.count(join_key) != 0) {
    //    std::cout << "has join key\n";
    auto &l = ht_[join_key];
    for (auto it = l.begin(); it != l.end(); ++it) {
      if (it->join_key_ == join_key) {
        res.emplace_back(it);
      }
    }
    return res;
  }
  return std::nullopt;
}

}  // namespace bustub