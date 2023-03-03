#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  auto vec_node = Walk(key, root_);
  if (root_ == nullptr) { return nullptr; }
  if (key.empty()) {
    if (root_->is_value_node_) {
      auto ptr = dynamic_cast<const TrieNodeWithValue<T> *>(root_.get());
      if (ptr) {
        return ptr->value_.get();
      }
    }
    return nullptr;
  }

  int n = key.size();
  int m = vec_node.size();
  // key路径存在
  if (m == n) {
    auto ptr_cur_node = vec_node[m - 1];
    auto node = ptr_cur_node->children_.at(key[m - 1]);
    if (node->is_value_node_) {
      auto ptr = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
      if (ptr) {
        return ptr->value_.get();
      }
      return nullptr;
    }
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  auto vec_node = Walk(key, root_);
  std::shared_ptr<const TrieNode> new_root;
  int n = key.size();
  int m = vec_node.size();

  if (vec_node.empty()) {
    if (n == 0) {
      auto ptr_value = std::make_shared<T>(std::move(value));
      if (root_) {
        auto ptr = root_->Clone();
        auto root = std::make_shared<TrieNodeWithValue<T>>(ptr->children_, ptr_value);
        return Trie(root);
      }
      auto ptr_new_root = std::make_shared<TrieNodeWithValue<T>>(ptr_value);
      return Trie(ptr_new_root);
    }

    if (root_) {
      if (root_->is_value_node_ || !root_->children_.empty()) {
        new_root = root_->Clone();
        ++m;
      }
    } else {
      new_root = std::make_shared<TrieNode>();
      ++m;
    }

  } else {
    new_root = std::shared_ptr<const TrieNode>(root_->Clone());
  }

  // 复用前缀节点的子节点
  auto ptr_cur_node = new_root;
  for (int i = 1; i < m; ++i) {
    auto ptr_new_node = std::shared_ptr<TrieNode>(vec_node[i]->Clone());
    auto ptr = std::const_pointer_cast<TrieNode>(ptr_cur_node);
    ptr->children_[key[i - 1]] = ptr_new_node;

    ptr_cur_node = ptr_new_node;
  }

  // 构建新的路径节点
  for (int i = m - 1; i < n - 1; ++i) {
    std::shared_ptr<TrieNode> ptr_new_node;
    if (ptr_cur_node->children_.count(key[i])) {
      auto node = ptr_cur_node->children_.at(key[i]);
      if (!node->children_.empty() || node->is_value_node_) {
        auto ptr = node->Clone();
        ptr_new_node = std::move(ptr);
      } else {
        ptr_new_node = std::make_shared<TrieNode>();
      }
    } else {
      ptr_new_node = std::make_shared<TrieNode>();
    }
    auto ptr = std::const_pointer_cast<TrieNode>(ptr_cur_node);
    ptr->children_[key[i]] = ptr_new_node;

    ptr_cur_node = ptr_new_node;
  }

  // 构建值叶子节点
  std::shared_ptr<const TrieNode> ptr_new_node;
  if (ptr_cur_node->children_.count(key[n - 1])) {
    auto node = ptr_cur_node->children_.at(key[n - 1]);
    auto ptr_value = std::make_shared<T>(std::move(value));
    ptr_new_node = std::make_shared<TrieNodeWithValue<T>>(node->children_, ptr_value);
  } else {
    auto ptr_value = std::make_shared<T>(std::move(value));
    ptr_new_node = std::make_shared<TrieNodeWithValue<T>>(ptr_value);
  }

  auto ptr = std::const_pointer_cast<TrieNode>(ptr_cur_node);
  ptr->children_[key[n - 1]] = ptr_new_node;

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (root_ == nullptr) {
    return Trie(nullptr);
  }
  auto new_root = std::shared_ptr<const TrieNode>(root_->Clone());
  auto vec_node = Walk(key, new_root);

  int n = key.size();
  int m = vec_node.size();

  // key存在
  if (n == m) {
    auto node = vec_node[n - 1]->children_.at(key[n - 1]);
    int l = n;
    // 删除后原key保留的长度，因为路径可能存在TrinodeWithValue节点，如国key所指向node不为叶节点则保留所有的可以的所有路径
    if (node->children_.empty()) {
      for (int i = n - 1; i >= 0; --i) {
        auto ptr_cur_node = vec_node[i];
        if (ptr_cur_node->is_value_node_ || ptr_cur_node->children_.size() > 1) {
          break;
        }
        --l;
      }
      if (l == 0) {
        if (root_->children_.empty()) { return Trie(nullptr); }
        new_root = std::shared_ptr(root_->Clone());
        auto p = std::const_pointer_cast<TrieNode>(new_root);
        p->children_.erase(key[0]);
        return Trie(new_root);
      }
      auto ptr_cur_node = new_root;
      for (int i = 1; i < l; ++i) {
        auto ptr_new_node = std::shared_ptr<const TrieNode>(vec_node[i]->Clone());
        auto ptr = std::const_pointer_cast<TrieNode>(ptr_cur_node);

        ptr->children_[key[i - 1]] = ptr_new_node;
        ptr_cur_node = ptr_new_node;
      }
      auto ptr = std::const_pointer_cast<TrieNode>(ptr_cur_node);
      ptr->children_.erase(key[l - 1]);
    } else {
      auto ptr_cur_node = new_root;
      for (int i = 1; i < n; ++i) {
        auto ptr_new_node = std::shared_ptr<const TrieNode>(vec_node[i]->Clone());
        auto ptr = std::const_pointer_cast<TrieNode>(ptr_cur_node);

        ptr->children_[key[i - 1]] = ptr_new_node;
        ptr_cur_node = ptr_new_node;
      }

      auto ptr_tail_node = std::shared_ptr<const TrieNode>(ptr_cur_node->children_.at(key[n - 1])->Clone());
      auto ptr_new_node = std::make_shared<TrieNode>(ptr_tail_node->children_);
      // auto ptr_temp = dynamic_cast<const TrieNode *>(ptr_tail_node.get());
      auto ptr = std::const_pointer_cast<TrieNode>(ptr_cur_node);
      ptr->children_[key[n - 1]] = ptr_new_node;
    }
  }

  return Trie(new_root);
}
auto Trie::Walk(std::string_view key, std::shared_ptr<const TrieNode> ptr_cur_node) const
    -> std::vector<std::shared_ptr<const TrieNode>> {
  std::vector<decltype(ptr_cur_node)> res;

  if (!ptr_cur_node) {
    return res;
  }

  int level = 0;
  int n = key.size();
  while (level < n && ptr_cur_node->children_.count(key[level]) != 0) {
    res.push_back(ptr_cur_node);
    ptr_cur_node = ptr_cur_node->children_.at(key[level++]);
  }
  return res;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
