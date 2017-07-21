//
// Created by Aaron Ren on 16/07/2017.
//

#ifndef AFS_EXTRIE_HPP
#define AFS_EXTRIE_HPP

#include <cstddef>
#include <vector>
#include <exception>
#include <utility>
#include <map>
#include <boost/thread/shared_mutex.hpp>
#include <mutex>
#include <functional>
#include <memory>
#include "sugar.hpp"

namespace AFS {

/// 和trie的区别在于，通过vector<U>来索引，

template<class U, class T>
class extrie {
    friend class node_iterator;
    friend class const_node_iterator;
private:
    struct Node;
    using node_ptr = Node *;
    using mapUPtr = std::map<U, node_ptr>;
    using readLock = boost::shared_lock<boost::shared_mutex>;
    using writeLock = std::unique_lock<boost::shared_mutex>;

    struct Node {
        mapUPtr child;
        node_ptr pnt;
        T *value;
        boost::shared_mutex m;
    };

    // 保存的数据结构
    node_ptr header; // 根/空节点, end()节点
    size_t sz = 0;

private:
    node_ptr get_node() {
        node_ptr result = new Node;
        result->value = nullptr;
        result->pnt = header;
        return result;
    }
    void put_node(node_ptr p) {
        delete p->value;
        delete p;
    }

    void init() {
        sz = 0;
        header = get_node();
    }
    void destroy(node_ptr p) {
        for (auto &&item : p->child)
            destroy(item.second);
        put_node(p);
    }

    std::pair<node_ptr, std::unique_ptr<std::vector<readLock>>>
    _find(const std::vector<U> & index) const {
        auto rlks = std::make_unique<std::vector<readLock>>();
        rlks->emplace_back(readLock(header->m));

        node_ptr p = header;
        typename std::map<U, node_ptr>::iterator tmp;
        for (auto && idx : index) {
            tmp = p->child.find(idx);
            if (tmp == p->child.end())
                return std::make_pair(p, std::move(rlks));
            p = tmp->second;
            rlks->emplace_back(readLock(p->m));
        }
        return std::make_pair(p, std::move(rlks));
    }
    void copy(node_ptr p, const node_ptr & op) {
        if (op->value)
            p->value = new T(*op->value);
        for (auto &&iter : op->child) {
            node_ptr q = get_node();
            p->child.insert(std::make_pair(iter.first, q));
            copy(q, iter.second);
            q->pnt = p;
        }
    }

public:
    // todo
    extrie() {
        init();
    }
    extrie(const extrie & other) {
        init();
        sz = other.sz;
        copy(header, other.header);
    }
    extrie &operator=(const extrie & other) {
        if (this == &other)
            return *this;
        destroy(header);
        init();
        sz = other.sz;
        copy(header, other.header);
    }
    extrie(extrie && other) noexcept {
        (*this) = std::move(other);
    }
    extrie &operator=(extrie && other) noexcept {
        if (this == &other)
            return *this;
        header = other.header;
        sz = other.sz;
        other.header = nullptr;
        return *this;
    }
    ~extrie() {
        if (header)
            destroy(header);
    }

    // todo
    size_t size() const {
        return sz;
    }
    bool empty() const {
        return !sz;
    }

    // todo
    // 返回insert是否成功
    template <class TT>
    bool insert(const std::vector<U> & index, TT && value) {
        // using universal reference to avoid overloading for rvalue & lvalue

        node_ptr p = header;
        typename std::map<U, node_ptr>::iterator iter;
        bool insert_succeed = false;
        for (auto &&idx : index) {
            tie(iter, insert_succeed) = p->child.insert(std::make_pair(idx, node_ptr()));
            // 如果原来没有这个节点，则需要初始化新得到的这个节点
            if (insert_succeed) {
                iter->second = get_node();
                iter->second->pnt = p;
                ++sz;
            }
            p = iter->second;
        }
        if (!insert_succeed)
            return 0;
        iter->second->value = new T(std::forward<TT>(value));
        return 1;
    };

    // todo
    // 返回remove是否成功
    bool remove(const std::vector<U> & index) {
        node_ptr p = header;
        typename std::map<U, node_ptr>::iterator tmp;
        for (auto && idx : index) {
            tmp = p->child.find(idx);
            if (tmp == p->child.end())
                return false;
            p = tmp->second;
        }
        if (p->value == nullptr)
            return false;
        --sz;
        p->pnt->child.erase(p->pnt->child.find(index.back()));
        put_node(p);
        return true;
    }

    // todo
    // 检查对应键值是否存在
    bool check(const std::vector<U> & index) const {
        return _find(index) != nullptr;
    }

    // todo
    // 返回index对应值
    T & operator[](const std::vector<U> & index) {
        insert(index, T());
        return *_find(index)->value;
    }

    // 用于遍历一个文件夹
    void get_node_iter(const std::vector<U> & index, std::function fn) const;
};
}

#endif //AFS_EXTRIE_HPP
