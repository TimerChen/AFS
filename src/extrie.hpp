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
    struct Node {
        mapUPtr child;
        node_ptr pnt;
        T *value;
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

    node_ptr _find(const std::vector<U> & index) const {
        node_ptr p = header;
        typename std::map<U, node_ptr>::iterator tmp;
        for (auto && idx : index) {
            tmp = p->child.find(idx);
            if (tmp == p->child.end())
                return nullptr;
            p = tmp->second;
        }
        return p;
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
    class const_node_iterator;
    class node_iterator {
        friend class extrie;
        friend class const_node_iterator;

        node_ptr node;
        typename mapUPtr::iterator iter;

        node_iterator(node_ptr p, typename mapUPtr::iterator iter_)
                : node(p), iter(iter_) {}
    public:
        node_iterator() : node(nullptr) {}

        // iter++, etc.
        auto operator++(int) {
            auto tmp = *this;
            ++(*this);
            return tmp;
        }
        auto& operator++() {
            ++iter;
            return *this;
        }
        auto operator--(int) {
            auto tmp = *this;
            --(*this);
            return tmp;
        }
        auto & operator--() {
            ++iter;
            return *this;
        }

        // a operator to check whether two iterators are same (pointing to the same memory).
        T & operator*() const {
            return *(iter->second->value);
        }
        bool operator==(const node_iterator &rhs) const {
            return (node == rhs.node && iter == rhs.iter);
        }
        bool operator==(const const_node_iterator &rhs) const {
            return (node == rhs.node && iter == rhs.iter);
        }
        bool operator!=(const node_iterator &rhs) const {
            return !(*this == rhs);
        }
        bool operator!=(const const_node_iterator &rhs) const {
            return !(*this == rhs);
        }

        T* operator->() const noexcept {
            return &(operator*());
        }
    };
    class const_node_iterator {
        friend class extrie;
        friend class node_iterator;

        node_ptr node;
        typename mapUPtr::iterator iter;
        const_node_iterator(node_ptr p, typename mapUPtr::iterator iter)
                : node(p), iter(iter) {}
    public:
        const_node_iterator() : node(nullptr) {}
        const_node_iterator(const node_iterator &other)
                : node(other.node), iter(other.iter) {}

        // iter++, etc.
        auto operator++(int) {
            auto tmp = *this;
            ++(*this);
            return tmp;
        }
        auto& operator++() {
            ++iter;
            return *this;
        }
        auto operator--(int) {
            auto tmp = *this;
            --(*this);
            return tmp;
        }
        auto & operator--() {
            ++iter;
            return *this;
        }


        // a operator to check whether two iterators are same (pointing to the same memory).
        T & operator*() const {
            return *(iter->second->value);
        }
        bool operator==(const node_iterator &rhs) const {
            return (node == rhs.node && iter == rhs.iter);
        }
        bool operator==(const const_node_iterator &rhs) const {
            return (node == rhs.node && iter == rhs.iter);
        }
        bool operator!=(const node_iterator &rhs) const {
            return !(*this == rhs);
        }
        bool operator!=(const const_node_iterator &rhs) const {
            return !(*this == rhs);
        }

        T* operator->() const noexcept {
            return &(operator*());
        }
    };

public:
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

    size_t size() const {
        return sz;
    }
    bool empty() const {
        return !sz;
    }

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

    // 检查对应键值是否存在
    bool check(const std::vector<U> & index) const {
        return _find(index) != nullptr;
    }

    // 返回index对应值
    T & operator[](const std::vector<U> & index) {
        insert(index, T());
        return *_find(index)->value;
    }

    // 返回对应结点的begin(), end()
    // 用于遍历一个文件夹
    std::pair<node_iterator, node_iterator>
    get_node_iter(const std::vector<U> & index) const {
        node_ptr p = _find(index);
        if (!p)
            return std::make_pair(node_iterator(), node_iterator());
        node_iterator beg(p, p->child.begin());
        node_iterator end(p, p->child.end());
        return std::make_pair(std::move(beg), std::move(end));
    };

    // 类似于 cd, cd ..
    node_iterator enter(const node_iterator &iter, const U & idx) const {
        node_ptr nxt = iter.node->child.find(idx);
        return node_iterator(nxt, nxt->child.begin());
    }
    node_iterator exit(const node_iterator &iter) const {
        node_ptr prev = iter.node->pnt;
        return node_iterator(prev, prev->child.begin());
    }
};
}

#endif //AFS_EXTRIE_HPP
