//
// Created by Aaron Ren on 16/07/2017.
//

#ifndef AFS_EXTRIE_HPP
#define AFS_EXTRIE_HPP

#include <cstddef>
#include <vector>
#include <utility>
#include <map>
#include <boost/thread/shared_mutex.hpp>
#include <mutex>
#include <functional>
#include <memory>
#include <exception>
#include <boost/tuple/tuple.hpp>
#include "sugar.hpp"

namespace AFS {
class illegal_index : public std::exception {};
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
        mutable boost::shared_mutex m;
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
        writeLock lk(p->m);
        for (auto &&item : p->child)
            destroy(item.second);
        lk.unlock();
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

    size_t size() const {
        readLock rlk(header->m);
        return sz;
    }
    bool empty() const {
        readLock rlk(header->m);
        return !sz;
    }

    // 返回insert是否成功
    template <class TT>
    bool insert(const std::vector<U> & index, TT && value) {
        /* 考虑一次插入 a/b/c/d/e，真正被修改的是d的child数组以及
         * e本身，所以只需要获得d和e的写锁，而abc以及header则只需
         * 要获得读锁。
         * 实现如下：为了获得一系列锁，所以使用一个readLock的vector和
         * 两个writeLock来管理。首先将出发结点设置为header，然后遍历
         * index直到size-3，在每一次进入下一层结点之前获得当前结点的读锁。
         * 同理，再获得最后第二层的写锁。（注意插入路径为a的情况）
         */
        auto rlks = std::make_unique<std::vector<readLock>>();
        auto wlks = std::make_unique<std::vector<writeLock>>();

        node_ptr p = header;
        typename std::map<U, node_ptr>::iterator iter;
        int i = 0;
        auto foo = [&]() {
            auto &idx = index[i];
            iter = p->child.find(idx);
            if (iter == p->child.end())
                return false;
            p = iter->second;
            return true;
        };
        for (; i < (int)index.size() - 2; ++i) {
            rlks->emplace_back(readLock(p->m));
            if (!foo()) return false;
        }
        if (i != (int)index.size() - 1) {
            wlks->emplace_back(writeLock(p->m));
            if (!foo()) return false;
        }
        bool insert = false;
        tie(iter, insert) = p->child.insert(std::make_pair(index.back(), get_node()));
        if (!insert) return false;
        iter->second->pnt = p;
        p = iter->second;
        wlks->emplace_back(writeLock(p->m));
        p->value = new T(std::forward<TT>(value));
        ++sz;
        return true;
    };

    // todo
    // 返回remove是否成功
    bool remove(const std::vector<U> & index) {
        throw ;
        auto rlks = std::make_unique<std::vector<readLock>>();
        auto wlks = std::make_unique<std::vector<writeLock>>();

        node_ptr p = header;
        typename std::map<U, node_ptr>::iterator iter;
        int i = 0;
        auto foo = [&]() {
            auto &idx = index[i];
            iter = p->child.find(idx);
            if (iter == p->child.end())
                return false;
            p = iter->second;
            return true;
        };
        for (; i < (int)index.size() - 2; ++i) {
            rlks->emplace_back(readLock(p->m));
            if (!foo()) return false;
        }
        for (; i < (int)index.size(); ++i) {
//            wlk1 = writeLock(p->m);
            if (!foo()) return false;
        }
        --sz;
        p->pnt->child.erase(p->pnt->child.find(index.back()));
        put_node(p);
        return true;
    }

    // 检查对应键值是否存在
    bool check(const std::vector<U> & index) const {
        return _find(index).first != nullptr;
    }

    // 返回index对应值
    T operator[](const std::vector<U> & index) const {
        auto ptr_vec = _find(index);
        if (!ptr_vec.first->value)
            throw illegal_index();
        return *ptr_vec.first->value;
    }

    // 用于遍历一个文件夹，对每一个文件进行操作
    void iterate(const std::vector<U> & index,
                 std::vector<std::function<void(T&)>> fcs);

    // 用于对某一特定文件进行操作
	void operate(const std::vector<U> & index, std::function<void(T&)> f);

    // 选择fn结果最高者返回
    T choose(const std::vector<U> & index, std::function<int(const T&)> fn) const;
};
}

#endif //AFS_EXTRIE_HPP
