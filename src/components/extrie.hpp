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

enum class ExtrieError {
	OK = 0, Exist, NotExist, FUCKU
};

template<class U, class T>
class extrie {
private:
	struct Node;
	using node_ptr = Node *;
	using mapUPtr = std::map<U, node_ptr>;
	using indexIter = typename std::vector<U>::const_iterator;
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
	node_ptr get_node();
	void put_node(node_ptr p);

	void init();
	void destroy(node_ptr p);

	std::pair<node_ptr, std::unique_ptr<std::vector<readLock>>>
	_find(indexIter beg, const indexIter & end) const {
		auto rlks = std::make_unique<std::vector<readLock>>();
		rlks->emplace_back(readLock(header->m));

		node_ptr p = header;
		typename std::map<U, node_ptr>::iterator tmp;
		for (; beg != end; ++beg) {
			auto & idx = *beg;
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
	void _iterate(node_ptr p, const std::function<void(T&)> & fcs) {
		writeLock wlk(p->m);
		for (auto &&item : p->child) {
			auto ptr = item.second;
			_iterate(ptr, fcs);
		}
		if (p == header)
			return;
		fcs(*p->value);
	}
	template <class D>
	void _collect(node_ptr p, std::unique_ptr<std::vector<D>> vec,
	              const std::function<bool(const T&)> &condition,
	              const std::function<D(const T&)> &fc) const {
		readLock rlk(p->m);
		for (auto &&item : p->child)
			_collect(item.second, vec, condition, fc);
		if (p == header)
			return;
		if (condition(*p->value))
			vec->push_back(fc(*p->value));
	}

	void _remove_if(node_ptr p, node_ptr pnt, const U & idx,
	                const std::function<bool(const T&)> & condition) {
		writeLock wlk(p->m);
		for (auto &&item : p->child)
			_remove_if(item.second, p, item.first, condition);
		if (p == header)
			return;
		if (condition(*p->value)) {
			pnt->child.erase(pnt->child.find(idx));
			put_node(p);
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
	ExtrieError insert(indexIter beg, const indexIter end, TT && value) {
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

		/* 对于一次创建a/b/c，可以把a/，a/b/，和a/b/c理解为点，
		 * 而将a，b，c理解为边。这样p就是当前所处的结点，而beg
		 * 就是下一条要走的边。我们希望获得最后两个点（一个已有，
		 * 一个需新建）的写锁，也就是说，如果当前要走的边是最后一条
		 * 的话，那么我们要获得这个点的写锁。之后我们走过这条边，
		 * 对于这条边，走过它意味着创建一个新的结点，然后我们获得这个
		 * 新创建结点的写锁。
		 */
		for (; beg < end - 1; ++beg) {
			rlks->emplace_back(readLock(p->m));
			auto &idx = *beg;
			iter = p->child.find(idx);
			if (iter == p->child.end())
				return ExtrieError::NotExist;
			p = iter->second;
		}
		wlks->emplace_back(writeLock(p->m));
		bool insert = false;
		tie(iter, insert) = p->child.insert(std::make_pair(*(end - 1), get_node()));
		if (!insert)
			return ExtrieError::Exist;
		iter->second->pnt = p;
		p = iter->second;
		wlks->emplace_back(writeLock(p->m));
		p->value = new T(std::forward<TT>(value));
		++sz;
		return ExtrieError::OK;
	}
	template <class TT>
	ExtrieError insert(const std::vector<U> & index, TT && value) {
		return insert(index.cbegin(), index.cend(), std::forward<TT>(value));
	};

	// 返回remove是否成功
	ExtrieError remove(indexIter beg, const indexIter & end) {
		auto rlks = std::make_unique<std::vector<readLock>>();
		auto wlks = std::make_unique<std::vector<writeLock>>();

		node_ptr p = header;
		typename std::map<U, node_ptr>::iterator iter;

		for (; beg < end - 1; ++beg) {
			rlks->emplace_back(readLock(p->m));
			auto &idx = *beg;
			iter = p->child.find(idx);
			if (iter == p->child.end())
				return ExtrieError::NotExist;
			p = iter->second;
		}
		rlks->emplace_back(readLock(p->m));
		iter = p->child.find(*(end - 1));
		if (iter == p->child.end())
			return ExtrieError::NotExist;
		rlks->pop_back();
		wlks->push_back(writeLock(p->m));
		p->child.erase(iter);
		wlks->pop_back();
		destroy(iter->second);

		return ExtrieError::OK;
	}
	ExtrieError remove(const std::vector<U> & index) {
		return remove(index.begin(), index.end());
	}

	// 检查对应键值是否存在
	bool check(indexIter beg, const indexIter & end) const {
		return _find(beg, end).first != nullptr;
	}
	bool check(const std::vector<U> & index) const {
		return check(index.cbegin(), index.cend());
	}

	// 返回index对应值
	const T& at(indexIter beg, const indexIter & end) const {
		auto ptr_vec = _find(beg, end);
		if (!ptr_vec.first->value)
			throw illegal_index();
		return *ptr_vec.first->value;
	}
	const T& operator[](const std::vector<U> & index) const {
		return at(index.cbegin(), index.cend());
	}

	// 用于遍历一个文件夹，对每一个文件进行操作
	void iterate(const std::vector<U> & index,
	             const std::function<void(T&)> & fcs) {
		node_ptr p;
		std::unique_ptr<std::vector<readLock>> rlks;
		tie_move(p, rlks, _find(index.begin(), index.end()));
		rlks->pop_back();
		_iterate(p, fcs);
	}

	template <class D>
	std::unique_ptr<std::vector<D>>
	nonsub_collect(const std::vector<U> & index, const std::function<D(const T&)> & fc) const {
		node_ptr p;
		std::unique_ptr<std::vector<readLock>> rlks;
		tie_move(p, rlks, _find(index.begin(), index.end()));
		auto result = std::make_unique<std::vector<D>>();
		for (auto &&item : p->child) {
			readLock lk(item.second->m);
			result->push_back(fc(*item.second->value));
		}
		return result;
	}

	template <class D>
	std::unique_ptr<std::vector<D>>
	collect_if(const std::vector<U> & index,
	           std::function<bool(const T&)> condition,
	           std::function<D(const T&)> fc) const {
		node_ptr p;
		std::unique_ptr<std::vector<readLock>> rlks;
		tie_move(p, rlks, _find(index.begin(), index.end()));
		auto result = std::make_unique<std::vector<D>>();
		rlks->pop_back();
		_collect(p, result, condition, fc);
		return result;
	}

	void remove_if(const std::function<bool(const T&)> & condition) {
		_remove_if(header, nullptr, U(), condition);
	}

	// 用于对某一特定文件进行操作
	void operate(const std::vector<U> & index, std::function<void(T&)> f);

	// 选择fn结果最高者返回
	T choose(const std::vector<U> & index, std::function<int(const T&)> fn) const;
};
}

// implement
namespace AFS {

template <class U, class V>
typename extrie<U, V>::node_ptr extrie<U, V>::get_node() {
	auto result = new Node;
	result->value = nullptr;
	result->pnt = header;
	return result;
}

template <class U, class V>
void extrie<U, V>::put_node(node_ptr p) {
	delete p->value;
	delete p;
}

template <class U, class V>
void extrie<U, V>::init() {
	sz = 0;
	header = get_node();
}

template <class U, class V>
void extrie<U, V>::destroy(node_ptr p) {
	writeLock lk(p->m);
	for (auto &&item : p->child)
		destroy(item.second);
	lk.unlock();
	put_node(p);
	--sz;
}

}

#endif //AFS_EXTRIE_HPP
