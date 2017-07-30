//
// Created by Aaron Ren on 21/07/2017.
//

#ifndef AFS_SUGAR_HPP
#define AFS_SUGAR_HPP

#include <utility>

namespace AFS {

/// tie(x, y) = val; // val is a std::pair
template <class U, class V>
class TieTemplate {
private:
	U &a;
	V &b;
public:
	TieTemplate(U &a_, V &b_)
			: a(a_), b(b_) {}
	TieTemplate& operator=(const std::pair<U, V> & p) {
		a = p.first;
		b = p.second;
		return *this;
	}
};

template <class U, class V>
TieTemplate<U, V> tie(U & a, V & b) {
	return TieTemplate<U, V>(a, b);
};

template <class U, class V>
void tie_move(U & a, V & b, std::pair<U, V> && p) {
    a = std::move(p.first);
    b = std::move(p.second);
};

}

namespace AFS {
template <class T>
bool remove(vector<T> & vec, const T & val) {
	auto iter = std::find(vec.begin(), vec.end(), val);
	if (iter == vec.end())
		return false;
	while (iter != vec.end()) {
		*iter = std::move(*(iter + 1));
		++iter;
	}
	vec.pop_back();
	return true;
}
}

#endif //AFS_SUGAR_HPP
