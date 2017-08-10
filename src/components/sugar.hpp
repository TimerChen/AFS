//
// Created by Aaron Ren on 21/07/2017.
//

#ifndef AFS_SUGAR_HPP
#define AFS_SUGAR_HPP

#include <utility>
#include <vector>
#include <string>
#include <fstream>

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
bool remove(std::vector<T> & vec, const T & val) {
	auto iter = std::find(vec.begin(), vec.end(), val);
	if (iter == vec.end())
		return false;
	if (vec.size() == 1) {
		vec.clear();
		return true;
	}
	while (iter != vec.end() - 1) {
		*iter = *(iter + 1);
		++iter;
	}
	vec.pop_back();
	return true;
}
}

namespace AFS {
template <class U>
class IOTool {
public:
	void write(std::ofstream & out, const U & data) const {
		data.write(out);
	}

	void read(std::ifstream & in, U & data) {
		data.read(in);
	}
};

template <>
class IOTool<std::string> {
public:
	void write(std::ofstream & out, const std::string & data) const {
		auto sz = (int)data.size();
		out.write((char*)&sz, sizeof(sz));
		for (auto &&ch : data)
			out.write(&ch, sizeof(ch));
	}

	void read(std::ifstream & in, std::string & data) {
		int len;
		in.read((char*)&len, sizeof(int));
		auto buffer = new char[len];
		in.read(buffer, sizeof(char) * len);
		data = std::string(buffer, buffer + len);
	}
};

template <>
class IOTool<int> {
	void write(std::ofstream & out, const int & data) const {
		out.write((char*)&data, sizeof(data));
	}

	void read(std::ifstream & in, int & data) {
		in.read((char*)&data, sizeof(data));
	}
};

}

#endif //AFS_SUGAR_HPP
