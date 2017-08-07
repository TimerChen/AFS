//
// Created by Aaron Ren on 25/07/2017.
//

#ifndef AFS_PARSER_HPP
#define AFS_PARSER_HPP

#include <string>
#include <vector>
#include <memory>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

namespace AFS {

class PathParser {
private:
	PathParser() = default;

public:
	static PathParser& instance() {
		static PathParser in;
		return in;
	}

	std::unique_ptr<std::vector<std::string>>
	parse(const std::string & str) {
		auto result = std::make_unique<std::vector<std::string>>();
		boost::split(*result, str,boost::is_any_of("/"));
		if (result->begin()->empty())
			result->erase(result->begin());
		return result;
	}

};

class Convertor {
public:
	static std::string uintToString(std::uint64_t x) {
		std::string result;
		do {
			result.push_back(char(x % 10) + '0');
			x /= 10;
		} while (x);
		std::reverse(result.begin(), result.end());
		return result;
	}

	static std::uint64_t stringToUint(const std::string & str) {
		std::uint64_t result = 0;
		for (auto &&item : str) {
			result *= 10;
			result += item - '0';
		}
		return result;
	}
};

}

#endif //AFS_PARSER_HPP
