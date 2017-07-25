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
		return result;
	}

};


}

#endif //AFS_PARSER_HPP
