//
// Created by Aaron Ren on 21/07/2017.
//

#ifndef AFS_SETTING_H
#define AFS_SETTING_H

#include <vector>
#include <string>
#include <exception>

// alias
namespace AFS {
using uint = std::uint64_t;
using ServerAddress = std::string;
using PathType = std::string;
using Path = std::vector<PathType>;

}

// exceptions
namespace AFS {

class NonexistentFile : public std::exception {};

}

#endif //AFS_SETTING_H
