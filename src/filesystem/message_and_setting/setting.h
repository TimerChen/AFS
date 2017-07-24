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

using ServerAddress = int;
using Err = int;
using PathType = std::string;
using Path = std::vector<PathType>;
using Handle = void*;

}

// exceptions
namespace AFS {

class NonexistentFile : public std::exception {};

}

#endif //AFS_SETTING_H
