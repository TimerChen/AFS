//
// Created by Aaron Ren on 28/07/2017.
//

#ifndef AFS_PATH_FILEDATA_H
#define AFS_PATH_FILEDATA_H

#include "common.h"
#include "setting.hpp"
#include "extrie.hpp"
#include <vector>
#include <memory>
#include <utility>

// the mapping from paths to the file data

namespace AFS {

struct Filedata {
	enum class Type {
		System, Folder, File, Unknown
	};

	Type        type{Type::Unknown};
	std::string name;
	size_t      replicationFactor{3};
	std::vector<ChunkHandle>
			    handles;
	time_t      deleteTime{0};
};

class PathFiledata {
	using PathType = std::string;
	using Path     = std::vector<PathType>;
private:
	extrie<PathType, Filedata> mp;

	bool checkData(const Path & path) const;
public:
	std::pair<MasterError, Filedata>
	getData(const Path & path) const;

	std::pair<MasterError, std::unique_ptr<std::vector<std::string>>>
	listName(const Path & path) const;

};
}

#endif //AFS_PATH_FILEDATA_H
