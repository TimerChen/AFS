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
	static constexpr time_t TimeToDelete = 3 * 24 * 60 * 60;
	// a file which has been "deleted" after "TimeToDelete" seconds
	extrie<PathType, Filedata> mp;
	readWriteMutex m;

	bool checkData(const Path & path) const;
public:
	std::pair<MasterError, Filedata>
	getData(const Path & path) const;

	void deleteDeletedFiles() {
		auto Expired = [](const Filedata & data) {
			return data.deleteTime + TimeToDelete < time(nullptr);
		};
		mp.remove_if(Expired);
	}
};
}

#endif //AFS_PATH_FILEDATA_H
