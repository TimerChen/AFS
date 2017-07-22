//
// Created by Aaron Ren on 22/07/2017.
//

#ifndef AFS_METADATA_H
#define AFS_METADATA_H

#include "extrie.hpp"
#include "setting.h"
#include <string>
#include <boost/thread.hpp>
#include <map>

// metadata
namespace AFS {

struct Metadata {
    struct ChunkData {
        Handle        handle;
        ServerAddress address;
    } chunk_data;

    enum class Type {
        folder, file, system
    } type;
    // todo
};

class MetadataContainer {
private:
    extrie<PathType, Metadata> ext;

public:
    template <class Md>
    bool mkdir(const Path &path, Md && md) {
        return ext.insert(path, std::forward(md));
    }

    template <class Md>
    bool create(const Path & path, Md && md) {
        return ext.insert(path, std::forward(md));
    }

    bool checkData(const Path & path) const {
        return ext.check(path);
    }

    Metadata getData(const Path & path) const {
        return ext[path];
    }
};

}

// chunk server data
namespace AFS {

struct ChunkServerData {

};

class ChunkServerDataContainer {
    using readLock = boost::shared_lock<boost::shared_mutex>;
    using writeLock = std::unique_lock<boost::shared_mutex>;
private:
    std::map<ServerAddress, ChunkServerData> csd;
	boost::shared_mutex m;
public:
	std::pair<ChunkServerData, bool>
	getDate(const ServerAddress & address) {
		auto lk = readLock(m);
		auto iter = csd.find(address);
		if (iter == csd.end())
			return std::make_pair(ChunkServerData(), false);
		return std::make_pair(iter->second, true);
	}

	template <class Csd>
	bool writeData(const ServerAddress & address, Csd && d) {
		auto lk = writeLock(m);
		bool success = csd.insert(std::make_pair(address, std::forward(d))).second;
		return success;
	}
};

}


#endif //AFS_METADATA_H
