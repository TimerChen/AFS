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
#include <vector>
#include <utility>

// metadata
namespace AFS {

struct Metadata {
	struct FileData {
		int replication_factor = 3;

		struct ChunkData {
			Handle        handle;
			ServerAddress address;
		};

		std::vector<ChunkData> chunkData;
	};
	struct FolderData {};

	enum class Type {
		folder, file, system
	} type;

	FileData   fileData;
	FolderData folderData;
	// todo
};

class MetadataContainer {
public:
	using Error = extrie<PathType, Metadata>::Error;
private:
	extrie<PathType, Metadata> ext;

public:
	Error createFolder(const Path & path) {
		Metadata md;
		md.type = Metadata::Type::folder;
		// todo
		return ext.insert(path, std::move(md));
	}

	Error createFile(const Path & path) {
		Metadata md;
		md.type = Metadata::Type::file;
		// todo
		return ext.insert(path, std::move(md));
	}

	bool checkData(const Path & path) const {
		return ext.check(path);
	}

	std::pair<Metadata, bool>
	getData(const Path & path) const {
		if (!checkData(path))
			return std::make_pair(Metadata(), false);
		return std::make_pair(ext[path], true);
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
