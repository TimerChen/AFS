//
// Created by Aaron Ren on 22/07/2017.
//

#ifndef AFS_METADATA_H
#define AFS_METADATA_H

#include "extrie.hpp"
#include "setting.h"
#include "common.h"
#include <string>
#include <boost/thread.hpp>
#include <map>
#include <vector>
#include <memory>
#include <utility>

// chunk data
namespace AFS {
struct ChunkData {
	ChunkHandle   handle;
	ChunkVersion  version;
};

class ChunkDataMap {
	std::map<ChunkHandle, ChunkData> mp;
};

}

// metadata
namespace AFS {

struct Metadata {
	struct FileData {
		int replication_factor = 3;
		std::vector<std::shared_ptr<ChunkData>> chunkData;
	};
	struct FolderData {};

	enum class Type {
		folder, file, system, unknown
	} type{Type::unknown};

	std::string name;
	FileData    fileData;
	FolderData  folderData;
	// todo
};

class MetadataContainer {
public:
	using Error = extrie<PathType, Metadata>::Error;
private:
	extrie<PathType, Metadata> ext;

	bool checkData(const Path & path) const {
		return ext.check(path);
	}
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

	Error deleteFile(const Path & path) {
		auto mdErr = getData(path);
		if (mdErr.first == Error::NotExist)
			return Error::NotExist;
		if (mdErr.second.type != Metadata::Type::file)
			return Error::NotExist;
		return ext.remove(path);
	}

	std::pair<Error, std::unique_ptr<std::vector<std::string>>>
	listName(const Path & path) const {
		auto errMd = getData(path);
		auto result2 = std::make_unique<std::vector<std::string>>();
		if (errMd.first == Error::NotExist)
			return std::make_pair(Error::NotExist, std::move(result2));
		if (errMd.second.type != Metadata::Type::folder)
			return std::make_pair(Error::NotExist, std::move(result2));
		auto collect_name = [](const Metadata & md)->std::string {
			return md.name;
		};
		result2 = ext.nonsub_collect<std::string>(path, collect_name);
		return std::make_pair(Error::OK, std::move(result2));
	};

	std::pair<Error, Metadata>
	getData(const Path & path) const {
		if (!checkData(path))
			return std::make_pair(Error::NotExist, Metadata());
		return std::make_pair(Error::OK, ext[path]);
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
