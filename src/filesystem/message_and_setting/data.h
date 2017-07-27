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
#include <boost/thread/shared_mutex.hpp>
#include <map>
#include <mutex>
#include <vector>
#include <memory>
#include <utility>
#include <ctime>
#include <functional>

// chunk data
namespace AFS {
struct ChunkData {
	std::vector<ServerAddress>
				 location;    // 各副本位置
	uint         primary{0};  // 主chunk索引
	std::time_t  stamp{0};    // 租约到期时间
	ChunkVersion version;
};

class ChunkDataMap {
private:
	std::time_t lease_time{60}; // 默认租约时间(秒)以及每次延长至的时间
	std::map<ChunkHandle, ChunkData> mp;
	mutable boost::shared_mutex m;
	using readLock = boost::shared_lock<boost::shared_mutex>;
	using writeLock = std::unique_lock<boost::shared_mutex>;

public:
	void extendLease(const ChunkHandle & handle) {
		writeLock lk(m);
		mp[handle].stamp = time(nullptr) + lease_time;
	}

	bool expired(const ChunkHandle & handle, const ChunkHandle & version) {
		readLock rlk(m);
		auto & data = mp[handle];
		if (data.version > version)
			return true;
		rlk.unlock();
		writeLock wlk(m); // todo ???
		data.version = version;
		return false;
	}

	void removeHandle(const std::vector<ChunkHandle> & handles) {
		writeLock lk(m);
		for (auto &&handle : handles) {
			auto iter = mp.find(handle);
			if (iter != mp.end())
				mp.erase(iter);
		}
	}

	bool checkHandle(const ChunkHandle & handle) const {
		readLock lk(m);
		return mp.find(handle) != mp.cend();
	}

	std::pair<bool, ChunkData>
	getData(const ChunkHandle & handle) const {
		readLock lk(m);
		auto iter = mp.find(handle);
		if (iter == mp.end())
			return std::make_pair(false, ChunkData());
		return std::make_pair(true, iter->second);
	}

	std::pair<bool, ChunkData>
	getData(const ChunkHandle & handle,
	        const std::function<bool(const ChunkData & data)> & condition,
			const std::function<void(ChunkData & data)> & f) {
		writeLock lk(m);
		auto iter = mp.find(handle);
		if (iter == mp.end())
			return std::make_pair(false, ChunkData());
		if (condition(iter->second))
			f(iter->second);
		return std::make_pair(true, iter->second);
	};
};

}

// metadata
namespace AFS {

struct Metadata {
	struct FileData {
		int replication_factor = 3;
		std::vector<ChunkHandle> handles;
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

	Error deleteFile(const Path & path, std::function<void(const Metadata&)> del) {
		auto mdErr = getData(path);
		if (mdErr.first == Error::NotExist)
			return Error::NotExist;
		if (mdErr.second.type != Metadata::Type::file)
			return Error::NotExist;
		del(mdErr.second);
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


#endif //AFS_METADATA_H
