//
// Created by Aaron Ren on 28/07/2017.
//

#ifndef AFS_PATH_FILEDATA_H
#define AFS_PATH_FILEDATA_H

#include "common.h"
#include "setting.hpp"
#include "extrie.hpp"
#include "handle_chunkdata.h"
#include <vector>
#include <memory>
#include <utility>

// the mapping from paths to the file data

namespace AFS {

struct FileDataBase {
	enum class Type {
		System, Folder, File, Unknown
	};

	Type        type{Type::Unknown};
	std::string name;
	size_t      replicationFactor{3};
	time_t      deleteTime{0};
};

struct FileData : public FileDataBase {
	using Type = FileDataBase::Type;
	
	std::vector<MemoryPool::FilePtr> handles;
};

struct FileDataCopy : public FileDataBase {
	using Type = FileDataBase::Type;

	std::vector<ChunkHandle> handles;

	FileDataCopy() = default;
	explicit FileDataCopy(const FileData & data) : FileDataBase(data) {
		handles.resize(data.handles.size());
		for (int i = 0; i < (int)handles.size(); ++i)
			handles[i] = data.handles[i].getHandle();
	}
	FileDataCopy & operator=(const FileDataCopy &) = default;
	FileDataCopy(const FileDataCopy &) = default;
	FileDataCopy(FileDataCopy &&) = default;
	FileDataCopy & operator=(FileDataCopy &&) = default;
	~FileDataCopy() = default;
};

class PathFileData {
	using PathType = std::string;
	using Path     = std::vector<PathType>;
private:
	static constexpr time_t TimeToDelete = 3 * 24 * 60 * 60;
	// a file which has been "deleted" after "TimeToDelete" seconds
	extrie<PathType, FileData> mp;

	bool checkData(const Path & path) const;
public:
	std::pair<MasterError, FileDataCopy>
	getData(const Path & path) const;

	void deleteDeletedFiles();

	std::pair<MasterError, ChunkHandle>
	getHandle(const Path & path, std::uint64_t idx,
	          const std::function<void(ChunkHandle)> & createChunk) {
		ChunkHandle ans;
		auto f = [&](FileData & data) {
			if (data.handles.size() < idx) // 添加一个也不够的情况
				return;
			if (data.handles.size() == idx) {
				data.handles.emplace_back(MemoryPool::instance().newChunk());
				createChunk(data.handles.back().getHandle());
			}
			ans = data.handles[idx].getHandle();
		};
		mp.iterate(path, f);
		// todo err
		return std::make_pair(MasterError::OK, ans);
	};

	MasterError createFolder(const Path & path) {
		FileData md;
		md.type = FileData::Type::Folder;
		return ErrTranslator::extrieErrToMasterErr(mp.insert(path, std::move(md)));
	}

	MasterError createFile(const Path & path) {
		FileData md;
		md.type = FileData::Type::File;
		return ErrTranslator::extrieErrToMasterErr(mp.insert(path, std::move(md)));
	}

	std::pair<MasterError, std::unique_ptr<std::vector<std::string>>>
	listName(const Path & path) const;

	MasterError deleteFile(const Path & path);
};
}

#endif //AFS_PATH_FILEDATA_H
