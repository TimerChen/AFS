//
// Created by Aaron Ren on 28/07/2017.
//

#ifndef AFS_PATH_FILEDATA_H
#define AFS_PATH_FILEDATA_H

#include "common.h"
#include "setting.hpp"
#include "extrie.hpp"
#include "handle_chunkdata.h"
#include <fstream>
#include <queue>
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

	virtual void write(std::ofstream & out) const {
		out.write((char*)&type, sizeof(type));
		auto sz = (int)name.size();
		out.write((char*)&sz, sizeof(sz));
		for (auto &&ch : name)
			out.write(&ch, sizeof(ch));
		out.write((char*)&replicationFactor, sizeof(replicationFactor));
		out.write((char*)&deleteTime, sizeof(deleteTime));
	}

	virtual void read(std::ifstream & in) {
		in.read((char*)&type, sizeof(Type));

		int len;
		in.read((char*)&len, sizeof(int));
		auto buffer = new char[len];
		in.read(buffer, sizeof(char) * len);
		name = std::string(buffer, buffer + len);

		in.read((char*)&replicationFactor, sizeof(replicationFactor));
		in.read((char*)&deleteTime, sizeof(deleteTime));
	}
};

struct FileData : public FileDataBase {
	using Type = FileDataBase::Type;
	
	std::vector<MemoryPool::ChunkPtr> handles;

	void write(std::ofstream & out) const final {
		FileDataBase::write(out);
		auto sz = (int)handles.size();
		out.write((char*)&sz, sizeof(sz));
		for (auto &&item : handles)
			item.write(out);
	}

	void read(std::ifstream & in) final {
		FileDataBase::read(in);
		int sz;
		in.read((char*)&sz, sizeof(int));
		handles.resize(sz);
		for (auto &&item : handles)
			item.read(in);
	}
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
	          const std::function<void(ChunkHandle)> & createChunk);;

	MasterError createFolder(const Path & path);

	MasterError createFile(const Path & path);

	std::pair<MasterError, std::unique_ptr<std::vector<std::string>>>
	listName(const Path & path) const;

	MasterError deleteFile(const Path & path);

	void reReplicate(std::priority_queue<std::pair<size_t, Address>> & pq,
	const std::function<GFSError(const Address & /*src*/, const Address &, ChunkHandle)> & send) {
		auto foo = [&](FileData & data) {
			for (auto &&chunk : data.handles) {
				auto handle = chunk.getHandle();
				MemoryPool::instance().updateData_if(handle,
				[&](const ChunkData & cdata) {
					return cdata.location.size() < data.replicationFactor;
				},
				[&](ChunkData & cdata) {
					auto numAAddr = pq.top();
					GFSError err;
					for (auto &&location : cdata.location) {
						err = send(location, numAAddr.second, handle);
						if (err.errCode == GFSErrorCode::OK)
							break;
					}
					if (err.errCode == GFSErrorCode::OK) {
						pq.pop();
						pq.push(std::make_pair(numAAddr.first - 1, std::move(numAAddr.second)));
					}
				});
			}
		};
		mp.iterate({}, foo);
	}

	void read(std::ifstream & in) {
		mp.read(in);
	}
	void write(std::ofstream & out) const {
		mp.write(out);
	}

	void clear() {
		mp.clear();
	}

};
}

#endif //AFS_PATH_FILEDATA_H
