//
// Created by Aaron Ren on 28/07/2017.
//

#include "path_filedata.h"

bool AFS::PathFileData::checkData(const AFS::PathFileData::Path &path) const {
	return mp.check(path);
}

std::pair<AFS::MasterError, AFS::FileDataCopy>
AFS::PathFileData::getData(const AFS::PathFileData::Path &path) const {
	if (!checkData(path))
		return std::make_pair(MasterError::NotExists, FileDataCopy());
	return std::make_pair(MasterError::OK, FileDataCopy(mp[path]));
}

void AFS::PathFileData::deleteDeletedFiles() {
	auto Expired = [](const FileData & data) {
		return data.deleteTime + TimeToDelete < time(nullptr);
	};
	mp.remove_if(Expired);
}

std::pair<AFS::MasterError, std::unique_ptr<std::vector<std::string>>>
AFS::PathFileData::listName(const AFS::PathFileData::Path &path) const {
	auto errMd = getData(path);
	auto result2 = std::make_unique<std::vector<std::string>>();
	if (errMd.first == MasterError::NotExists)
		return std::make_pair(MasterError::NotExists, std::move(result2));
	if (errMd.second.type != FileData::Type::Folder)
		return std::make_pair(MasterError::NotExists, std::move(result2));
	auto collect_name = [](const FileData & md)->std::string {
		return md.name;
	};
	result2 = mp.nonsub_collect<std::string>(path, collect_name);
	return std::make_pair(MasterError::OK, std::move(result2));
}

AFS::MasterError AFS::PathFileData::deleteFile(const AFS::PathFileData::Path &path) {
	auto errData = getData(path);
	if (errData.first == MasterError::NotExists)
		return MasterError::NotExists;
	if (errData.second.type != FileData::Type::File)
		return MasterError::NotExists;
	auto del = [](FileData &data) {
		data.deleteTime = time(nullptr);
	};
	mp.iterate(path, del);
	auto Expired = [](const FileData & data) {
		return data.deleteTime != 0;
	};
	mp.remove_if(Expired);
	// todo err
	return MasterError::OK;
}

AFS::MasterError AFS::PathFileData::createFile(const AFS::PathFileData::Path &path) {
	FileData md;
	md.type = FileData::Type::File;
	md.name = path.back();
	return ErrTranslator::extrieErrToMasterErr(mp.insert(path, std::move(md)));
}

AFS::MasterError AFS::PathFileData::createFolder(const AFS::PathFileData::Path &path) {
	FileData md;
	md.type = FileData::Type::Folder;
	md.name = path.back();
	return ErrTranslator::extrieErrToMasterErr(mp.insert(path, std::move(md)));
}

std::pair<AFS::MasterError, AFS::ChunkHandle>
AFS::PathFileData::getHandle(const AFS::PathFileData::Path &path, std::uint64_t idx,
                             const std::function<void(AFS::ChunkHandle)> &createChunk) {
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
}
