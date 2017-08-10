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
		return md.name + (md.type == FileData::Type::Folder ? "/" : "");
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
	if (!mp.iterate(path, del))
		return MasterError::NotExists;

	auto Expired = [](const FileData & data) {
		return data.deleteTime != 0;
	};
	mp.remove_if(Expired);
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
	md.replicationFactor = 0;
	md.name = path.back();
	return ErrTranslator::extrieErrToMasterErr(mp.insert(path, std::move(md)));
}

std::pair<AFS::MasterError, AFS::ChunkHandle>
AFS::PathFileData::getHandle(const AFS::PathFileData::Path &path, std::uint64_t idx,
                             const std::function<bool(AFS::ChunkHandle)> &createChunk) {
	bool success = false;
	ChunkHandle ans;
	auto f = [&](FileData & data) {
		if (data.handles.size() < idx) // 添加一个也不够的情况
			return;
		if (data.handles.size() == idx) {
			data.handles.emplace_back(MemoryPool::instance().newChunk());
			if (!createChunk(data.handles.back().getHandle())) // 创建失败
				return;
		}
		success = true;
		ans = data.handles[idx].getHandle();
	};
	if (!mp.iterate(path, f) || !success)
		return std::make_pair(MasterError::Failed, ans);
	return std::make_pair(MasterError::OK, ans);
}

void AFS::PathFileData::reReplicate(std::priority_queue<std::pair<size_t, AFS::Address>> &pq,
                                    const std::function<AFS::GFSError(const AFS::Address &, const AFS::Address &,
                                                                      AFS::ChunkHandle)> &send) {
	auto re = [&](FileData & data) { // 对于某一份文件，检查其各个chunk，对于不够的复制
		if (data.type != FileData::Type::File)
			return ;

		for (auto &&chunk : data.handles) {
			auto handle = chunk.getHandle();
			ChunkData cdata;
			bool flag = MemoryPool::instance().return_if(handle, cdata,[&](const ChunkData & tdata) { // 如果副本服务器个数小于复制因子
				return tdata.location.size() < data.replicationFactor;
			});
			if (flag) {
				auto numAAddr = pq.top();
				while (std::find(cdata.location.begin(), cdata.location.end(),
					   numAAddr.second) != cdata.location.end()) {
					 pq.pop();
					 numAAddr = pq.top();
					 pq.push(std::make_pair(2 * (numAAddr.first - 1), numAAddr.second));
				} // 取一个当前location中没有的服务器
				//std::cerr << "Chunk " << handle << " is in danger.\n";
				GFSError err;
				// 让某一个服务器给目标服务器发送消息
				for (auto &&location : cdata.location) {
					   //std::cerr << "trying send " << location << " to " << numAAddr.second << std::endl;
					   err = send(location, numAAddr.second, handle);
					 if (err.errCode == GFSErrorCode::OK) {
						  break;
					 }
					 if (err.errCode == GFSErrorCode::OK) {
						  pq.pop();
						  pq.push(std::make_pair(2 * (numAAddr.first - 1), numAAddr.second));
					 }
				}
			}
		}
	};
	mp.iterate({}, re);
}
