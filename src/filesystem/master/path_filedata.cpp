//
// Created by Aaron Ren on 28/07/2017.
//

#include "path_filedata.h"
#include <fstream>

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
		if (data.handles.size() < idx) { // 添加一个也不够的情况
//			std::cerr << "idx overflow\n";
			return;
		}
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

void AFS::PathFileData::read(std::ifstream &in) {
	mp.read(in);
}

void AFS::PathFileData::reReplicate(const std::vector<Address> & servers,
                                    const std::function<AFS::GFSError(const AFS::Address &, const AFS::Address &,
                                                                      AFS::ChunkHandle)> &send) {
	std::ofstream fout("./data/log.txt", std::ios::app);
	auto re = [&](FileData & data) { // 对于某一份文件，检查其各个chunk，对于不够的复制
		if (data.type != FileData::Type::File)
			return ;

		for (auto &&chunk : data.handles) {
			auto handle = chunk.getHandle();
			ChunkData cdata;

			// 检查副本服务器个数小于复制因子
			bool flag = MemoryPool::instance().return_if(handle, cdata,[&](const ChunkData & tdata) {
				return tdata.location.size() < data.replicationFactor;
			});
			if (flag) {
				fout << time(nullptr) << ", ";
				fout << "chunk " << handle << " is in danger.\n";
//				std::cerr << "chunk " << handle << " is in danger.\n";

				for (auto &&tar : servers) {
					bool success = false;
					for (auto &&src : cdata.location) {
						if (cdata.location.end()
						    != std::find(cdata.location.begin(), cdata.location.end(), tar))
							continue;

						fout << time(nullptr) << ", ";
						fout << "trying... send " << handle << " from " << src << " to " << tar << std::endl;
						GFSError gErr;
						try {
							gErr = send(src, tar, handle);
						} catch (...) {
							gErr.errCode = GFSErrorCode::TransmissionErr;
						}
						if (gErr.errCode == GFSErrorCode::OK) {
							fout << "succeed!\n";
							success = true;
							break;
						}
						fout << "failed, errCode = " << (int)gErr.errCode << std::endl;
					}
					if (success) {
						break;
					}
				}
			}
		}
	};
	mp.iterate_all(re);
}

void AFS::PathFileData::write(std::ofstream &out) const {
	mp.write(out);
}

void AFS::PathFileData::clear() {
	mp.clear();
}
