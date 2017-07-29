//
// Created by Aaron Ren on 21/07/2017.
//

#include "master.h"

AFS::Master::Master(LightDS::Service &srv, const std::string &rootDir)
		: Server(srv, rootDir)  {
	srv.RPCBind<std::tuple<GFSError, std::vector<ChunkHandle>>
			(std::vector<ChunkHandle>, std::vector<std::tuple<ChunkHandle, ChunkVersion>>, std::vector<ChunkHandle>)>
			("Heartbeat", std::bind(&Master::RPCHeartbeat, this,
			                        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

	srv.RPCBind<std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t>
			(ChunkHandle)>
			("GetPrimaryAndSecondaries", std::bind(&Master::RPCGetPrimaryAndSecondaries, this,
			                                       std::placeholders::_1));

	srv.RPCBind<std::tuple<GFSError, std::vector<std::string>>
			(ChunkHandle)>
			("GetReplicas", std::bind(&Master::RPCGetReplicas, this,
			                          std::placeholders::_1));

	srv.RPCBind<std::tuple<GFSError, bool, std::uint64_t, std::uint64_t>
			(std::string)>
			("GetFileInfo", std::bind(&Master::RPCGetFileInfo, this,
			                          std::placeholders::_1));

	srv.RPCBind<GFSError
			(std::string)>
			("CreateFile", std::bind(&Master::RPCCreateFile, this,
			                         std::placeholders::_1));

	srv.RPCBind<GFSError
			(std::string)>
			("DeleteFile", std::bind(&Master::RPCDeleteFile, this,
			                         std::placeholders::_1));

	srv.RPCBind<GFSError
			(std::string)>
			("Mkdir", std::bind(&Master::RPCMkdir, this,
			                    std::placeholders::_1));

	srv.RPCBind<std::tuple<GFSError, std::vector<std::string>>
			(std::string)>
			("ListFile", std::bind(&Master::RPCListFile, this,
			                       std::placeholders::_1));

	srv.RPCBind<std::tuple<GFSError, ChunkHandle>
			(std::string, std::uint64_t)>
			("GetChunkHandle", std::bind(&Master::RPCGetChunkHandle, this,
			                             std::placeholders::_1, std::placeholders::_2));
}

size_t AFS::Master::leaseExtend(const std::string &addr, const std::vector<AFS::ChunkHandle> &handles) {
	size_t result = 0;
	for (auto &&handle : handles)
		result += (hcdm.extendLease(handle, addr) == MasterError::OK);
	return result;
}

std::unique_ptr<std::vector<AFS::ChunkHandle>>
AFS::Master::checkGarbage(const std::vector<std::tuple<AFS::ChunkHandle, AFS::ChunkVersion>> &chunks) {
	auto result = std::make_unique<std::vector<ChunkHandle>>();
	for (auto &&chunk : chunks) {
		if (!hcdm.checkChunkVersion(std::get<0>(chunk), std::get<1>(chunk)))
			result->push_back(std::get<0>(chunk));
	}
	return result;
}

void AFS::Master::deleteFailedChunks(const AFS::Address &addr, const std::vector<AFS::ChunkHandle> &chunks) {
	asdm.deleteFailedChunks(addr, chunks);
	hcdm.deleteFailedChunks(addr, chunks);
}

AFS::MasterError AFS::Master::updateChunkServer(const AFS::Address &addr,
                                                const std::vector<std::tuple<AFS::ChunkHandle, AFS::ChunkVersion>> &chunks) {
	asdm.updateChunkServer(addr, chunks);
	hcdm.updateChunkServer(addr, chunks);
	return MasterError::OK;
}

std::tuple<AFS::GFSError, std::vector<AFS::ChunkHandle>>
AFS::Master::RPCHeartbeat(std::vector<AFS::ChunkHandle> leaseExtensions,
                          std::vector<std::tuple<AFS::ChunkHandle, AFS::ChunkVersion>> chunks,
                          std::vector<AFS::ChunkHandle> failedChunks) {
	auto result1 = ErrTranslator::masterErrTOGFSError(updateChunkServer(srv.getRPCCaller(), chunks));
	auto success_num = leaseExtend(srv.getRPCCaller(), leaseExtensions);
	auto result2 = std::move(*checkGarbage(chunks).release());
	deleteFailedChunks(srv.getRPCCaller(), failedChunks);
	return std::make_tuple(GFSError(result1), result2);
}

void AFS::Master::checkDeadChunkServer() {
	asdm.checkDeadChunkServer(std::bind(&HandleChunkdata::eraseDeadServersChunk, &hcdm,
	                                    std::placeholders::_1, std::placeholders::_2));
}

AFS::GFSError AFS::Master::RPCMkdir(std::string path_str) {
	GFSError result;
	auto path = PathParser::instance().parse(path_str);
	MasterError err = pfdm.createFolder(*path);
	result.errCode = ErrTranslator::masterErrTOGFSError(err);
	return result;
}

std::tuple<AFS::GFSError, bool, uint64_t, uint64_t>
AFS::Master::RPCGetFileInfo(std::string path_str) {
	GFSError err;
	auto path = PathParser::instance().parse(path_str);
	auto errMd = pfdm.getData(*path);
	err.errCode = ErrTranslator::masterErrTOGFSError(errMd.first);
	if (errMd.first == MasterError::NotExists)
		return std::make_tuple(err, false, std::uint64_t(), std::uint64_t());
	if (errMd.second.type == Filedata::Type::Folder)
		return std::make_tuple(err, true, std::uint64_t(), std::uint64_t());
	// todo length
	return std::make_tuple(err, false, -1, errMd.second.handles.size());
}

std::tuple<AFS::GFSError, std::vector<std::string>>
AFS::Master::RPCListFile(std::string path_str) {
	std::tuple<GFSError, std::vector<std::string>> result;
	auto path = PathParser::instance().parse(path_str);
	auto errPtr = pfdm.listName(*path);
	std::get<0>(result).errCode = ErrTranslator::masterErrTOGFSError(errPtr.first);
	std::get<1>(result) = *errPtr.second;
	return result;
}

std::tuple<AFS::GFSError, std::vector<std::string>>
AFS::Master::RPCGetReplicas(AFS::ChunkHandle handle) {
	GFSError err;
	auto errData = hcdm.getData(handle);
	if (errData.first == MasterError::NotExists) {
		err.errCode = GFSErrorCode::NoSuchChunk;
		return std::make_tuple(err, std::vector<std::string>());
	}
	err.errCode = GFSErrorCode::OK;
	return std::make_tuple(err, std::move(errData.second.location));
}

AFS::GFSError AFS::Master::RPCDeleteFile(std::string path_str) {
	auto path = PathParser::instance().parse(path_str);
	return ErrTranslator::masterErrTOGFSError(pfdm.deleteFile(*path));
}
