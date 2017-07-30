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

void AFS::Master::checkDeadChunkServer() {
	asdm.checkDeadChunkServer();
}

void AFS::Master::collectGarbage() {
	pfdm.deleteDeletedFiles();
}

size_t AFS::Master::leaseExtend(const Address &addr, const std::vector<AFS::ChunkHandle> &handles) {
	auto permission = [addr = addr](const ChunkData & data)->bool {
		return (data.leaseGrantTime != 0) && (addr == data.primary);
	};
	auto extendLease = [](ChunkData & data) {
		data.leaseGrantTime = time(nullptr);
	};
	size_t cnt = 0;
	for (auto &&handle : handles) {
		cnt += (size_t)MemoryPool::instance().updateData_if(handle, permission, extendLease);
	}
	return cnt;
}

AFS::MasterError AFS::Master::updateChunkServer(const AFS::Address &addr,
                                                const std::vector<std::tuple<AFS::ChunkHandle, AFS::ChunkVersion>> &chunks) {
	asdm.updateChunkServer(addr, chunks);
	// todo err
	return MasterError::OK;
}

std::unique_ptr<std::vector<AFS::ChunkHandle>>
AFS::Master::checkGarbage(const std::vector<std::tuple<AFS::ChunkHandle,
		AFS::ChunkVersion>> &chunks) {
	auto result = std::make_unique<std::vector<ChunkHandle>>();
	auto expired = [](ChunkVersion version, const ChunkData & data) {
		return version < data.version;
	};
	for (auto &&chunk : chunks) {
		if (MemoryPool::instance().checkData(std::get<0>(chunk),
		                                     std::bind(expired, std::get<1>(chunk), std::placeholders::_1)))
			result->push_back(std::get<0>(chunk));
	}
	return result;
}

void AFS::Master::deleteFailedChunks(const AFS::Address &addr,
                                     const std::vector<AFS::ChunkHandle> &chunks) {
	asdm.deleteFailedChunks(addr, chunks);
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

std::tuple<AFS::GFSError, AFS::ChunkHandle>
AFS::Master::RPCGetChunkHandle(std::string path_str, std::uint64_t chunkIndex) {
	auto path = PathParser::instance().parse(path_str);
	auto createChunk = [&](ChunkHandle handle){
		Address addr;
		GFSError err;
		do {
			addr = asdm.chooseServer();
			// todo wtf
//				err = srv.RPCCall({addr, (uint16_t)0}, "CreateChunk", handle).get();
		} while (err.errCode != GFSErrorCode::OK);
		asdm.addChunk(addr, handle);
	};
	auto errMd = pfdm.getHandle(*path, chunkIndex, createChunk);
	return std::make_tuple(
			ErrTranslator::masterErrTOGFSError(errMd.first),
			errMd.second
	);
}
