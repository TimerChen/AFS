//
// Created by Aaron Ren on 21/07/2017.
//

#include "master.h"
#include "parser.hpp"


AFS::GFSError AFS::Master::RPCMkdir(std::string path_str) {
	GFSError result;
	auto path = PathParser::instance().parse(path_str);
	MetadataContainer::Error err = mdc.createFile(*path);
	result.errCode = metadataErrToGFSErr(err);
	return result;
}

std::tuple<AFS::GFSError, std::vector<std::string>> AFS::Master::RPCListFile(std::string path_str) {
	std::tuple<GFSError, std::vector<std::string>> result;
	auto path = PathParser::instance().parse(path_str);
	auto errPtr = mdc.listName(*path);
	std::get<0>(result).errCode = metadataErrToGFSErr(errPtr.first);
	std::get<1>(result) = *errPtr.second;
	return result;
}

std::tuple<AFS::GFSError, AFS::ChunkHandle>
AFS::Master::RPCGetChunkHandle(std::string path_str, std::uint64_t chunkIndex) {
	GFSError err;
	auto path = PathParser::instance().parse(path_str);
	auto errMd = mdc.getData(*path);
	if (errMd.first != MetadataContainer::Error::OK) {
		err.errCode = metadataErrToGFSErr(errMd.first);
		return std::make_tuple(err, ChunkHandle());
	}
	if (errMd.second.type != Metadata::Type::file) {
		err.errCode = GFSErrorCode::WrongOperation;
		return std::make_tuple(err, ChunkHandle());
	}
	// todo chunk不够则创建
	auto & handles = errMd.second.fileData.handles;
	auto addresses = csdc.getSomeServers(chunkIndex - handles.size() + 1);
	for (auto &&address : *addresses)
		srv.RPCCall({address, Port}, "CreateChunk", chunkHandleID++);
	
	auto handle = errMd.second.fileData.handles[chunkIndex];
	return std::make_tuple(err, handle);
}

std::tuple<AFS::GFSError, std::vector<std::string>> AFS::Master::RPCGetReplicas(AFS::ChunkHandle handle) {
	GFSError err;
	auto flagData = cdm.getData(handle);
	if (!flagData.first) {
		err.errCode = GFSErrorCode::NoSuchChunk;
		return std::make_tuple(err, std::vector<std::string>());
	}
	err.errCode = GFSErrorCode::OK;
	return std::make_tuple(err, std::move(flagData.second.location));
}

AFS::GFSError AFS::Master::RPCCreateFile(std::string path_str) {
	/* 此操作仅仅是在元数据中创建对应的文件，而不会和
	 * chunk server交互，只有当真正往文件中写数据的
	 * 时候，才会通知某个chunk server来创建对应chunk
	 */
	GFSError result;
	auto path = PathParser::instance().parse(path_str);
	MetadataContainer::Error err = mdc.createFile(*path);
	result.errCode = metadataErrToGFSErr(err);
	return result;
}
