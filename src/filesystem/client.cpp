#include "client.h"

#include "error.hpp"
#include "master.h"
#include "message.hpp"

namespace AFS {

bool Client::checkMasterAddress() const {
	return !masterAdd.empty();
}

Client::Client(LightDS::Service &_srv) : srv(_srv) {}

ClientErr Client::fileCreate(const std::string &dir) {
	if(!checkMasterAddress())
		return ClientErr(ClientErrCode::MasterNotFound);
	auto err = srv.RPCCall({masterAdd, 0}, "CreateFile", dir).get().as<GFSError>();
	if (err.errCode == GFSErrorCode::OK)
		return ClientErr(ClientErrCode::OK);

	// handle err
	switch ((int)err.errCode) {
		case (int)GFSErrorCode::FileDirAlreadyExists:
			return ClientErr(ClientErrCode::FileDirAlreadyExists);
		case (int)GFSErrorCode::NoSuchFileDir:
			return ClientErr(ClientErrCode::NoSuchFileDir);
		default:
			throw ;
	}
}

ClientErr Client::fileMkdir(const std::string &dir) {
	if(!checkMasterAddress())
		return ClientErr(ClientErrCode::MasterNotFound);
	auto err = srv.RPCCall({masterAdd, 0}, "Mkdir", dir).get().as<GFSError>();
	if (err.errCode == GFSErrorCode::OK)
		return ClientErr(ClientErrCode::OK);

	// handle err
	switch ((int)err.errCode) {
		case (int)GFSErrorCode::FileDirAlreadyExists:
			return ClientErr(ClientErrCode::FileDirAlreadyExists);
		case (int)GFSErrorCode::NoSuchFileDir:
			return ClientErr(ClientErrCode::NoSuchFileDir);
		default:
			throw ;
	}
}

ClientErr Client::_fileAppend(const std::string &dir, const std::string &data) {
	GFSError gErr;
	std::uint64_t chunkIdx, handle, id, expire, offset;
	std::string primary;
	std::vector<std::string> secondaries;

	ClientErr err(ClientErrCode::Unknown);
	enum {
		GetFileInfo = 0, GetHandle, GetAddresses, PushData, AppendChunk,
		EndFlow
	};
	int cur = GetFileInfo;

	auto getFileInfo = [&]()->void {
		bool isDir;
		std::uint64_t len;
		std::tie(gErr, isDir, len, chunkIdx)
				= srv.RPCCall({masterAdd, 0}, "GetFileInfo", dir).get()
				.as<std::tuple<GFSError,
						bool /*IsDir*/,
						std::uint64_t /*Length*/,
						std::uint64_t /*Chunks*/>>();
		if (gErr.errCode == GFSErrorCode::NoSuchFileDir) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::NoSuchFileDir);
			return;
		}
		if (isDir) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::WrongOperation);
			return;
		}
		cur = GetHandle;
	};
	auto getHandle  = [&]()->void {
		std::tie(gErr, handle) =
				srv.RPCCall({masterAdd, 0}, "GetChunkHandle", dir,
				            chunkIdx).get().as<std::tuple<GFSError, ChunkHandle>>();
		switch ((int)gErr.errCode) {
			case (int) GFSErrorCode::OK:
				break;
			default:
				cur = EndFlow;
				err = ClientErr(ClientErrCode::Unknown);
				return;
		}
		cur = GetAddresses;
	};
	auto getAddresses = [&]()->void {
		std::tie(gErr, primary, secondaries, expire)
				= srv.RPCCall({masterAdd, 0}, "GetPrimaryAndSecondaries", handle)
				.get().as<std::tuple<GFSError,
						std::string /*Primary Address*/,
						std::vector<std::string> /*Secondary Addresses*/,
						std::uint64_t /*Expire Timestamp*/>>();
		if (gErr.errCode == GFSErrorCode::NoSuchChunk) {
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK)
			throw;
		cur = PushData;
	};
	auto pushData = [&]()->void {
		static int repeatTime = 0;
		id = rand();
		gErr = srv.RPCCall({primary, 0}, "PushData", id, data)
				.get().as<GFSError>();
		if (gErr.errCode != GFSErrorCode::OK) {
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				cur = PushData;
			} else {
				cur = EndFlow;
			}
		}
		const auto & addrs = secondaries;
		auto iter = addrs.begin();
		for (; iter != addrs.end(); ++iter) {
			gErr = srv.RPCCall({*iter, 0}, "PushData", id, data).get().as<GFSError>();
			if (gErr.errCode != GFSErrorCode::OK)
				break;
		}
		if (iter != addrs.end()) {
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				cur = PushData;
			} else {
				cur = EndFlow;
			}
		}
		repeatTime = 0;
		cur = AppendChunk;
	};
	auto appendChunk = [&]()->void {
		std::tie(gErr, offset)
				= srv.RPCCall({primary, 0}, "AppendChunk", handle, id, secondaries).get()
				.as<std::tuple<GFSError, std::uint64_t>>();
		static int repeatTime = 0;
		if (gErr.errCode == GFSErrorCode::PermissionDenied) {
			if (repeatTime < 3) {
				++repeatTime;
				cur = GetAddresses;
			} else {
				err = ClientErrCode::Unknown;
				cur = EndFlow;
			}
			return;
		}
		if (gErr.errCode == GFSErrorCode::ChunkFull) {
			++chunkIdx;
			cur = GetHandle;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK) {
			if (repeatTime < 3) {
				++repeatTime;
				cur = GetFileInfo;
			} else {
				err = ClientErrCode::Unknown;
				cur = EndFlow;
			}
			return ;
		}
		err = ClientErrCode::OK;
		cur = EndFlow;
	};
	auto endFlow = [&]()->void {};

	std::vector<std::function<void()>> states
			= {getFileInfo, getHandle, getAddresses, pushData, appendChunk, endFlow};


	while (cur != EndFlow) {
		states[cur]();
	}
	return err;
}

}
