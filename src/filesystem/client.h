#ifndef AFS_CLIENT_H
#define AFS_CLIENT_H

#include <vector>
#include <tuple>
#include <string>
#include <service.hpp>
#include "common.h"
#include "setting.hpp"

namespace AFS {

class Client {
public:
	explicit Client(LightDS::Service &_srv, const Address &MasterAdd, const uint16_t &MasterPort=13208);

	void setMaster(Address add, uint16_t port);

protected:

	bool checkMasterAddress() const;

public:
	//You can use this to interactive with you by console.
	void Run( std::istream &in=std::cin, std::ostream &out=std::cout );

	ClientErr fileCreate(const std::string &dir);
	ClientErr fileMkdir(const std::string &dir);


	ClientErr fileRead(const std::string &dir, const std::string &localDir, const std::uint64_t &offset, const std::uint64_t &fileLength);
	ClientErr fileRead_str(const std::string &dir, std::string &data, const std::uint64_t &offset, const std::uint64_t &length);

	std::tuple<ClientErr, std::int64_t /*actual size*/>
	fileRead(const ChunkHandle & handle, const std::uint64_t & offset, std::vector<char> & data);;

	ClientErr fileWrite(const std::string &dir, const std::string &localDir, const std::uint64_t &offset);
	ClientErr fileWrite_str(const std::string &dir, const std::string &data, const std::uint64_t &offset);

	ClientErr fileWrite(const ChunkHandle & handle, const std::uint64_t & offset, const std::vector<char> & data) {
		GFSError gErr;
		std::uint64_t id, expire;
		std::string primary;
		std::vector<std::string> secondaries;

		ClientErr err(ClientErrCode::Unknown);
		std::function<void()> state, getAddresses, pushData, applyChunk, endFlow;
		bool run = true;

		getAddresses = [&]()->void {
			std::tie(gErr, primary, secondaries, expire)
					= srv.RPCCall({masterAdd, masterPort}, "GetPrimaryAndSecondaries", handle)
					.get().as<std::tuple<GFSError,
							std::string /*Primary Address*/,
							std::vector<std::string> /*Secondary Addresses*/,
							std::uint64_t /*Expire Timestamp*/>>();
			if (gErr.errCode == GFSErrorCode::NoSuchChunk) {
				err = ClientErr(ClientErrCode::Unknown);
				state = endFlow;
				return;
			}
			if (gErr.errCode != GFSErrorCode::OK)
				throw;
			state = pushData;
		};
		pushData = [&]()->void {
			static int repeatTime = 0;
			id = rand();
			gErr = srv.RPCCall({primary, masterPort}, "PushData", id, data)
					.get().as<GFSError>();
			if (gErr.errCode != GFSErrorCode::OK) {
				err =  ClientErr(ClientErrCode::Unknown);
				if (repeatTime < 3) {
					++repeatTime;
					state = pushData;
				} else {
					state = endFlow;
				}
			}
			const auto & addrs = secondaries;
			auto iter = addrs.begin();
			for (; iter != addrs.end(); ++iter) {
				gErr = srv.RPCCall({*iter, masterPort}, "PushData", id, data).get().as<GFSError>();
				if (gErr.errCode != GFSErrorCode::OK)
					break;
			}
			if (iter != addrs.end()) {
				err =  ClientErr(ClientErrCode::Unknown);
				if (repeatTime < 3) {
					++repeatTime;
					state = pushData;
				} else {
					state = endFlow;
				}
			}
			repeatTime = 0;
			state = applyChunk;
		};
		applyChunk = [&]()->void {
			gErr = srv.RPCCall({primary, masterPort}, "WriteChunk", handle, id, offset, secondaries).get()
					.as<GFSError>();
			static int repeatTime = 0;
			if (gErr.errCode != GFSErrorCode::OK) {
				if (repeatTime < 3) {
					++repeatTime;
					state = getAddresses;
				} else {
					err = ClientErrCode::Unknown;
					state = endFlow;
				}
				return ;
			}
			err = ClientErrCode::OK;
			state = endFlow;
		};
		endFlow = [&]()->void {run = false;};

		state = getAddresses;

		while (run)
			state();
		return err;
	}

	ClientErr fileAppend(const std::string &dir, const std::string &localDir);
	ClientErr fileAppend_str(const std::string &dir, const std::string &data);

	std::tuple<ClientErr, std::uint64_t /*offset*/>
	fileAppend(const ChunkHandle & handle, const std::vector<char> & data);

	std::tuple<ClientErr, std::vector<std::string>> listFile(const std::string & dir);

	std::tuple<ClientErr, ChunkHandle> getChunkHandle(const std::string & dir, size_t idx);

protected:
	Address masterAdd;
	uint16_t masterPort;
	LightDS::Service &srv;
	char buffer[CHUNK_SIZE];

private:
	static GFSErrorCode toGFSErrorCode(ClientErrCode err);

	static GFSError toGFSError(ClientErr err);

public: // test
	// create file
	GFSError Create(const std::string & dir);

	GFSError Mkdir(const std::string & dir);

	std::tuple<GFSError, std::vector<std::string>> List(const std::string & dir);

	std::tuple<GFSError, ChunkHandle>
			GetChunkHandle(const std::string & dir, std::size_t idx);

	GFSError WriteChunk(const ChunkHandle & handle, const std::uint64_t & offset, const std::vector<char> & data);

	std::tuple<GFSError, std::uint64_t /*size*/>
	ReadChunk(const ChunkHandle & handle, const std::uint64_t & offset, std::vector<char> & data);

	std::tuple<GFSError, std::uint64_t /*offset*/>
	AppendChunk(const ChunkHandle & handle, const std::vector<char> & data);
};

}

#endif // CLIENT_H
