#ifndef AFS_CHUNKSERVER_H
#define AFS_CHUNKSERVER_H

#include <string>
#include <service.hpp>

#include "common.h"
#include "server.hpp"
#include "chunk.h"

namespace AFS {

/*
class ChunkServer : public Server
{

	class ChunkData
	{

	public:

		long long name, version;
		short end;
		char data[Chunk::CHUNKSIZE];

		ChunkData();

	};


public:
	ChunkServer();

};
*/





class ChunkServer : public Server
{
public:
	enum MutationType : std::uint32_t
	{
		MutationWrite,
		MutationAppend,
		MutationPad
	};
public:
	ChunkServer(LightDS::Service &Srv, const std::string &RootDir);
	~ChunkServer();
	virtual void Start();
	virtual void Shutdown();

protected:
	void Heartbeat();

	// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
	GFSError
		RPCCreateChunk(ChunkHandle handle);

	// RPCReadChunk is called by client, read chunk data and return
	std::tuple<GFSError, std::string /*Data*/>
		RPCReadChunk(ChunkHandle handle, std::uint64_t offset, std::uint64_t length);

	// RPCWriteChunk is called by client
	// applies chunk write to itself (primary) and asks secondaries to do the same.
	GFSError
		RPCWriteChunk(ChunkHandle handle, std::uint64_t dataID, std::uint64_t offset, std::vector<std::string> secondaries);

	// RPCAppendChunk is called by client to apply atomic record append.
	// The length of data should be within max append size.
	// If the chunk size after appending the data will excceed the limit,
	// pad current chunk and ask the client to retry on the next chunk.
	std::tuple<GFSError, std::uint64_t /*offset*/>
		RPCAppendChunk(ChunkHandle handle, std::uint64_t dataID, std::vector<std::string> secondaries);

	// RPCApplyMutation is called by primary to apply mutations
	GFSError
		RPCApplyMutation(ChunkHandle handle, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset, std::uint64_t length);

	// RPCSendCopy is called by master, send the whole copy to given address
	GFSError
		RPCSendCopy(ChunkHandle handle, std::string addr);

	// RPCApplyCopy is called by another replica
	// rewrite the local version to given copy data
	GFSError
		RPCApplyCopy(ChunkHandle handle, ChunkVersion version, std::string data, std::uint64_t serialNo);

	// RPCGrantLease is called by master
	// mark the chunkserver as primary
	GFSError
		RPCGrantLease(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>>);

	// RPCUpdateVersion is called by master
	// update the given chunks' version to 'newVersion'
	GFSError
		RPCUpdateVersion(ChunkHandle handle, ChunkVersion newVersion);

	// RPCPushDataAndForward is called by client.
	// It saves client pushed data to memory buffer.
	// This should be replaced by a chain forwarding.
	GFSError
		RPCPushData(std::uint64_t dataID, std::string data);

};



}

MSGPACK_ADD_ENUM(AFS::ChunkServer::MutationType);

#endif
