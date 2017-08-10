#ifndef AFS_CHUNKSERVER_H
#define AFS_CHUNKSERVER_H

#include <string>
#include <service.hpp>
#include <sstream>
#include <queue>
#include <mutex>

#include "common.h"
#include "server.hpp"
#include "chunk.h"
#include <chunkpool.h>

namespace AFS {

class ChunkServer : public Server
{

	//friend class BasicTest;

public:
	enum MutationType : std::uint32_t
	{
		MutationWrite,
		MutationAppend,
		MutationPad
	};
public:


public:
	ChunkServer(LightDS::Service &Srv, const std::string &RootDir);

	~ChunkServer();

	void Start();

	void Shutdown();


public:
	// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
	GFSError
		RPCCreateChunk(ChunkHandle handle);

	// RPCPushDataAndForward is called by client.
	// It saves client pushed data to memory buffer.
	// This should be replaced by a chain forwarding.
	GFSError
		RPCPushData(std::uint64_t dataID, std::string data);

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
		RPCApplyMutation(ChunkHandle handle, ChunkVersion version, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset);

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


protected:


	void load();

	void loadFiles( const boost::filesystem::path &Path );

	void loadSettings();

	Chunk loadChunkInfo_noLock
		( const ChunkHandle &handle );

	std::string loadChunkData_noLock
		( const ChunkHandle &handle, const std::uint64_t &offset=0, const std::uint64_t &length=CHUNK_SIZE );

	Chunk loadChunkInfo
		( const ChunkHandle &handle );

	std::string loadChunkData
		( const ChunkHandle &handle, const std::uint64_t &offset=0, const std::uint64_t &length=CHUNK_SIZE );

	void save();

	void saveChunkInfo_noLock
		( const ChunkHandle &handle, const Chunk &c );

	void saveChunkData_noLock
		( const ChunkHandle &handle, const char *data, const std::uint64_t &offset=0, const std::uint64_t &length=CHUNK_SIZE );

	void saveChunkInfo
		( const ChunkHandle &handle, const Chunk &c );

	void saveChunkData
		( const ChunkHandle &handle, const char *data, const std::uint64_t &offset=0, const std::uint64_t &length=CHUNK_SIZE );

	void bindFunctions();

	void deleteData( const std::uint64_t &dataID );
	void deleteChunks( const ChunkHandle &handle );

	bool checkChunk( const std::int64_t &handle );
	void garbageCollect( const std::vector<ChunkHandle> &gbg );
	GFSError heartBeat();
	void Heartbeat();
	void clearData();
protected:
	std::uint32_t MaxCacheSize;
	std::map< ChunkHandle, Chunk > chunks;
	std::map< ChunkHandle, ReadWriteMutex > chunkMutex;
	std::map< std::uint64_t, std::tuple<char*,std::uint64_t/*length*/> > dataCache;
	std::deque<std::uint64_t> cacheQueue;
	ChunkPool chunkPool;
	ReadWriteMutex lock_chunks, lock_dataCache, lock_cacheQueue, lock_chunkMutex;

protected:
	std::string masterIP;
	std::uint16_t masterPort;
	boost::filesystem::path chunkFolder;
};



}

MSGPACK_ADD_ENUM(AFS::ChunkServer::MutationType);

#endif
