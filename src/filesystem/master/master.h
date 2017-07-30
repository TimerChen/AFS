//
// Created by Aaron Ren on 21/07/2017.
//

#ifndef AFS_MASTER_H
#define AFS_MASTER_H

#include "parser.hpp"
#include "common.h"
#include "server.hpp"
#include "path_filedata.h"
#include "address_serverdata.h"
#include "handle_chunkdata.h"
#include <memory>
#include <string>
#include <service.hpp>
#include <map>
#include <set>

/// ACM File System!
namespace AFS {

class Master : public Server {
private:
	PathFiledata      pfdm;
	AddressServerData asdm;

private:
	// return the number of the chunks whose lease has been extended successfully
	size_t leaseExtend(const Address & addr, const std::vector<ChunkHandle> & handles);

	// return the garbage chunks' handles
	std::unique_ptr<std::vector<ChunkHandle>>
	checkGarbage(const std::vector<std::tuple<ChunkHandle, ChunkVersion>> & chunks);

	// delete the failed chunks
	void deleteFailedChunks(const Address & addr, const std::vector<ChunkHandle> & chunks);

	MasterError updateChunkServer(const Address & addr,
	                       const std::vector<std::tuple<ChunkHandle, ChunkVersion>> & chunks);

	void checkDeadChunkServer();

	void collectGarbage();

	void reReplicate() {
		throw ;
	}
protected:
	// BackgroundActivity does all the background activities:
	// dead chunkserver handling, garbage collection, stale replica detection, etc
	void BackgroundActivity() {
		checkDeadChunkServer();
		collectGarbage();
		// todo re-replicate
		throw;
	}

	// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
	std::tuple<GFSError, std::vector<ChunkHandle> /*Garbage Chunks*/>
	RPCHeartbeat(std::vector<ChunkHandle> leaseExtensions,
	             std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks,
	             std::vector<ChunkHandle> failedChunks);

	// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
	// If no one holds the lease currently, grant one.
	std::tuple<GFSError,
			std::string /*Primary Address*/,
			std::vector<std::string> /*Secondary Addresses*/,
			std::uint64_t /*Expire Timestamp*/>
	RPCGetPrimaryAndSecondaries(ChunkHandle handle) {throw;}

	// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
	std::tuple<GFSError, std::vector<std::string> /*Locations*/>
	RPCGetReplicas(ChunkHandle handle) {throw; };

	// RPCGetFileInfo is called by client to get file information
	std::tuple<GFSError,
			bool /*IsDir*/,
			std::uint64_t /*Length*/,
			std::uint64_t /*Chunks*/>
	RPCGetFileInfo(std::string path_str) {throw;};

	// RPCCreateFile is called by client to create a new file
	GFSError
	RPCCreateFile(std::string path_str) {throw;}

	// RPCCreateFile is called by client to delete a file
	GFSError
	RPCDeleteFile(std::string path_str) {throw;}

	// RPCMkdir is called by client to make a new directory
	GFSError
	RPCMkdir(std::string path_str) {throw;}

	// RPCListFile is called by client to get the file list
	std::tuple<GFSError, std::vector<std::string> /*FileNames*/>
	RPCListFile(std::string path_str) {throw;};

	// RPCGetChunkHandle returns the chunk handle of (path, index).
	// If the requested index is larger than the number of chunks of this path by exactly one, create one.
	std::tuple<GFSError, ChunkHandle>
	RPCGetChunkHandle(std::string path_str, std::uint64_t chunkIndex) {
		throw;
	}

public:
	Master(LightDS::Service &srv, const std::string &rootDir);

	void Start() {throw;}
	void Shutdown() {throw;}

};

}


#endif //AFS_MASTER_H
