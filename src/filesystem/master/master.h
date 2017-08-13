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
#include "log_contaner.h"
#include <memory>
#include <string>
#include <service.hpp>
#include <map>
#include <set>

/// ACM File System!
namespace AFS {

class Master : public Server {
public:
	~Master() final {
		std::unique_lock<std::mutex>(backgroundM);
		writeLock lk(globalMutex);
//		std::cerr << "master destroying\n";
	}

protected:
	void save() final;
	void load() final;
private:
	PathFileData      pfdm;
	AddressServerData asdm;
	LogContainer      logc;

	// 用于在开关机和存读档时候，除了上述情况以外的函数，都只应尝试获得读锁
	readWriteMutex    globalMutex;
	std::mutex        backgroundM;
	std::mutex        shutdownM;

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

	void reReplicate();

	void bindFunctions();
protected:
public: // debug
	// BackgroundActivity does all the background activities:
	// dead chunkserver handling, garbage collection, stale replica detection, etc
	void BackgroundActivity();

	// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
	std::tuple<GFSError, std::vector<ChunkHandle> /*Garbage Chunks*/>
	RPCHeartbeat(std::vector<ChunkHandle> leaseExtensions,
	             std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks,
	             std::vector<ChunkHandle> failedChunks);

	// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
	// If no one holds the lease currently, grant one.
	/* 返回GFSErrorCode类型：
	 * OK: 成功
	 * NoSuchChunk: 找不到handle对于的chunk
	 * Unknown: 其他
	 */
	std::tuple<GFSError,
			std::string /*Primary Address*/,
			std::vector<std::string> /*Secondary Addresses*/,
			std::uint64_t /*Expire Timestamp*/>
	RPCGetPrimaryAndSecondaries(ChunkHandle handle);

	// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
	std::tuple<GFSError, std::vector<std::string> /*Locations*/>
	RPCGetReplicas(ChunkHandle handle);

	// RPCGetFileInfo is called by client to get file information
	/* 返回GFSErrorCode类型：
	 * OK: 成功
	 * NoSuchFileDir: 路径中有部分不存在
	 * WrongOperation: 路径对应为文件夹
	 * Unknown：其他
	 */
	std::tuple<GFSError,
			bool /*IsDir*/,
			std::uint64_t /*Length*/,
			std::uint64_t /*Chunks*/>
	RPCGetFileInfo(std::string path_str);

	// RPCCreateFile is called by client to create a new file
	GFSError
	RPCCreateFile(std::string path_str);

	// RPCCreateFile is called by client to delete a file
	GFSError
	RPCDeleteFile(std::string path_str);

	// RPCMkdir is called by client to make a new directory
	GFSError
	RPCMkdir(std::string path_str);

	// RPCListFile is called by client to get the file list
	std::tuple<GFSError, std::vector<std::string> /*FileNames*/>
	RPCListFile(std::string path_str);

	// RPCGetChunkHandle returns the chunk handle of (path, index).
	// If the requested index is larger than the number of chunks of this path by exactly one, create one.
	/* 返回GFSErrorCode类型
	 * OK: 成功
	 * ServerNotFound: 当前所有副本所在服务器都不在线
	 * Unknown: 其他
	 */
	std::tuple<GFSError, ChunkHandle>
	RPCGetChunkHandle(std::string path_str, std::uint64_t chunkIndex);

public:
	Master(LightDS::Service &srv, const std::string &rootDir);

	void Start() final;
	void Shutdown() final;

private:
	void write(std::ofstream & out) const;

	void read(std::ifstream & in);

protected:
	std::uint16_t chunkPort;
};

}


#endif //AFS_MASTER_H
