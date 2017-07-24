//
// Created by Aaron Ren on 21/07/2017.
//

#ifndef AFS_MASTER_H
#define AFS_MASTER_H

#include "setting.h"
#include "data.h"
#include "log.h"
#include "common.h"
#include <string>
#include <map>

/// ACM File System!
namespace AFS {

/** Master
 *  · 主体即为MetadataContainer mdc以及
 *    chunk server相关的信息
 *  · 保存的数据包括路径到元数据的映射（mdc）
 *  · 元数据中除了一般意义的元数据，还包括对应
 *    chunk的信息等。
 *  · Background Activities
 *  ·· 后台工作包括：
 *  ·· 实现上全都利用extrie的iterate操作，
 *     iterate接收一个索引（表示文件夹），以及一个
 *     const vector<function<void(Metadata &)>> & f
 */
class Master {
private:
	MetadataContainer        mdc;
	ChunkServerDataContainer csdc;
	LogContainer             lc;

private:
	// createFolder

	// fileRead & fileWrite & append
//    ? getFileChunk(const Path & path) const;

	void collectGarbage();

	void detectExpiredChunk();

	void loadBalance();

	void checkpoint();

	/** reReplication:
	 *  · 检查当前的副本情况，如果有文件的（某个chunk的）
	 *    副本数不满足复制因子，则复制该chunk。
	 *  · Master会选择某个含有该chunk的chunk服务器，以及
	 *    新的chunk将处于的chunk服务器。命令前者传输一份chunk
	 *    到后者上。
	 *  · 此操作作为后台活动的一部分，同样是利用extrie的iterate
	 *    来完成的。
	 */
	class reReplication {
	private:
		ChunkServerDataContainer & csdc;
		LogContainer             & lc;

	public:
		reReplication(ChunkServerDataContainer & c,
		              LogContainer & l) : csdc(c), lc(l) {}
		void operator()(Metadata & md) {
			if (md.type != Metadata::Type::file)
				return;

			// todo
		}
	};

protected:
	// BackgroundActivity does all the background activities:
	// dead chunkserver handling, garbage collection, stale replica detection, etc
	void BackgroundActivity();

	// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
	std::tuple<GFSError, std::vector<ChunkHandle> /*Garbage Chunks*/>
	RPCHeartbeat(std::vector<ChunkHandle> leaseExtensions, std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks, std::vector<ChunkHandle> failedChunks);

	// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
	// If no one holds the lease currently, grant one.
	std::tuple<GFSError, std::string /*Primary Address*/, std::vector<std::string> /*Secondary Addresses*/, std::uint64_t /*Expire Timestamp*/>
	RPCGetPrimaryAndSecondaries(ChunkHandle handle);

	// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
	std::tuple<GFSError, std::vector<std::string> /*Locations*/>
	RPCGetReplicas(ChunkHandle handle);

	// RPCGetFileInfo is called by client to get file information
	std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/>
	RPCGetFileInfo(std::string path);

	// RPCCreateFile is called by client to create a new file
	GFSError
	RPCCreateFile(std::string path);

	// RPCCreateFile is called by client to delete a file
	GFSError
	RPCDeleteFile(std::string path);

	// RPCMkdir is called by client to make a new directory
	GFSError
	RPCMkdir(std::string path);

	// RPCListFile is called by client to get the file list
	std::tuple<GFSError, std::vector<std::string> /*FileNames*/>
	RPCListFile(std::string path);

	// RPCGetChunkHandle returns the chunk handle of (path, index).
	// If the requested index is larger than the number of chunks of this path by exactly one, create one.
	std::tuple<GFSError, ChunkHandle>
	RPCGetChunkHandle(std::string path, std::uint64_t chunkIndex);

protected:
//	LightDS::Service &srv;
	std::string rootDir;

public:
//	Master(LightDS::Service &srv, const std::string &rootDir);
	void Start();
	void Shutdown();

};

}


#endif //AFS_MASTER_H
