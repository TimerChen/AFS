//
// Created by Aaron Ren on 28/07/2017.
//

#ifndef AFS_HANDLE_CHUNKDATA_H
#define AFS_HANDLE_CHUNKDATA_H

#include "setting.hpp"
#include "common.h"
#include <vector>
#include <map>
#include <ctime>

namespace AFS {
struct Chunkdata {
	static constexpr time_t ExpiredTime = 60;
	std::vector<Address> location; // the locations of the replicas
	size_t               primary{0};  // the index of the primary chunk
	time_t               leaseGrantTime{0};   // the time the primary chunk granted the lease
	ChunkVersion         version{0};  // the version of this chunk
};

class HandleChunkdata {
private:
	std::map<ChunkHandle, Chunkdata> mp;
	mutable readWriteMutex m;

public:
	std::pair<MasterError, Chunkdata>
	getData(const ChunkHandle &handle) const;

	MasterError extendLease(const ChunkHandle & handle, const Address & addr);

	// return true if the chunk is up-to-date
	bool checkChunkVersion(ChunkHandle handle, ChunkVersion version);

	void deleteFailedChunks(const Address & addr, const std::vector<ChunkHandle> & chunks);

	void updateChunkServer(const Address & addr,
	                       const std::vector<std::tuple<ChunkHandle, ChunkVersion>> & chunks);

	void eraseDeadServersChunk(const Address & addr, ChunkHandle handle);

	void eraseDeletedFileCHunk(ChunkHandle handle) {
		writeLock lk(m);
		auto iter = mp.find(handle);
		if (iter != mp.end())
			mp.erase(iter);
	}
};
}


#endif //AFS_HANDLE_CHUNKDATA_H
