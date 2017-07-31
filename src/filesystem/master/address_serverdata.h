//
// Created by Aaron Ren on 28/07/2017.
//

#ifndef AFS_ADDRESS_SERVERDATA_H
#define AFS_ADDRESS_SERVERDATA_H

#include <map>
#include <vector>
#include <queue>
#include <functional>
#include "common.h"
#include "handle_chunkdata.h"
#include "setting.hpp"

namespace AFS {

struct ServerDataBase {
	time_t lastBeatTime{0};
};

struct ServerData : public ServerDataBase {
	std::vector<MemoryPool::ServerPtr> handles;
};

struct ServerDataCopy : public ServerDataBase {
	std::vector<ChunkHandle> handles;

	ServerDataCopy() = default;
	explicit ServerDataCopy(const ServerData & data) : ServerDataBase(data) {
		handles.resize(data.handles.size());
		for (int i = 0; i < (int)handles.size(); ++i)
			handles[i] = data.handles[i].getHandle();
	}
	ServerDataCopy & operator=(const ServerDataCopy &) = default;
	ServerDataCopy(const ServerDataCopy &) = default;
	ServerDataCopy(ServerDataCopy &&) = default;
	ServerDataCopy & operator=(ServerDataCopy &&) = default;
	~ServerDataCopy() = default;
};

class AddressServerData {
private:
	static constexpr time_t ExpiredTime = 60; // 若服务器...s不心跳，则认为思死亡
	std::map<Address, ServerData> mp;
	mutable readWriteMutex m;

private:
	bool isDead(const ServerData & data) const;

public:
	std::pair<MasterError, ServerDataCopy>
	getData(const Address &addr) const;

	void checkDeadChunkServer();

	void updateChunkServer(const Address &addr,
	                       const std::vector<std::tuple<ChunkHandle, ChunkVersion>> & chunks);

	void deleteFailedChunks(const Address & addr, const std::vector<ChunkHandle> & handles);

	Address chooseServer() const {
		readLock lk(m);
		static auto lastChoice = mp.cbegin();
		while (1) {
			++lastChoice;
			if (lastChoice == mp.cend()) {
				// todo sleep
				lastChoice = mp.cbegin();
			}
			if (lastChoice->second.handles.size() < ChunkNumPerServer) {
				return lastChoice->first;
			}
		}
	}

	void addChunk(const Address & addr, ChunkHandle handle) {
		writeLock lk(m);
		auto & data = mp[addr];
		data.handles.emplace_back(MemoryPool::instance().getServerPtr(handle, addr));
	}
};
}


#endif //AFS_ADDRESS_SERVERDATA_H