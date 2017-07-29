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
#include "setting.hpp"

namespace AFS {
struct Serverdata {
	std::vector<std::tuple<ChunkHandle, ChunkVersion>> handles{ChunkHandle()};
	time_t lastBeatTime{0};
};

class AddressServerdata {
private:
	static constexpr time_t ExpiredTime = 60; // 若服务器...s不心跳，则认为思死亡
	std::map<Address, Serverdata> mp;
	mutable readWriteMutex m;

private:
	bool isDead(const Serverdata & data) const;

public:
	std::pair<MasterError, Serverdata>
	getData(const Address &addr) const;

	void deleteFailedChunks(const Address & addr, const std::vector<ChunkHandle> & handles);

	void updateChunkServer(const Address & addr,
	                  std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks);


	// f is used to delete the locations in the chunk datas
	void checkDeadChunkServer(std::function<void(const Address &, ChunkHandle)> f);

	// since the ChunkNumPerServer is not absolutely accurate, the left space may be negtive
	std::unique_ptr<std::priority_queue<std::pair<int /*left space*/, Address>>>
			get_pq() const {
		readLock lk(m);
		auto result = std::make_unique<std::priority_queue<std::pair<int, Address>>>();
		for (auto &&item : mp) {
			result->push(std::make_pair(ChunkNumPerServer - item.second.handles.size(), item.first));
		}
		return result;
	};
};
}


#endif //AFS_ADDRESS_SERVERDATA_H
