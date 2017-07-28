//
// Created by Aaron Ren on 28/07/2017.
//

#ifndef AFS_ADDRESS_SERVERDATA_H
#define AFS_ADDRESS_SERVERDATA_H

#include <map>
#include <vector>
#include "common.h"
#include "setting.hpp"

namespace AFS {
struct Serverdata {
	std::vector<std::tuple<ChunkHandle, ChunkVersion>> handles{ChunkHandle()};
	time_t lastBeatTime{0};
};

class AddressServerdata {
private:
	std::map<Address, Serverdata> mp;
	readWriteMutex m;

public:
	std::pair<MasterError, Serverdata>
	getData(const Address &addr) const;

	void deleteFailedChunks(const Address & addr, const std::vector<ChunkHandle> & handles);

	void updateChunkServer(const Address & addr,
	                  std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks);
};
}


#endif //AFS_ADDRESS_SERVERDATA_H
