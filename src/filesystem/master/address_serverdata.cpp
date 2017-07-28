//
// Created by Aaron Ren on 28/07/2017.
//

#include "address_serverdata.h"

std::pair<AFS::MasterError, AFS::Serverdata>
AFS::AddressServerdata::getData(const AFS::Address &addr) const {
	auto iter = mp.find(addr);
	if (iter == mp.cend())
		return std::make_pair(MasterError::NotExists, Serverdata());
	return std::make_pair(MasterError::OK, iter->second);
}

void AFS::AddressServerdata::deleteFailedChunks(const AFS::Address &addr, const std::vector<AFS::ChunkHandle> &handles) {
	writeLock lk(m);
	auto iter = mp.find(addr);
	if (iter == mp.end())
		return;
	auto & data = iter->second;

	for (auto &&handle : handles) {
		auto eiter = std::find_if(data.handles.begin(), data.handles.end(),
		                         [=](const std::tuple<ChunkHandle, ChunkVersion> &d) {
			                         return std::get<0>(d) == handle;
		                         });
		if (eiter != data.handles.end())
			data.handles.erase(eiter);
	}
}

void AFS::AddressServerdata::updateChunkServer(const AFS::Address &addr,
                                               std::vector<std::tuple<AFS::ChunkHandle, AFS::ChunkVersion>> chunks) {
	writeLock lk(m);
	auto & data = mp[addr];
	data.lastBeatTime = time(nullptr);
	data.handles = std::move(chunks);
}
