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

bool AFS::AddressServerdata::isDead(const AFS::Serverdata &data) const {
	return data.lastBeatTime + ExpiredTime < time(nullptr);
}

void AFS::AddressServerdata::checkDeadChunkServer(std::function<void(const Address &, ChunkHandle)> f) {
	readLock lk(m);
	std::vector<std::map<Address, Serverdata>::iterator> iterErased;
	for (auto iter = mp.begin(); iter != mp.end(); ++iter) {
		Serverdata & data = iter->second;
		if (isDead(data)) {
			iterErased.push_back(iter);
			for (auto &&handleVersion : data.handles)
				f(iter->first, std::get<0>(handleVersion));
		}
	}

	lk.unlock();
	writeLock wlk(m);
	for (auto &&eiter : iterErased) {
		mp.erase(eiter);
	}
}
