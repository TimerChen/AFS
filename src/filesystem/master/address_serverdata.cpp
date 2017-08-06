//
// Created by Aaron Ren on 28/07/2017.
//

#include "address_serverdata.h"

std::pair<AFS::MasterError, AFS::ServerDataCopy>
AFS::AddressServerData::getData(const AFS::Address &addr) const {
	auto iter = mp.find(addr);
	if (iter == mp.cend())
		return std::make_pair(MasterError::NotExists, ServerDataCopy());
	return std::make_pair(MasterError::OK, ServerDataCopy(iter->second));
}

bool AFS::AddressServerData::isDead(const AFS::ServerData &data) const {
	return data.lastBeatTime + ExpiredTime < time(nullptr);
}

std::unique_ptr<std::vector<AFS::Address>>
AFS::AddressServerData::checkDeadChunkServer() {
	readLock lk(m);
	std::vector<std::map<Address, ServerData>::iterator> iterErased;
	for (auto iter = mp.begin(); iter != mp.end(); ++iter) {
		const ServerData & data = iter->second;
		if (isDead(data))
			iterErased.push_back(iter);
	}

	lk.unlock();
	writeLock wlk(m);
	auto result = std::make_unique<std::vector<AFS::Address>>();
	for (auto &&eiter : iterErased) {
		result->emplace_back(eiter->first);
		mp.erase(eiter);
	}
	return result;
}

void
AFS::AddressServerData::updateChunkServer(const AFS::Address &addr,
                                               const std::vector<std::tuple<AFS::ChunkHandle, AFS::ChunkVersion>> &chunks) {
	writeLock lk(m);
	auto & data = mp[addr];
	data.lastBeatTime = time(nullptr);
	data.handles.clear();
	data.handles.reserve(chunks.size());
	for (auto &&chunk : chunks) {
		if (MemoryPool::instance().exists(std::get<0>(chunk))) {
			data.handles.emplace_back(MemoryPool::instance().getServerPtr(std::get<0>(chunk), addr));
			continue;
		}
	}
}

void AFS::AddressServerData::deleteFailedChunks(const AFS::Address &addr,
                                                const std::vector<AFS::ChunkHandle> &handles) {
	writeLock lk(m);
	auto iter = mp.find(addr);
	if (iter == mp.end())
		return;
	auto & data = iter->second;
	for (auto &&handle : handles) {
		auto eiter = std::find_if(data.handles.begin(), data.handles.end(),
		                          [=](const MemoryPool::ServerPtr &d) {
			                          return d.getHandle() == handle;
		                          });
		if (eiter != data.handles.end())
			data.handles.erase(eiter);
	}
}
