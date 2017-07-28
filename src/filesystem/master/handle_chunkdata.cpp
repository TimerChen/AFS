//
// Created by Aaron Ren on 28/07/2017.
//

#include "handle_chunkdata.h"

std::pair<AFS::MasterError, AFS::Chunkdata>
AFS::HandleChunkdata::getData(const AFS::ChunkHandle &handle) const {
	auto iter = mp.find(handle);
	if (iter == mp.cend())
		return std::make_pair(MasterError::NotExists, Chunkdata());
	return std::make_pair(MasterError::OK, iter->second);
}

AFS::MasterError
AFS::HandleChunkdata::extendLease(const AFS::ChunkHandle &handle, const AFS::Address &addr) {
	writeLock lk(m);
	auto iter = mp.find(handle);
	if (iter == mp.end())
		return MasterError::NotExists;
	auto & data = iter->second;
	if (data.location[data.primary] != addr)
		return MasterError::PermissonDenied;
	data.leaseGrantTime = time(nullptr);
	return MasterError::OK;
}

bool AFS::HandleChunkdata::checkChunkVersion(AFS::ChunkHandle handle, AFS::ChunkVersion version) {
	readLock rlk(m);
	auto & curVersion = mp[handle].version;
	if (version < curVersion)
		return false;
	if (version == curVersion)
		return true;
	rlk.unlock();
	writeLock wlk(m);
	curVersion = version;
	return true;
}

void AFS::HandleChunkdata::deleteFailedChunks(const AFS::Address &addr,
                                              const std::vector<AFS::ChunkHandle> &chunks) {
	writeLock lk(m);
	for (auto &&chunk : chunks) {
		auto iter = mp.find(chunk);
		if (iter == mp.end())
			continue;
		auto & location = iter->second.location;
		location.erase(std::find(location.begin(), location.end(), addr));
		if (location.empty())
			mp.erase(iter);
	}
}

void AFS::HandleChunkdata::updateChunkServer(const AFS::Address &addr,
                                             const std::vector<std::tuple<AFS::ChunkHandle, AFS::ChunkVersion>> &chunks) {
	for (auto &&chunk : chunks) {
		auto iter = mp.find(std::get<0>(chunk));
		if (iter == mp.end() || iter->second.version < std::get<1>(chunk))
			continue;
		auto & locations = iter->second.location;
		if (std::find(locations.begin(), locations.end(), addr) == locations.end())
			locations.emplace_back(addr);
	}
}

void AFS::HandleChunkdata::eraseDeadServersChunk(const AFS::Address &addr, AFS::ChunkHandle handle) {
	writeLock lk(m);
	auto iter = mp.find(handle);
	if (iter == mp.end())
		return;
	Chunkdata & data = iter->second;
	auto eiter = std::find(data.location.begin(), data.location.end(), addr);
	if (eiter == data.location.end())
		return;
	if (eiter - data.location.begin() == data.primary)
		data.leaseGrantTime = 0;
	data.location.erase(eiter);
}
