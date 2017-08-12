//
// Created by Aaron Ren on 28/07/2017.
//

#include "address_serverdata.h"
#include <fstream>

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
	std::ofstream fout("./data/log.txt", std::ios::app);
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
		fout << time(nullptr) << ", " << eiter->first << " died, last beat time = "
		     << eiter->second.lastBeatTime << std::endl;

		std::cerr << eiter->first << ", last beat time = " << eiter->second.lastBeatTime << std::endl;
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
	//std::cerr << data.lastBeatTime << std::endl;
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

AFS::Address AFS::AddressServerData::chooseServer(const std::vector<std::string> &already) const {
	readLock lk(m);
	for (auto && item : mp) {
		if (std::find(already.begin(), already.end(), item.first) != already.end())
			continue;
		return item.first;
	}
	return "";

//		staic auto lastChoice = mp.cbegin();
//		auto start = lastChoice;
//		while (1) {
//			++lastChoice;
//			if (lastChoice == start) // 找了一圈还找不到就算失败
//				return "";
//			if (lastChoice == mp.cend())
//				lastChoice = mp.cbegin();
//			if (lastChoice->second.handles.size() < ChunkNumPerServer) {
//				return lastChoice->first;
//			}
//		}
}

void AFS::AddressServerData::addChunk(const AFS::Address &addr, AFS::ChunkHandle handle) {
	writeLock lk(m);
	auto & data = mp[addr];
	for (auto &&item : data.handles) {
		if (item.getHandle() == handle)
			return;
	}
	data.handles.emplace_back(MemoryPool::instance().getServerPtr(handle, addr));
}

void AFS::AddressServerData::write(std::ofstream &out) const {
	readLock lk(m);
	IOTool<Address> tool1;
	IOTool<ServerData> tool2;
	auto sz = (int)mp.size();
	out.write((char*)&sz, sizeof(sz));
	for (auto &&item : mp) {
		tool1.write(out, item.first);
		tool2.write(out, item.second);
	}
}

void AFS::AddressServerData::read(std::ifstream &in) {
	writeLock lk(m);
	IOTool<Address> tool1;
	IOTool<ServerData> tool2;
	int sz = -1;
	in.read((char*)&sz, sizeof(int));
	for (int i = 0; i < sz; ++i) {
		std::pair<Address, ServerData> tmp;
		tool1.read(in, tmp.first);
		tool2.read(in, tmp.second);
		mp.insert(std::move(tmp));
	}
}

std::unique_ptr<std::priority_queue<std::pair<size_t, AFS::Address>>> AFS::AddressServerData::getPQ() const {
	readLock lk(m);
	auto result = std::make_unique<std::priority_queue<std::pair<size_t, Address>>>();
	for (auto &&item : mp) {
		result->push(std::make_pair(item.second.handles.size(), item.first));
	}
	if (result->size() == 2) {
		//std::cerr << "!@!" << std::endl;
	}
	return result;
}

void AFS::AddressServerData::clear() {
	writeLock lk(m);
	mp.clear();
}

std::unique_ptr<std::vector<AFS::Address>> AFS::AddressServerData::listServers() const {
	readLock lk(m);
	auto result = std::make_unique<std::vector<Address>>();
	for (auto &&item : mp) {
		result->push_back(item.first);
	}
	return result;
}
