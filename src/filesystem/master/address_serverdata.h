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

	virtual void write(std::ofstream & out) const {
		out.write((char*)&lastBeatTime, sizeof(lastBeatTime));
	}
	virtual void read(std::ifstream & in) {
		in.read((char*)&lastBeatTime, sizeof(lastBeatTime));
	}
};

struct ServerData : public ServerDataBase {
	std::vector<MemoryPool::ServerPtr> handles;

	void write(std::ofstream & out) const final {
		IOTool<MemoryPool::ServerPtr> tool;
		auto sz = (int)handles.size();
		out.write((char*)&sz, sizeof(sz));
		for (auto &&handle : handles) {
			tool.write(out, handle);
		}
	}

	void read(std::ifstream & in) final {
		int sz;
		in.read((char*)&sz, sizeof(sz));
		handles.resize(sz);
		IOTool<MemoryPool::ServerPtr> tool;
		for (int i = 0; i < sz; ++i)
			tool.read(in, handles[i]);
	}
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
	static constexpr time_t ExpiredTime = 60; // 若服务器...s不心跳，则认为死亡
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

	void write(std::ofstream & out) const {
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

	void read(std::ifstream & in) {
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

	// 第一维为服务器上当前chunk数，第二维维地址
	// 这些数据是为了帮助负载平衡的，所以即使在实际使用时候，较真实情况相对落后也没关系
	std::unique_ptr<std::priority_queue<std::pair<size_t, Address>>>
	getPQ() const {
		readLock lk(m);
		auto result = std::make_unique<std::priority_queue<std::pair<size_t, Address>>>();
		for (auto &&item : mp) {
			result->push(std::make_pair(item.second.handles.size(), item.first));
		}
		return result;
	};

	void clear() {
		writeLock lk(m);
		mp.clear();
	}
};
}


#endif //AFS_ADDRESS_SERVERDATA_H
