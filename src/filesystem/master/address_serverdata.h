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
public: // debug
	std::map<Address, ServerData> mp;
	mutable readWriteMutex m;

private:
	bool isDead(const ServerData & data) const;

public:
	std::pair<MasterError, ServerDataCopy>
	getData(const Address &addr) const;

	std::unique_ptr<std::vector<Address>> checkDeadChunkServer();

	void
	updateChunkServer(const Address &addr,
	                       const std::vector<std::tuple<ChunkHandle, ChunkVersion>> & chunks);

	void deleteFailedChunks(const Address & addr, const std::vector<ChunkHandle> & handles);

	Address chooseServer(const std::vector<std::string> & already) const;

	void addChunk(const Address & addr, ChunkHandle handle);

	void write(std::ofstream & out) const;

	void read(std::ifstream & in);

	// 第一维为服务器上当前chunk数，第二维维地址
	// 这些数据是为了帮助负载平衡的，所以即使在实际使用时候，较真实情况相对落后也没关系
	std::unique_ptr<std::priority_queue<std::pair<size_t, Address>>>
	getPQ() const;;

	void clear();
};
}


#endif //AFS_ADDRESS_SERVERDATA_H
