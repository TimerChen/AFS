//
// Created by Aaron Ren on 28/07/2017.
//

#include "handle_chunkdata.h"

namespace AFS {

MemoryPool &MemoryPool::instance() {
	static MemoryPool in;
	return in;
}

MemoryPool::ChunkPtr MemoryPool::newChunk() {
	writeLock lk(m);
	if (recycler.empty()) {
		container.emplace_back(ExtendedData());
		container.back().fileCnt++;
		return ChunkPtr(container.size() - 1 /*| (0 << 32)*/);
	}
	size_t pos = recycler.back();
	auto tmp = container[pos].recycleCnt + 1;
	container[pos] = ExtendedData();
	container[pos].fileCnt = 1;
	container[pos].recycleCnt = tmp;
	recycler.pop_back();
	return ChunkPtr(ChunkHandle(pos) | (tmp << 32));
}

MemoryPool::ServerPtr MemoryPool::getServerPtr(ChunkHandle handle, const Address &addr) {
	writeLock lk(m);
	auto & exData = container[handle & 0xffffffff];
	exData.serverCnt++;
	exData.data.location.push_back(addr);
	return ServerPtr(handle, addr);
}

ChunkData MemoryPool::getData(ChunkHandle h) const {
	readLock lk(m);
	return container[h & 0xffffffff].data;
}

bool MemoryPool::updateData_if(ChunkHandle handle, const std::function<bool(const ChunkData &)> &condition,
                               const std::function<void(ChunkData &)> &f) {
	writeLock lk(m);
	auto & data = container[handle & 0xffffffff].data;
	if (condition(data)) {
		f(data);
		return true;
	}
	return false;
}

void MemoryPool::updateData(ChunkHandle handle, const std::function<void(ChunkData &)> &f) {
	updateData_if(handle, [](const ChunkData &) {return true;}, f);
}

}