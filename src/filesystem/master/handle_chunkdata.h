//
// Created by Aaron Ren on 28/07/2017.
//

#ifndef AFS_HANDLE_CHUNKDATA_H
#define AFS_HANDLE_CHUNKDATA_H

#include "setting.hpp"
#include "common.h"
#include "sugar.hpp"
#include <vector>
#include <map>
#include <ctime>
#include <gtest/gtest.h>

namespace AFS {
struct ChunkData {
	std::vector<Address> location; // the locations of the replicas
	Address              primary;
	time_t               leaseGrantTime{0};   // the time the primary chunk granted the lease
	ChunkVersion         version{0};  // the version of this chunk
};

class MemoryPool {
// DEBUG
	friend class hcdmTest;
    FRIEND_TEST(hcdmTest, ChunkPtr);
	FRIEND_TEST(hcdmTest, ServerPtr);

	friend class Ptr;

	struct ExtendedData {
		ChunkData data;
		int fileCnt{0};
		int serverCnt{0};
		std::uint64_t recycleCnt{0};
	};

	mutable readWriteMutex m;
	std::vector<ExtendedData>   container;
	std::vector<size_t>         recycler;

public:
	static MemoryPool & instance();

	class Ptr {
		friend class MemoryPool;
		static constexpr std::uint64_t InitVal = UINT64_MAX;

	protected:
	public: // debug
		ChunkHandle handle{InitVal};

		bool empty() const {
			return handle == InitVal;
		}
		size_t getPos() const {
			return handle & 0xffffffff;
		}
		explicit Ptr(ChunkHandle _handle)
				: handle(_handle) {}
	public:
		Ptr() = default;
		Ptr(const Ptr &) = delete;
		Ptr(Ptr && p) noexcept {
			handle = p.handle;
			p.handle = InitVal;
		}
		Ptr & operator=(const Ptr &) = delete;
		Ptr & operator=(Ptr && p) noexcept {
			if (this == &p)
				return *this;
			handle = p.handle;
			p.handle = InitVal;
			return *this;
		}
		virtual ~Ptr() = default;

		ChunkHandle getHandle() const {
			return handle;
		}
	};

	class ChunkPtr : public Ptr {
		friend class MemoryPool;
		explicit ChunkPtr(ChunkHandle handle) : Ptr(handle) {}

		void terminate() {
			if (empty())
				return;
			MemoryPool::instance().recycler.push_back(getPos());
			handle = InitVal;
		}
	public:
		ChunkPtr() = default;
		ChunkPtr(const ChunkPtr & p) = delete;
		ChunkPtr(ChunkPtr && ) = default;
		ChunkPtr & operator=(const ChunkPtr &) = delete;
		ChunkPtr & operator=(ChunkPtr &&) = default;
		~ChunkPtr() override {
			if (empty())
				return;
			writeLock lk(MemoryPool::instance().m);
			if (--MemoryPool::instance().container[getPos()].fileCnt == 0)
				terminate();
		}
	};

	class ServerPtr : public Ptr {
		friend class MemoryPool;

		Address addr;

		explicit ServerPtr(ChunkHandle h, Address _addr)
				: Ptr(h), addr(std::move(_addr)) {}
	public:
		ServerPtr() = default;
		ServerPtr(const ServerPtr &) = delete;
		ServerPtr& operator=(const ServerPtr &) = delete;
		ServerPtr(ServerPtr && o) noexcept
				: Ptr(std::move(o)), addr(std::move(o.addr)) {}
		ServerPtr & operator=(ServerPtr && o) noexcept {
			if (this == &o)
				return *this;
			handle = o.handle;
			o.handle = InitVal;
			addr = std::move(o.addr);
			return *this;
		}
		~ServerPtr() override {
			if (empty())
				return;
			writeLock lk(MemoryPool::instance().m);
			auto & exData = MemoryPool::instance().container[getPos()];
			--exData.serverCnt;
			remove(exData.data.location, addr);
		}
	};

	ChunkPtr   newChunk();
	ServerPtr getServerPtr(ChunkHandle handle, const Address & addr);

	ChunkData getData(ChunkHandle h) const;

	bool updateData_if(ChunkHandle handle,
	                   const std::function<bool(const ChunkData & data)> & condition,
	                   const std::function<void(ChunkData & data)> & f);
	void updateData(ChunkHandle handle,
	                const std::function<void(ChunkData & data)> & f);

	bool checkData(ChunkHandle handle,
	               const std::function<bool(const ChunkData & data)> & check) const {
		readLock lk(m);
		size_t pos = handle & 0xffffffff;
		return check(container[pos].data);
	}

	bool exists(ChunkHandle handle) {
		readLock lk(m);
		size_t pos = handle & 0xffffffff;
		if (pos > container.size())
			return false;
		return container[pos].fileCnt != 0;
	}
};


}


#endif //AFS_HANDLE_CHUNKDATA_H
