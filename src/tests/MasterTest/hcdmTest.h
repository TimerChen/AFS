//
// Created by Aaron Ren on 31/07/2017.
//

#ifndef AFS_HCDMTEST_H
#define AFS_HCDMTEST_H

#include "handle_chunkdata.h"
#include "parser.hpp"
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <memory>
#include <algorithm>

using std::string;
using std::vector;
using std::cerr;
using std::endl;
using std::make_unique;
using std::unique_ptr;
using std::move;
using std::uint64_t;
using std::cout;

namespace AFS {

class hcdmTest : public ::testing::Test {
protected:
	static constexpr size_t ChunkNum = 10;
	static constexpr size_t ServerNum = 5;
	MemoryPool &in;
	vector<MemoryPool::ChunkPtr>  files;
	vector<MemoryPool::ServerPtr> servers;

	hcdmTest() : in(MemoryPool::instance()) {}

	unique_ptr<vector<MemoryPool::ChunkPtr>>
	getSomeChunks() {
		auto result = make_unique<vector<MemoryPool::ChunkPtr>>();
		for (int i = 0; i < ChunkNum; ++i)
			result->emplace_back(in.newChunk());

		auto reLaunch = [&]() {
			int num = rand() % (int)ChunkNum;
			for (int i = 0; i < num; ++i)
				result->pop_back();
			std::random_shuffle(result->begin(), result->end());
			for (int i = 0; i < num; ++i)
				result->push_back(in.newChunk());
		};

		for (int i = 0; i < 5; ++i) {
			reLaunch();
		}

		return result;
	}

	virtual void SetUp() {
		files = move(*(getSomeChunks().release()));
	}

	virtual void TearDown() {

	}
};

TEST_F(hcdmTest, DISABLED_ChunkPtr) {
	files.clear();
	constexpr size_t ChunkNum = 10;
	// construct
	for (int i = 0; i < ChunkNum; ++i) {
		files.emplace_back(in.newChunk());
	}
	ASSERT_EQ(ChunkNum, in.container.size());
	ASSERT_EQ(0, in.recycler.size());

	// destruct
	for (int i = 0; i < ChunkNum / 2; ++i)
		files.pop_back();
	ASSERT_EQ(in.container.size() - in.recycler.size(),
	          ChunkNum - ChunkNum / 2);

	// recycle
	// this part of test has already passed
	// and is incompatible with the current test.
	// So, the next few lines should be ignored.
//	for (int i = 0; i < ChunkNum / 2; ++i) {
//		auto nextUsed = in.recycler.back();
//		files.emplace_back(in.newChunk());
//		ASSERT_EQ(nextUsed | ((uint64_t) 1 << 32), files.back().getHandle());
//	}
//	ASSERT_EQ(0, in.recycler.size());
//	ASSERT_EQ(ChunkNum, in.container.size());
}

TEST_F(hcdmTest, DISABLED_ServerPtr) {
	struct Log {
		uint64_t serverId;
		uint64_t handle;

		Log(uint64_t id = 0, uint64_t h = 0) : serverId(id), handle(h) {}

		bool operator<(const Log & other) const {
			return serverId < other.serverId
			       || (serverId == other.serverId && handle < other.handle);
		}
		bool operator==(const Log & other) const {
			return serverId == other.serverId && handle == other.handle;
		}
	};
	vector<Log> stdlogs, logs;

	cout << "get servers" << endl;

	for (int i = 0; i < ServerNum; ++i) {
		auto handle = files[rand() % ChunkNum].getHandle();
		stdlogs.emplace_back(i, handle);
		servers.emplace_back(in.getServerPtr(handle,
		                                     Convertor::uintToString(i)));
	}

	for (int i = 0; i < in.container.size(); ++i) {
		const auto & data = in.container[i];
		if (data.fileCnt == 0)
			continue;
		uint64_t handle = i | (data.recycleCnt << 32);
		for (auto &&location : data.data.location) {
			logs.emplace_back(Convertor::stringToUint(location), handle);
		}
	}

	std::sort(stdlogs.begin(), stdlogs.end());
	std::sort(logs.begin(), logs.end());

	cout << "checking eq" << endl;

	EXPECT_EQ(stdlogs, logs);

	for (int i = 0; i < stdlogs.size(); ++i) {
		EXPECT_EQ(stdlogs[i], logs[i]);
	}
}

}

#endif //AFS_HCDMTEST_H
