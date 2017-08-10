#ifndef GFS_TEST_GRAYBOX_BASIC_H
#define GFS_TEST_GRAYBOX_BASIC_H

#include "common_headers.h"
#include "common.h"
#include "graybox_env.h"

using namespace AFS;

class BasicTest
{
public:
	BasicTest(Environment &env);
	BasicTest(const BasicTest &) = delete;
	~BasicTest();

	bool Run();

public:
	TestResult TestCreateFile();
	TestResult TestMkdirList();
	TestResult TestGetChunkHandle();
	TestResult TestWriteChunk();
	TestResult TestReadChunk();
	TestResult TestReplicaConsistency();
	TestResult TestAppendChunk();

	TestResult TestBigData();

	TestResult TestShutdown();
	TestResult TestReReplication();
	TestResult TestPersistentChunkServer();
	TestResult TestPersistentMaster();
	TestResult TestDiskError();

protected:
	// return the number of replicas
	size_t checkConsistency(ChunkHandle chunk, std::uint64_t offset, std::uint64_t length);

	void checkData(const std::string &path, std::uint64_t offset, const std::vector<char> &expectedData);

protected:
	bool RunTestcase(const std::string &name, TestResult(BasicTest::*func)());

	template<typename Ret, typename ...Args>
	std::function<Ret(const Args &...)> RPC(LightDS::User::RPCAddress addr, const std::string &rpcName)
	{
		return [this, addr, rpcName](const Args &...args) -> Ret
		{
			return user.RPCCall(addr, rpcName, args...).get().template as<Ret>();
		};
	}

	template<typename Ret, typename ...Args, typename T>
	std::function<Ret(const Args &...)> RPC(LightDS::User::RPCAddress addr, const std::string &rpcName, Ret(T::*)(Args...))
	{
		return RPC<Ret, Args...>(addr, rpcName);
	}

	void generateRandomData(std::vector<char> &data, unsigned int seed);

protected:
	Environment &env;
	LightDS::User user;
	Client client;

	static const size_t pressure;
	static const size_t nThread;
	static const size_t chunkSize;
	static const size_t serverTimeout;	//in second
};

#endif
