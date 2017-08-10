#include "common_headers.h"
#include "graybox_basic.h"

using boost::format;

const size_t BasicTest::pressure = 1024 * 1024;
const size_t BasicTest::nThread = 8;
const size_t BasicTest::chunkSize = 64 * 1024 * 1024;
const size_t BasicTest::serverTimeout = 30;

BasicTest::BasicTest(Environment &env) :
	env(env), user(env.GetNodeServerAddr()), client(user)
{
	env.ResetMaster(0);
	env.ResetChunkServer(1);
	env.ResetChunkServer(2);
	env.ResetChunkServer(3);
	env.ResetChunkServer(4);

	env.StartMaster(0);
	env.StartChunkServer(1);
	env.StartChunkServer(2);
	env.StartChunkServer(3);
	env.StartChunkServer(4);
}

BasicTest::~BasicTest()
{
	env.Stop(4);
	env.Stop(3);
	env.Stop(2);
	env.Stop(1);
	env.Stop(0);
}

bool BasicTest::Run()
{
	std::pair<std::string, TestResult(BasicTest::*)()> testcases[] = {
//		{"CreateFile", &BasicTest::TestCreateFile},
//		{"MkdirList", &BasicTest::TestMkdirList},
//		{"GetChunkHandle", &BasicTest::TestGetChunkHandle},
//		{"WriteChunk", &BasicTest::TestWriteChunk},
//		{"ReadChunk", &BasicTest::TestReadChunk},
//		{"ReplicaConsistency", &BasicTest::TestReplicaConsistency},
//		{"AppendChunk", &BasicTest::TestAppendChunk},
//		{"BigData", &BasicTest::TestBigData},

		{"ReReplication", &BasicTest::TestReReplication},
		{"PersistentChunkServer", &BasicTest::TestPersistentChunkServer},
		{"PersistentMaster", &BasicTest::TestPersistentMaster},
		{"DiskError", &BasicTest::TestDiskError},

		{"Shutdown", &BasicTest::TestShutdown},
	};

	for (auto &pair : testcases)
	{
		if (!RunTestcase(pair.first, pair.second))
			return false;
	}
	return true;
}

bool BasicTest::RunTestcase(const std::string &name, TestResult(BasicTest::*func)())
{
	std::cerr << "Testcase: " << name << std::endl;
	TestResult result;
	try
	{
		result = (this->*func)();
	}
	catch (TestException &e)
	{
		result = e.getTestResult();
	}
	if (result.passed)
	{
		std::cerr << "\tPASS" << std::endl;
		return true;
	}
	else
	{
		std::cerr << "\tFAIL: " << result.description << std::endl;
		return false;
	}
}

TestResult BasicTest::TestCreateFile()
{
	client.Create("/TestCreateFile.txt") | must_succ;

	GFSError err = client.Create("/TestCreateFile.txt");
	if (err.errCode == GFSErrorCode::OK)
		return TestResult::Fail("The same file should not be created twice");

	return TestResult::Pass();
}

TestResult BasicTest::TestMkdirList()
{
	client.Mkdir("/dir1") | must_succ;
	client.Mkdir("/dir2") | must_succ;

	static const std::vector<int> filesInDir1 = { 1, 2, 3, 4 };
	static const std::vector<int> filesInDir2 = { 4, 5 };

	for (int id : filesInDir1)
		client.Create("/dir1/file" + std::to_string(id) + ".txt") | must_succ;
	for (int id : filesInDir2)
		client.Create("/dir2/file" + std::to_string(id) + ".txt") | must_succ;

	GFSError err = client.Create("/dir2/file4.txt");
	if (err.errCode == GFSErrorCode::OK)
		return TestResult::Fail("The same file should not be created twice");

	std::vector<std::string> list1 = client.List("/dir1") | must_succ;
	std::vector<std::string> list2 = client.List("/dir2") | must_succ;

	if (list1.size() != filesInDir1.size())
		return TestResult::Fail("Number of files mismatch in dir1 (" + std::to_string(list1.size()) + "/" + std::to_string(list2.size()) + ")");
	if(list2.size() != filesInDir2.size())
		return TestResult::Fail("Number of files mismatch in dir2 (" + std::to_string(list1.size()) + "/" + std::to_string(list2.size()) + ")");
	
	for (int id : filesInDir1)
	{
		if (std::find(list1.begin(), list1.begin(), "file" + std::to_string(id) + ".txt") == list1.end())
			return TestResult::Fail("File 'file" + std::to_string(id) + ".txt' missed in dir1");
	}
	for (int id : filesInDir2)
	{
		if (std::find(list2.begin(), list2.begin(), "file" + std::to_string(id) + ".txt") == list2.end())
			return TestResult::Fail("File 'file" + std::to_string(id) + ".txt' missed in dir2");
	}

	return TestResult::Pass();
}

TestResult BasicTest::TestGetChunkHandle()
{
//	std::cerr << "Creating..." << std::endl;
	client.Create("/TestGetChunkHandle.txt") | must_succ;
//	std::cerr <<"OK\n"<< "Get...1...";
	ChunkHandle chunk1 = client.GetChunkHandle("/TestGetChunkHandle.txt", 0) | must_succ;
//	std::cerr << "2...";
	ChunkHandle chunk2 = client.GetChunkHandle("/TestGetChunkHandle.txt", 0) | must_succ;
//	std::cerr <<"OK\n";
	if (chunk1 != chunk2)
		return TestResult::Fail("Different Chunk Handle: " + std::to_string(chunk1) + " != " + std::to_string(chunk2));

	//std::cerr <<"Invalid Operation...";
	auto ret = client.GetChunkHandle("/TestGetChunkHandle.txt", 2);
	if (std::get<0>(ret).errCode == GFSErrorCode::OK)
		return TestResult::Fail("Discontinuous chunk should not be created");
	//std::cerr << "OK";
	return TestResult::Pass();
}

TestResult BasicTest::TestWriteChunk()
{
	client.Create("/TestWriteChunk.txt") | must_succ;
	ChunkHandle chunk = client.GetChunkHandle("/TestWriteChunk.txt", 0) | must_succ;
	//std::uint16_t nThread = 1;
	static const size_t sizePerThread = pressure;
	std::thread threads[nThread];

	bool succ = true;
	TestResult lastError;
	std::mutex mtxLastError;
	std::cerr << "\nMultithread testing...\n";
	for (size_t i = 0; i < nThread; i++)
	{
		threads[i] = std::thread([this, chunk, i, &succ, &lastError, &mtxLastError]()
		{
			//std::cerr << "thread " << i << std::endl;
			std::vector<char> data(sizePerThread);
			for (size_t j = 0; j < sizePerThread; j++)
				data[j] = char(j & 0xff);
			GFSError err = client.WriteChunk(chunk, i * sizePerThread, data);
			//std::cerr << "writeChunk completed, err.errcode = " << (int)err.errCode << std::endl;
			if (err.errCode != GFSErrorCode::OK)
			{
				std::lock_guard<std::mutex> lock(mtxLastError);
				succ = false;
				lastError = TestResult::Fail("An error occurred in writechunk: (code=" + std::to_string(int(err.errCode)) + "): " + lastError.description);
			}
		});
	}

	for (auto &thread : threads)
		thread.join();
	//std::cerr << "[OK]\n";
	if (!succ)
		return lastError;

	return TestResult::Pass();
}

TestResult BasicTest::TestReadChunk()
{
	ChunkHandle chunk = client.GetChunkHandle("/TestWriteChunk.txt", 0) | must_succ;

	static const size_t sizePerThread = pressure;
	//std::uint16_t nThread = 1;
	std::thread threads[nThread];

	bool succ = true;
	TestResult lastError;
	std::mutex mtxLastError;

	for (size_t i = 0; i < nThread; i++)
	{
		threads[i] = std::thread([this, chunk, i, &succ, &lastError, &mtxLastError]()
		{
			std::vector<char> data(sizePerThread);

			GFSError err;
			size_t n;

			std::tie(err, n) = client.ReadChunk(chunk, i * sizePerThread, data);

			if (err.errCode != GFSErrorCode::OK)
			{
				std::lock_guard<std::mutex> lock(mtxLastError);
				succ = false;
				lastError = TestResult::Fail("An error occurred in readchunk: (code=" + std::to_string(int(err.errCode)) + "): " + err.description);
				return;
			}
			if (n != sizePerThread)
			{
				std::lock_guard<std::mutex> lock(mtxLastError);
				succ = false;
				lastError = TestResult::Fail("Read size mismatch: " + std::to_string(n) + "/" + std::to_string(sizePerThread));
				return;
			}

			for (size_t j = 0; j < sizePerThread; j++)
			{
				if (data[j] != char(j & 0xff))
				{
					std::lock_guard<std::mutex> lock(mtxLastError);
					succ = false;
					lastError = TestResult::Fail("Read data mismatch at " + std::to_string(i * sizePerThread + j) + ": " + std::to_string(data[j]) + " != " + std::to_string(j & 0xff));
					return;
				}
			}
		});
	}

	for (auto &thread : threads)
		thread.join();

	if (!succ)
		return lastError;

	return TestResult::Pass();
}

size_t BasicTest::checkConsistency(ChunkHandle chunk, std::uint64_t offset, std::uint64_t length)
{
	std::cerr << "start\n";
	std::vector<LightDS::User::RPCAddress> addresses = user.ListService("master");
	assert(addresses.size() == 1);
	auto addrMaster = addresses[0];

	std::cerr << "getrep\n";
	std::vector<std::string> replicas = RPC(addrMaster, "GetReplicas", &Master::RPCGetReplicas)(chunk) | must_succ;
	std::vector<std::string> data;

	for (auto &replica : replicas)
	{
		std::cerr << "ReadChunk:" << replica << std::endl;
		auto tmp =
		user.RPCCall({replica,7778}, "ReadChunk", chunk, offset, length).get().as<
				std::tuple<GFSError, std::string /*Data*/> >();
		//auto tmp = std::move(RPC({replica,7778}, "ReadChunk", &ChunkServer::RPCReadChunk)(chunk, offset, length) | must_succ);
		std::cerr << "Return Over\n";
		if( std::get<0>(tmp) == AFS::GFSErrorCode::OK )
			data.push_back( std::get<1>(tmp) );
	}

	for (size_t i = 1; i < data.size(); i++)
	{
		if (data[i] != data[0])
			throw TestException(TestResult::Fail((format("Different replicas in chunk %d (between replica '%d' and replica '%d')") % chunk % replicas[0] % replicas[i]).str()));
	}

	return replicas.size();
}

TestResult BasicTest::TestReplicaConsistency()
{
	ChunkHandle chunk = client.GetChunkHandle("/TestWriteChunk.txt", 0) | must_succ;

	size_t n = checkConsistency(chunk, 0, nThread * pressure);

	if (n < 2)
		return TestResult::Fail((format("Too few chunks (n=%d)") % n).str());

	return TestResult::Pass();
}

TestResult BasicTest::TestAppendChunk()
{
	client.Create("/TestAppendChunk.txt") | must_succ;
	ChunkHandle chunk = client.GetChunkHandle("/TestAppendChunk.txt", 0) | must_succ;
	
	static const size_t sizePerThread = pressure;
	std::vector<char> data[nThread];

	for (size_t i = 0; i < nThread; i++)
	{
		data[i].resize(sizePerThread);
		for (size_t j = 0; j < sizePerThread; j++)
			data[i][j] = char((i*i) ^ j & 0xff);
	}

	std::future<std::uint64_t> fPositions[nThread];

	for (size_t i = 0; i < nThread; i++)
	{
		fPositions[i] = std::async(std::launch::async, [this, chunk, &data = data[i]]() -> std::uint64_t
		{
			return client.AppendChunk(chunk, data) | must_succ;
		});
	}

	std::uint64_t positions[nThread];

	for (size_t i = 0; i < nThread; i++)
		positions[i] = fPositions[i].get();

	for (size_t i = 0; i < nThread; i++)
		checkConsistency(chunk, positions[i], sizePerThread);

	for (size_t i = 0; i < nThread; i++)
	{
		std::vector<char> tmp(sizePerThread);
		std::uint64_t size = client.ReadChunk(chunk, positions[i], tmp) | must_succ;
		if (size != data[i].size())
			return TestResult::Fail((format("Size mismatch at %d") % positions[i]).str());
		for (size_t j = 0; j < tmp.size(); j++)
		{
			if (tmp[j] != data[i][j])
				return TestResult::Fail((format("Data mismatch at %d (%x != %x)") % (positions[i] + j) % tmp[j] % data[i][j]).str());
		}
	}

	return TestResult::Pass();
}

void BasicTest::generateRandomData(std::vector<char> &data, unsigned int seed)
{
	std::mt19937 gen(seed);
	std::uniform_int_distribution<int> dist(0, 0xff);
	for (char &c : data)
		c = char(dist(gen));
}

TestResult BasicTest::TestBigData()
{
	static const std::string path = "/BigData.txt";
	client.Create(path) | must_succ;

	static const size_t sizeData = 4 * chunkSize;

	std::vector<char> data(sizeData);
	generateRandomData(data, 0x20170807);

	client.Write(path, chunkSize / 2, data) | must_succ;

	std::vector<char> readData(sizeData);

	std::uint64_t length = client.Read(path, chunkSize / 2, readData) | must_succ;

	if (length != data.size())
		return TestResult::Fail((format("Data size mismatch %d != %d") % length % data.size()).str());

	for (size_t i = 0; i < sizeData; i++)
	{
		if (readData[i] != data[i])
			return TestResult::Fail((format("Data mismatch at %d (%x != %x)") % (chunkSize / 2 + i) % readData[i] % data[i]).str());
	}

	static const size_t appendCount = 5;
	std::vector<char> dataAppend;
	
	for (size_t i = 0; i < appendCount; i++)
	{
		std::cerr << "No." << i << std::endl;
		dataAppend.resize(chunkSize / 4 - 1);
		generateRandomData(dataAppend, 0x19260817 + i);

		std::uint64_t offset = client.Append(path, dataAppend) | must_succ;
		length = client.Read(path, offset, readData) | must_succ;
		if(length != dataAppend.size())
			return TestResult::Fail((format("Data size mismatch when testing append: %d != %d") % length % dataAppend.size()).str());
		for (size_t j = 0; j < dataAppend.size(); j++)
		{
			if (readData[j] != dataAppend[j])
				return TestResult::Fail((format("Data mismatch at %d (%x != %x)") % (offset + j) % readData[j] % data[j]).str());
		}
	}

	return TestResult::Pass();
}

void BasicTest::checkData(const std::string &path, std::uint64_t offset, const std::vector<char> &expectedData)
{
	std::vector<char> readData(expectedData.size());
	std::uint64_t length = client.Read(path, offset, readData) | must_succ;

	if (length != expectedData.size())
		throw TestException(TestResult::Fail((format("Data size mismatch: %d != %d") % length % expectedData.size()).str()));
	for (size_t i = 0; i < expectedData.size(); i++)
	{
		if (readData[i] != expectedData[i])
			throw TestException(TestResult::Fail((format("Data mismatch at %d (%x != %x)") % (offset + i) % readData[i] % expectedData[i]).str()));
	}
}

TestResult BasicTest::TestShutdown()
{
	using namespace std::chrono_literals;

	static const std::string path = "/Shutdown.txt";
	client.Create(path) | must_succ;

	static const size_t sizePerThread = chunkSize / 4 - 1;
	std::vector<char> data[nThread];

	for (size_t i = 0; i < nThread; i++)
	{
		data[i].resize(sizePerThread);
		generateRandomData(data[i], 0x23333333 + i);
	}

	std::future<std::uint64_t> fPositions[nThread];

	for (size_t i = 0; i < nThread; i++)
	{
		fPositions[i] = std::async(std::launch::async, [this, &data = data[i]]() -> std::uint64_t
		{
			return client.Append(path, data) | must_succ;
		});
	}

	std::this_thread::sleep_for(1ms);

	env.Stop(1);
	env.Stop(2);

	std::shared_ptr<void> defer(nullptr, [this](const void *)
	{
		env.StartChunkServer(1);
		env.StartChunkServer(2);
	});

	std::uint64_t positions[nThread];
	for (size_t i = 0; i < nThread; i++)
		positions[i] = fPositions[i].get();

	for (size_t i = 0; i < nThread; i++)
		checkData(path, positions[i], data[i]);

	return TestResult::Pass();
}

TestResult BasicTest::TestReReplication()
{
	const size_t serverTimeout = 5;
	static const std::string path = "/ReReplication.txt";
	client.Create(path) | must_succ;

	ChunkHandle chunk = client.GetChunkHandle(path, 0) | must_succ;

	std::vector<char> data(pressure);
	generateRandomData(data, 0x31415927);

	std::uint64_t offset = client.AppendChunk(chunk, data) | must_succ;

	env.Stop(1);
	env.Stop(2);
	std::cerr << "after stop " << time(nullptr) << std::endl;
	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout) * 2);
	std::cerr << "Sleep...END\n";
	size_t n = checkConsistency(chunk, offset, data.size());
	if (n != 2)
		return TestResult::Fail((format("Expect 2 replicas, got %d") % n).str());
	std::cerr << "pass 1\n";
	env.Stop(3);
	env.StartChunkServer(1);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout) * 2);

	n = checkConsistency(chunk, offset, data.size());
	if (n != 2)
		return TestResult::Fail((format("Expect 2 replicas, got %d") % n).str());

	env.Stop(4);
	env.StartChunkServer(2);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout) * 2);

	n = checkConsistency(chunk, offset, data.size());
	if (n != 2)
		return TestResult::Fail((format("Expect 2 replicas, got %d") % n).str());

	env.StartChunkServer(3);
	env.StartChunkServer(4);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	checkConsistency(chunk, offset, data.size());

	return TestResult::Pass();
}

TestResult BasicTest::TestPersistentChunkServer()
{
	static const std::string path = "/PersistentChunkServer.txt";
	client.Create(path) | must_succ;

	std::vector<char> data(pressure);
	generateRandomData(data, 0x27182818);

	std::uint64_t offset = client.Append(path, data) | must_succ;

	env.Stop(1);
	env.Stop(2);
	env.Stop(3);
	env.Stop(4);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout) * 2);

	env.StartChunkServer(1);
	env.StartChunkServer(2);
	env.StartChunkServer(3);
	env.StartChunkServer(4);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	checkData(path, offset, data);

	generateRandomData(data, 0x14142136);
	offset = client.Append(path, data) | must_succ;

	checkData(path, offset, data);

	return TestResult::Pass();
}

TestResult BasicTest::TestPersistentMaster()
{
	static const std::string path = "/PersistentMaster/file.txt";
	client.Mkdir("/PersistentMaster") | must_succ;
	client.Create(path) | must_succ;

	std::vector<char> data(pressure);
	generateRandomData(data, 0x57721566);

	std::uint64_t offset = client.Append(path, data) | must_succ;

	env.Stop(0);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	env.StartMaster(0);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	checkData(path, offset, data);

	return TestResult::Pass();
}

TestResult BasicTest::TestDiskError()
{
	static const std::string path = "/DiskError.txt";
	client.Create(path) | must_succ;

	std::vector<char> data[4];
	std::uint64_t offset[4];
	for (size_t i = 0; i < 4; i++)
	{
		data[i].resize(pressure);
		generateRandomData(data[i], 0x12345678 + i);
	}

	offset[0] = client.Append(path, data[0]) | must_succ;

	env.ResetChunkServer(1);
	env.ResetChunkServer(2);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	checkData(path, offset[0], data[0]);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	offset[1] = client.Append(path, data[1]) | must_succ;

	env.ResetChunkServer(2);
	env.ResetChunkServer(3);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	checkData(path, offset[1], data[1]);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	offset[2] = client.Append(path, data[2]) | must_succ;

	env.ResetChunkServer(3);
	env.ResetChunkServer(4);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	checkData(path, offset[2], data[2]);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	offset[3] = client.Append(path, data[3]) | must_succ;

	env.ResetChunkServer(4);
	env.ResetChunkServer(1);

	std::this_thread::sleep_for(std::chrono::seconds(serverTimeout));

	checkData(path, offset[3], data[3]);
	checkData(path, offset[2], data[2]);
	checkData(path, offset[1], data[1]);
	checkData(path, offset[0], data[0]);

	return TestResult::Pass();
}
