//
// Created by Aaron Ren on 21/07/2017.
//

#include "master.h"
#include <fstream>

AFS::Master::Master(LightDS::Service &srv, const std::string &rootDir)
		: Server(srv, rootDir), logc(rootDir, std::bind(&Master::write, this, std::placeholders::_1))  {

	chunkPort = 7778;
	bindFunctions();
}

void AFS::Master::bindFunctions()
{
	srv.RPCBind<std::tuple<GFSError, std::vector<ChunkHandle>>
			(std::vector<ChunkHandle>, std::vector<std::tuple<ChunkHandle, ChunkVersion>>, std::vector<ChunkHandle>)>
			("Heartbeat", std::bind(&Master::RPCHeartbeat, this,
									std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

	srv.RPCBind<std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t>
			(ChunkHandle)>
			("GetPrimaryAndSecondaries", std::bind(&Master::RPCGetPrimaryAndSecondaries, this,
												   std::placeholders::_1));

	srv.RPCBind<std::tuple<GFSError, std::vector<std::string>>
			(ChunkHandle)>
			("GetReplicas", std::bind(&Master::RPCGetReplicas, this,
									  std::placeholders::_1));

	srv.RPCBind<std::tuple<GFSError, bool, std::uint64_t, std::uint64_t>
			(std::string)>
			("GetFileInfo", std::bind(&Master::RPCGetFileInfo, this,
									  std::placeholders::_1));

	srv.RPCBind<GFSError
			(std::string)>
			("CreateFile", std::bind(&Master::RPCCreateFile, this,
									 std::placeholders::_1));

	srv.RPCBind<GFSError
			(std::string)>
			("DeleteFile", std::bind(&Master::RPCDeleteFile, this,
									 std::placeholders::_1));

	srv.RPCBind<GFSError
			(std::string)>
			("Mkdir", std::bind(&Master::RPCMkdir, this,
								std::placeholders::_1));

	srv.RPCBind<std::tuple<GFSError, std::vector<std::string>>
			(std::string)>
			("ListFile", std::bind(&Master::RPCListFile, this,
								   std::placeholders::_1));

	srv.RPCBind<std::tuple<GFSError, ChunkHandle>
			(std::string, std::uint64_t)>
			("GetChunkHandle", std::bind(&Master::RPCGetChunkHandle, this,
										 std::placeholders::_1, std::placeholders::_2));
}

void AFS::Master::checkDeadChunkServer() {
	//std::cerr << "original num = " << asdm.mp.size() << std::endl;
	auto deleted = std::move(*asdm.checkDeadChunkServer().release());
	//std::cerr << "original num = " << asdm.mp.size() << std::endl;
}

void AFS::Master::collectGarbage() {
	pfdm.deleteDeletedFiles();
}

size_t AFS::Master::leaseExtend(const Address &addr, const std::vector<AFS::ChunkHandle> &handles) {
	auto permission = [addr = addr](const ChunkData & data)->bool {
		return (data.leaseGrantTime != 0) && (addr == data.primary);
	};
	auto extendLease = [](ChunkData & data) {
		data.leaseGrantTime = time(nullptr);
	};
	//TODO...
	size_t cnt = 0;
	for (auto &&handle : handles) {
		cnt += (size_t)MemoryPool::instance().updateData_if(handle, permission, extendLease);
	}
	return cnt;
}

AFS::MasterError AFS::Master::updateChunkServer(const AFS::Address &addr,
                                                const std::vector<std::tuple<AFS::ChunkHandle, AFS::ChunkVersion>> &chunks) {
	asdm.updateChunkServer(addr, chunks);
	// todo err
	return MasterError::OK;
}

std::unique_ptr<std::vector<AFS::ChunkHandle>>
AFS::Master::checkGarbage(const std::vector<std::tuple<AFS::ChunkHandle,
		AFS::ChunkVersion>> &chunks) {
	auto result = std::make_unique<std::vector<ChunkHandle>>();
	auto expired = [](ChunkVersion version, const ChunkData & data) {
		return version < data.version;
	};
	for (auto &&chunk : chunks) {
		if (MemoryPool::instance().checkData(std::get<0>(chunk),
		                                     std::bind(expired, std::get<1>(chunk), std::placeholders::_1)))
			result->push_back(std::get<0>(chunk));
	}
	return result;
}

void AFS::Master::deleteFailedChunks(const AFS::Address &addr,
                                     const std::vector<AFS::ChunkHandle> &chunks) {
	asdm.deleteFailedChunks(addr, chunks);
}

std::tuple<AFS::GFSError, std::vector<AFS::ChunkHandle>>
AFS::Master::RPCHeartbeat(std::vector<AFS::ChunkHandle> leaseExtensions,
                          std::vector<std::tuple<AFS::ChunkHandle, AFS::ChunkVersion>> chunks,
                          std::vector<AFS::ChunkHandle> failedChunks) {
	readLock glk(globalMutex);
	if (!running)
		return std::make_tuple(GFSError(GFSErrorCode::MasterDown), std::vector<AFS::ChunkHandle>());

	std::ofstream fout("./data/" + srv.getRPCCaller() + ".txt", std::ios::app);
	fout << srv.getRPCCaller() << " beats " << time(nullptr) << std::endl;
	fout << "got: ";
	for (auto &&chunk : chunks) {
		fout << "(" << std::get<0>(chunk) << ", " << std::get<1>(chunk) << ")  ";
	}
	fout << std::endl;
	fout << "failed: ";
	for (auto &&failedChunk : failedChunks) {
		fout << failedChunk << ' ';
	}
	fout << std::endl;

	std::unique_lock<std::mutex> blk(backgroundM);
	auto result1 = ErrTranslator::masterErrTOGFSError(updateChunkServer(srv.getRPCCaller(), chunks));
	blk.unlock();

	auto success_num = leaseExtend(srv.getRPCCaller(), leaseExtensions);

	auto result2 = std::move(*checkGarbage(chunks).release());
	fout << "Garbage: ";
	for (auto &&item : result2) {
		fout << item << ' ';
	}
	fout << std::endl;

	deleteFailedChunks(srv.getRPCCaller(), failedChunks);

	return std::make_tuple(GFSError(result1), result2);
}

std::tuple<AFS::GFSError, AFS::ChunkHandle>
AFS::Master::RPCGetChunkHandle(std::string path_str, std::uint64_t chunkIndex) {
	readLock glk(globalMutex);
	if (!running)
		return std::make_tuple(GFSError(GFSErrorCode::MasterDown), ChunkHandle());

	auto logptr = logc.getPtr(OpCode::AddChunk);
	auto path = PathParser::instance().parse(path_str);

	auto createChunk = [&](ChunkHandle handle)->bool {
		Address addr;
		GFSError err;
		bool result = false;
		for (int k = 0; k < 3; ++k) { // 创建3个副本
			for (int i = 0; i < 5; ++i) {
				auto data = MemoryPool::instance().getData(handle);
				addr = asdm.chooseServer(data.location);
				if (addr == "") { // tem
					return false;
				}
				try {
					err = srv.RPCCall({addr, chunkPort}, "CreateChunk", handle).get().as<GFSError>();
				} catch (...) {
					return false;
				}
				if (err.errCode == GFSErrorCode::OK)
					break;
			}
			if (err.errCode == GFSErrorCode::OK) {
				asdm.addChunk(addr, handle);
				result = true;
			}
		}
		return result;
	};

	auto errMd = pfdm.getHandle(*path, chunkIndex, createChunk);
	return std::make_tuple(
			ErrTranslator::masterErrTOGFSError(errMd.first),
			errMd.second
	);
}

std::tuple<AFS::GFSError, std::vector<std::string>>
AFS::Master::RPCGetReplicas(AFS::ChunkHandle handle) {
	readLock glk(globalMutex);
	if (!running)
		return std::make_tuple(GFSError(GFSErrorCode::MasterDown), std::vector<std::string>());

	GFSError err;
	bool exists = MemoryPool::instance().exists(handle);
	if (!exists) {
		err.errCode = GFSErrorCode::NoSuchChunk;
		return std::make_tuple(err, std::vector<std::string>());
	}
	auto data = MemoryPool::instance().getData(handle);
	err.errCode = GFSErrorCode::OK;

	for( auto itr : data.location )
		itr = LightDS::User::RPCAddress({itr,chunkPort}).to_string();

	return std::make_tuple(err, std::move(data.location));
}

std::tuple<AFS::GFSError, bool, uint64_t, uint64_t>
AFS::Master::RPCGetFileInfo(std::string path_str) {
	std::cerr << "trying getting file info....\n";
	readLock glk(globalMutex);
	std::cerr << "RPCCall...";
	if (!running)
		return std::make_tuple(GFSError(GFSErrorCode::MasterDown), 0, 0, 0);
	std::cerr << "start getting...\n";
	GFSError err;
	std::cerr << "1...";
	auto path = PathParser::instance().parse(path_str);

	std::cerr << "2...";
	auto errMd = pfdm.getData(*path);

	std::cerr << "OK\n";
	err.errCode = ErrTranslator::masterErrTOGFSError(errMd.first);
	std::cerr << "got!!\n";
	if (errMd.first == MasterError::NotExists)
		return std::make_tuple(err, false, std::uint64_t(), std::uint64_t());
	if (errMd.second.type == FileData::Type::Folder) {
		err.errCode = GFSErrorCode::WrongOperation;
		return std::make_tuple(err, true, std::uint64_t(), std::uint64_t());
	}
	return std::make_tuple(err, false, -1, errMd.second.handles.size());
}

AFS::GFSError AFS::Master::RPCCreateFile(std::string path_str) {
	readLock glk(globalMutex);
	if (!running)
		return GFSError(GFSErrorCode::MasterDown);

	GFSError result;
	auto path = PathParser::instance().parse(path_str);
	MasterError err = pfdm.createFile(*path);
	result.errCode = ErrTranslator::masterErrTOGFSError(err);
	return result;
}

AFS::GFSError AFS::Master::RPCMkdir(std::string path_str) {
	readLock glk(globalMutex);
	if (!running)
		return GFSError(GFSErrorCode::MasterDown);

	GFSError result;
	auto path = PathParser::instance().parse(path_str);
	MasterError err = pfdm.createFolder(*path);
	result.errCode = ErrTranslator::masterErrTOGFSError(err);
	return result;
}

std::tuple<AFS::GFSError, std::vector<std::string>>
AFS::Master::RPCListFile(std::string path_str) {
	readLock glk(globalMutex);
	if (!running)
		return std::make_tuple(GFSError(GFSErrorCode::MasterDown), std::vector<std::string>());

	std::tuple<GFSError, std::vector<std::string>> result;
	auto path = PathParser::instance().parse(path_str);
	auto errPtr = pfdm.listName(*path);
	std::get<0>(result).errCode = ErrTranslator::masterErrTOGFSError(errPtr.first);
	std::get<1>(result) = *errPtr.second;
	return result;
}

AFS::GFSError AFS::Master::RPCDeleteFile(std::string path_str) {
	readLock glk(globalMutex);
	if (!running)
		return GFSError(GFSErrorCode::MasterDown);

	auto path = PathParser::instance().parse(path_str);
	return ErrTranslator::masterErrTOGFSError(pfdm.deleteFile(*path));
}

std::tuple<AFS::GFSError, std::string, std::vector<std::string>, uint64_t>
AFS::Master::RPCGetPrimaryAndSecondaries(AFS::ChunkHandle handle) {
	readLock glk(globalMutex);
	if (!running)
		return std::make_tuple(GFSError(GFSErrorCode::MasterDown), "", std::vector<std::string>(), 0);

	GFSError err;
	auto leaseExpired = [](const ChunkData & data) {
		return data.leaseGrantTime + LeaseExpiredTime < time(nullptr);
	};
	bool grt = false;
	auto grantLease = [&](ChunkData & data) {
		// 尝试和某台服务器签订租约，如果尝试过后全都失败，则失败
		GFSError terr;
		int idx = 0;
		data.primary = "";
		std::cerr << "trying to grant lease:\n";
		for (auto &&addr : data.location) {
			std::cerr << "trying " << addr << std::endl;
			auto tmpTime = time(nullptr);
			try {
				std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>> tmp;
				tmp.emplace_back(std::make_tuple(handle, data.version + 1, tmpTime + LeaseExpiredTime));
				terr = srv.RPCCall({addr, chunkPort},
								   "GrantLease", tmp)
						.get().as<GFSError>();
			} catch (...) {

			}
			if (terr.errCode == GFSErrorCode::OK) {
				std::cerr << "success" << std::endl;
				data.leaseGrantTime = tmpTime;
				data.version++;
				data.primary = addr;
				grt = true;
				break;
			}
		}
	};
	MemoryPool::instance().updateData_if(handle, leaseExpired, grantLease);

	auto data = MemoryPool::instance().getData(handle);
	if (data.primary == "") { // 签订租约失败
		err.errCode = GFSErrorCode::Failed;
		return std::make_tuple(
				err, Address(), std::vector<Address>(), time_t()
		);
	}
	// 签订租约成功
	remove(data.location, data.primary);
	if (grt) {

		for (auto &&server : data.location) {
			srv.RPCCall({server, chunkPort}, "UpdateVersion", handle, data.version);
		}
	}
	err.errCode = GFSErrorCode::OK;

	std::cerr << "Chunk: " << handle << "\nPrimary is " << data.primary << std::endl;
	std::cerr << "Secondaries: \n";
	for (auto &&item : data.location)
		std::cerr << item << ' ';
	std::cerr << std::endl;

	return std::make_tuple(
			err, data.primary, data.location, data.leaseGrantTime + LeaseExpiredTime
	);
}

void AFS::Master::reReplicate() {
	auto servers = std::move(*(asdm.listServers().release()));
	if (servers.empty()) {
		return;
	}
	auto rpcCall = [&](const Address & src, const Address & tar, ChunkHandle handle)->GFSError {
		GFSError err;
		try {
			auto debug_data = MemoryPool::instance().getData(handle);
			std::cerr << src << "sending...\n";
			err = srv.RPCCall({src, chunkPort}, "SendCopy", handle, tar).get().as<GFSError>();
			if (err.errCode != GFSErrorCode::OK)
				return err;
			std::cerr << "sent!!!!\n";
			err.errCode = GFSErrorCode::OK;
			asdm.addChunk(tar, handle);
		} catch (...) {
			std::cerr << "wtf wtf wtf\n";
			err.errCode = GFSErrorCode::TransmissionErr;
		}
		return err;
	};
	pfdm.reReplicate(servers, rpcCall);
}

void AFS::Master::BackgroundActivity() {
	std::unique_lock<std::mutex> glk(shutdownM);
	int cnt = 0;
	while (1) {
		std::unique_lock<std::mutex>(backgroundM);
		if (!running)
			break;
		checkDeadChunkServer();
		//std::cerr << "bg check dead servers\n";
//		collectGarbage();
		reReplicate();
		//std::cerr << "bg reReplicate\n";
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
	std::cerr << "Background ended\n";
}

void AFS::Master::write(std::ofstream &out) const {
	MemoryPool::instance().write(out);
	asdm.write(out);
	pfdm.write(out);
}

void AFS::Master::read(std::ifstream &in) {
	pfdm.clear();
	asdm.clear();
	MemoryPool::instance().clear();
	MemoryPool::instance().read(in);
	asdm.read(in);
	pfdm.read(in);
}

void AFS::Master::Start() {
	std::unique_lock<std::mutex>(backgroundM);
	writeLock lk(globalMutex);
	load();
	running = true;
	std::thread t(&Master::BackgroundActivity, this);
	t.detach();
}

void AFS::Master::Shutdown() {
	std::unique_lock<std::mutex> blk(backgroundM);
	writeLock lk(globalMutex);
	running = false;
	blk.unlock();
	std::unique_lock<std::mutex> sdlk(shutdownM);
	save();
	pfdm.clear();
	std::cerr << "pfdm cleared\n";
	asdm.clear();
	std::cerr << "asdm cleared\n";
	MemoryPool::instance().clear();
	std::cerr << "memory pool cleared\n";
}

void AFS::Master::save() {
	std::ofstream fout(rootDir.string() + "archive.dat");
	write(fout);
}

void AFS::Master::load() {
	std::ifstream fin(rootDir.string() + "archive.dat");
	if (!fin.good()) {
//			std::cerr << "No archive" << std::endl;
		return;
	}
	read(fin);
	asdm.clear();
}
