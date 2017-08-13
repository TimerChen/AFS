/*
 * offset changes
 */
#include "client.h"

#include <fstream>
#include <sstream>

#include "error.hpp"
#include "master.h"
#include "message.hpp"

namespace AFS {

bool Client::checkMasterAddress() const {
	return !masterAdd.empty();
}

Client::Client(LightDS::User &_srv, const Address &MasterAdd, const uint16_t &MasterPort, const uint16_t &ChunkPort)
	: srv(_srv), masterAdd(MasterAdd), masterPort(MasterPort),chunkPort(ChunkPort)
{
	if( masterAdd == "" )
	{
		//setMaster_test();
		setMaster("127.0.0.10",7777);
	}
	buffer = new char[CHUNK_SIZE];
}

Client::~Client()
{
	delete [] buffer;
}


void Client::setMaster(Address add, uint16_t port) {
	masterAdd = std::move(add);
	masterPort = std::move(port);
}
void Client::setMaster_test()
{
	auto addr = srv.ListService("master")[0];
	//std::cerr << "Set Master(test) " << addr.ip << ":" << addr.port << std::endl;
	masterAdd = addr.ip;
	masterPort = addr.port;
}

ClientErr Client::fileCreate(const std::string &dir) {
	setMaster_test();
	if(!checkMasterAddress())
	{
		return ClientErr(ClientErrCode::MasterNotFound);
	}
	std::cerr << "ClientCreate\n";
	GFSError err;
	try {
		err = srv.RPCCall({masterAdd, masterPort}, "CreateFile", dir).get().as<GFSError>();
	} catch (...) {
		err.errCode = GFSErrorCode::TransmissionErr;
	}
	if (err.errCode == GFSErrorCode::OK)
		return ClientErr(ClientErrCode::OK);

	// handle err
	switch ((int)err.errCode) {
		case (int)GFSErrorCode::FileDirAlreadyExists:
			return ClientErr(ClientErrCode::FileDirAlreadyExists);
		case (int)GFSErrorCode::NoSuchFileDir:
			return ClientErr(ClientErrCode::NoSuchFileDir);
		default:
			break;
	}
	return ClientErr(ClientErrCode::Unknown);
}

ClientErr Client::fileMkdir(const std::string &dir) {
	if(!checkMasterAddress())
		return ClientErr(ClientErrCode::MasterNotFound);
	GFSError err;
	try {
		err = srv.RPCCall({masterAdd, masterPort}, "Mkdir", dir).get().as<GFSError>();
	} catch (...) {
		err.errCode = GFSErrorCode::TransmissionErr;
	}
	if (err.errCode == GFSErrorCode::OK)
		return ClientErr(ClientErrCode::OK);

	// handle err
	switch ((int)err.errCode) {
		case (int)GFSErrorCode::FileDirAlreadyExists:
			return ClientErr(ClientErrCode::FileDirAlreadyExists);
		case (int)GFSErrorCode::NoSuchFileDir:
			return ClientErr(ClientErrCode::NoSuchFileDir);
		default:
			break;
	}
	return ClientErr(ClientErrCode::Unknown);
}


/* fileAppend行为：
 * 1. 首先向master询问文件的信息，如果失败则直接返回对应错误信息，否则继续运行。
 * 2. 根据该文件含chunk个数定idx，并向master询问该chunk的handle
 *    a. 若成功则继续
 *    b. 若因NoSuchChunk或Unknown失败，则都返回Unknown，因为这一步不应该找不到。
 * 3. 向master询问该chunk的各个副本位置，主副本位置等。
 *    a. 成功则继续，任何类型失败都退出。
 * 4. 向各个副本推送数据，使用rand()来作为dataID，只要有一个失败，
 *    就重新推送，重复x次，在之后的重复中，会优先向上一次失败的副本推送。//todo 重复3次？
 * 5. 完成推送后，会通知主副本应用修改。
 *    a. 成功则继续，Unknown则退出
 *    b. 如果返回不再持有租约，则会回到3继续进行，重复3次
 *    c. 如果返回已满，则++idx，然后回到2，重复3次
 */
ClientErr Client::fileAppend_str(const std::string &dir, const std::string &data, std::uint64_t &offset) {
	GFSError gErr;
	std::uint64_t chunkIdx, handle, id, expire;
	std::string primary;
	std::vector<std::string> secondaries;

	ClientErr err(ClientErrCode::Unknown);
	enum {
		GetFileInfo = 0, GetHandle, GetAddresses, PushData, ApplyChunk,
		EndFlow
	};
	int cur = GetFileInfo;

	auto getFileInfo = [&]()->void {
		std::cerr << "get file info\n";
		bool isDir;
		std::uint64_t len;
		try {
			std::tie(gErr, isDir, len, chunkIdx)
					= srv.RPCCall({masterAdd, masterPort}, "GetFileInfo", dir).get()
					.as<std::tuple<GFSError,
							bool /*IsDir*/,
							std::uint64_t /*Length*/,
							std::uint64_t /*Chunks*/>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode == GFSErrorCode::NoSuchFileDir) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::NoSuchFileDir);
			return;
		}
		if (isDir) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::WrongOperation);
			return;
		}
		cur = GetHandle;
	};
	auto getHandle  = [&]()->void {
		std::cerr << "get handle\n";
		try {
			if(chunkIdx == 0)
				chunkIdx++;
			std::tie(gErr, handle) =
					srv.RPCCall({masterAdd, masterPort}, "GetChunkHandle", dir, chunkIdx-1
						  ).get().as<std::tuple<GFSError, ChunkHandle>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		switch ((int)gErr.errCode) {
			case (int) GFSErrorCode::OK:
				break;
			default:
				cur = EndFlow;
				err = ClientErr(ClientErrCode::Unknown);
				return;
		}
		cur = GetAddresses;
	};
	auto getAddresses = [&]()->void {
		static int repeatTime = 0;
		std::cerr << "get address\n";
		try {
			std::tie(gErr, primary, secondaries, expire)
					= srv.RPCCall({masterAdd, masterPort}, "GetPrimaryAndSecondaries", handle)
					.get().as<std::tuple<GFSError,
							std::string /*Primary Address*/,
							std::vector<std::string> /*Secondary Addresses*/,
							std::uint64_t /*Expire Timestamp*/>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode == GFSErrorCode::NoSuchChunk) {
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}

		if (gErr.errCode != GFSErrorCode::OK) {
						std::cerr <<"GAError-"<< (int)gErr.errCode << ":" << gErr.description << std::endl;
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				cur = PushData;
			} else {
				cur = EndFlow;
			}
		}
		cur = PushData;
	};
	auto pushData = [&]()->void {
		std::cerr << "push data\n";
		static int repeatTime = 0;
		id = rand();
		try {
			gErr = srv.RPCCall({primary, chunkPort}, "PushData", id, data)
					.get().as<GFSError>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
		}
		if (gErr.errCode != GFSErrorCode::OK) {

			std::cerr <<"PD0Error-"<< (int)gErr.errCode << ":" << gErr.description << std::endl;
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				cur = PushData;
			} else {
				cur = EndFlow;
			}
		}
		const auto & addrs = secondaries;
		auto iter = addrs.begin();
		for (; iter != addrs.end(); ++iter) {
			try {
				gErr = srv.RPCCall({*iter, chunkPort}, "PushData", id, data).get().as<GFSError>();
			} catch (...) {
				gErr.errCode = GFSErrorCode::TransmissionErr;
				err = ClientErr(ClientErrCode::Unknown);
			}
			if (gErr.errCode != GFSErrorCode::OK)
				break;
		}
		if (iter != addrs.end()) {

			std::cerr <<"PD1Error-"<< (int)gErr.errCode << ":" << gErr.description << std::endl;
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				cur = PushData;
			} else {
				cur = EndFlow;
			}
			return;
		}
		repeatTime = 0;
		cur = ApplyChunk;
	};
	auto applyChunk = [&]()->void {
		std::cerr << "apply chunk\n";
		try {
			std::tie(gErr, offset)
					= srv.RPCCall({primary, chunkPort}, "AppendChunk", handle, id, secondaries).get()
					.as<std::tuple<GFSError, std::uint64_t>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
		}
		static int repeatTime = 0;
		if (gErr.errCode == GFSErrorCode::OperationOverflow) {
			++chunkIdx;
			cur = GetHandle;
			return;
		}else
		if (gErr.errCode != GFSErrorCode::OK) {
			std::cerr <<"ACError-"<< (int)gErr.errCode << ":" << gErr.description << std::endl;
			if (repeatTime < 3) {
				++repeatTime;
				cur = GetFileInfo;
			} else {
				std::cerr << err.description << std::endl;
				err = ClientErrCode::Unknown;
				cur = EndFlow;
			}
			return ;
		}
		err = ClientErrCode::OK;
		cur = EndFlow;
	};
	auto endFlow = [&]()->void {

	};

	std::vector<std::function<void()>> states
			= {getFileInfo, getHandle, getAddresses, pushData, applyChunk, endFlow};


	while (cur != EndFlow) {
		states[cur]();
	}
	offset = (chunkIdx-1)*CHUNK_SIZE + offset;
	return err;
}
ClientErr Client::fileAppend(const std::string &dir, const std::string &localDir) {
	std::uint64_t fileLength;

	//Get file length
	std::fstream fin;
	std::uint64_t offset;
	fin.open(localDir, std::ios::in | std::ios::binary);
	fin.seekg( 0, std::ios_base::end );
	fileLength = fin.tellg();
	fin.seekg( 0, std::ios_base::beg );

	if( fileLength > CHUNK_SIZE/4 )
		return { ClientErrCode::WrongOperation, "Append-data is out of excepted length."};

	fin.read( buffer, fileLength );
	return fileAppend_str( dir, std::string(buffer, fileLength), offset );
}

/* fileWrite行为：
 * 1. 首先向master询问文件的信息，如果失败则直接返回对应错误信息，否则继续运行。
 * 2. 根据该文件含chunk个数定idx，并向master询问该chunk的handle
 *    a. 若成功则继续
 *    b. 若因NoSuchChunk或Unknown失败，则都返回Unknown，因为这一步不应该找不到。
 * 3. 向master询问该chunk的各个副本位置，主副本位置等。
 *    a. 成功则继续，任何类型失败都退出。
 * 4. 向各个副本推送数据，使用rand()来作为dataID，只要有一个失败，
 *    就重新推送，重复x次，在之后的重复中，会优先向上一次失败的副本推送。//todo 重复3次？
 * 5. 完成推送后，会通知主副本应用修改。
 *    a. 成功则继续，Unknown则退出
 *    b. 如果返回不再持有租约，则会回到3继续进行，重复3次
 *    c. 如果返回已满，则++idx，然后回到2，重复3次
 */
ClientErr Client::fileWrite_str(const std::string &dir, const std::string &data, const std::uint64_t &offset) {
	GFSError gErr;
	std::uint64_t chunkIdx, handle, id, expire, length;
	std::string primary;
	std::vector<std::string> secondaries;

	ClientErr err(ClientErrCode::Unknown);
	enum {
		GetFileInfo = 0, GetHandle, GetAddresses, PushData, ApplyChunk,
		EndFlow
	};
	int cur = GetFileInfo;
	length = data.size();

	auto getFileInfo = [&]()->void {
		std::cerr << "GetFileInfo\n";
		bool isDir;
		std::uint64_t len;
		try {
			std::tie(gErr, isDir, len, chunkIdx)
					= srv.RPCCall({masterAdd, masterPort}, "GetFileInfo", dir).get()
					.as<std::tuple<GFSError,
							bool /*IsDir*/,
							std::uint64_t /*Length*/,
							std::uint64_t /*Chunks*/>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode == GFSErrorCode::NoSuchFileDir) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::NoSuchFileDir);
			return;
		}
		if (isDir) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::WrongOperation);
			return;
		}
		//Write overflow
		if ( offset + length > len ) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::WrongOperation);
			return;
		}
		cur = GetHandle;
	};
	auto getHandle  = [&]()->void {
		std::cerr << "GetHandle\n";
		chunkIdx = offset/CHUNK_SIZE;
		try {
			std::tie(gErr, handle) =
					srv.RPCCall({masterAdd, masterPort}, "GetChunkHandle", dir,
					            chunkIdx).get().as<std::tuple<GFSError, ChunkHandle>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		switch ((int)gErr.errCode) {
			case (int) GFSErrorCode::OK:
				break;
			default:
				cur = EndFlow;
				err = ClientErr(ClientErrCode::Unknown);
				return;
		}
		cur = GetAddresses;
	};
	auto getAddresses = [&]()->void {
		std::cerr << "GetAddress\n";
		try {
			std::tie(gErr, primary, secondaries, expire)
					= srv.RPCCall({masterAdd, masterPort}, "GetPrimaryAndSecondaries", handle)
					.get().as<std::tuple<GFSError,
							std::string /*Primary Address*/,
							std::vector<std::string> /*Secondary Addresses*/,
							std::uint64_t /*Expire Timestamp*/>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode == GFSErrorCode::NoSuchChunk) {
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK)
			throw;
		cur = PushData;
	};
	auto pushData = [&]()->void {
		std::cerr << "PushData\n";
		static int repeatTime = 0;
		id = rand();
		try {
			gErr = srv.RPCCall({primary, chunkPort}, "PushData", id, data)
					.get().as<GFSError>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK) {
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				cur = PushData;
			} else {
				cur = EndFlow;
			}
		}
		const auto & addrs = secondaries;
		auto iter = addrs.begin();
		for (; iter != addrs.end(); ++iter) {
			try {
				gErr = srv.RPCCall({*iter, chunkPort}, "PushData", id, data).get().as<GFSError>();
			} catch (...) {
				gErr.errCode = GFSErrorCode::TransmissionErr;
				err = ClientErr(ClientErrCode::Unknown);
				cur = EndFlow;
				return;
			}
			if (gErr.errCode != GFSErrorCode::OK)
				break;
		}
		if (iter != addrs.end()) {
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				cur = PushData;
			} else {
				cur = EndFlow;
			}
		}
		repeatTime = 0;
		cur = ApplyChunk;
	};
	auto applyChunk = [&]()->void {
		std::cerr << "ApplyChunk\n";
		try {
			gErr = srv.RPCCall({primary, chunkPort}, "WriteChunk", handle, id, offset%CHUNK_SIZE, secondaries).get()
					.as<GFSError>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		static int repeatTime = 0;
		if (gErr.errCode != GFSErrorCode::OK) {
			if (repeatTime < 3) {
				++repeatTime;
				cur = GetFileInfo;
			} else {
				err = ClientErrCode::Unknown;
				cur = EndFlow;
			}
			return ;
		}
		err = ClientErrCode::OK;
		cur = EndFlow;
	};
	auto endFlow = [&]()->void {};

	std::vector<std::function<void()>> states
			= {getFileInfo, getHandle, getAddresses, pushData, applyChunk, endFlow};

	while (cur != EndFlow) {
		states[cur]();
	}
	return err;
}
ClientErr Client::fileWrite(const std::string &dir, const std::string &localDir, const std::uint64_t &offset)
{
	std::uint64_t fileLength,nowOffset=offset;
	ClientErr er;

	//Get file length
	std::fstream fin;
	fin.open(localDir, std::ios::in | std::ios::binary);
	fin.seekg( 0, std::ios_base::end );
	fileLength = fin.tellg();
	fin.seekg( 0, std::ios_base::beg );

	//Spilit file into pieces
	std::uint64_t nowPosition = 0, thisLength;
	while( nowPosition < fileLength )
	{
		thisLength = std::min( CHUNK_SIZE-nowOffset%CHUNK_SIZE, fileLength-nowPosition );

		fin.read( buffer, thisLength );

		er = fileWrite_str( dir, std::string( buffer, thisLength ), offset );
		if( er.code != ClientErrCode::OK )
			break;

		nowPosition += thisLength;
		nowOffset += thisLength;
	}

	fin.close();
	return er;
}
ClientErr Client::fileRead_str(const std::string &dir, std::string &data, const std::uint64_t &offset, const std::uint64_t &length)
{
	GFSError gErr;
	std::uint64_t chunkIdx, handle;
	std::string primary;
	std::vector<std::string> replicas;

	ClientErr err(ClientErrCode::Unknown);
	enum {
		GetFileInfo = 0, GetHandle, GetAddresses, ReadChunk,
		EndFlow
	};
	int cur = GetFileInfo;

	auto getFileInfo = [&]()->void {
		bool isDir;
		std::uint64_t len;
		try {
			std::tie(gErr, isDir, len, chunkIdx)
					= srv.RPCCall({masterAdd, masterPort}, "GetFileInfo", dir).get()
					.as<std::tuple<GFSError,
							bool /*IsDir*/,
							std::uint64_t /*Length*/,
							std::uint64_t /*Chunks*/>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode == GFSErrorCode::NoSuchFileDir) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::NoSuchFileDir);
			return;
		}
		if (isDir) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::WrongOperation);
			return;
		}
		//Read overflow
		if ( offset + length > len ) {
			cur = EndFlow;
			err = ClientErr(ClientErrCode::WrongOperation);
			return;
		}
		cur = GetHandle;
	};
	auto getHandle  = [&]()->void {
		chunkIdx = offset/CHUNK_SIZE;

		try {
			std::tie(gErr, handle) =
					srv.RPCCall({masterAdd, masterPort}, "GetChunkHandle", dir,
					            chunkIdx).get().as<std::tuple<GFSError, ChunkHandle>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		switch ((int)gErr.errCode) {
			case (int) GFSErrorCode::OK:
				break;
			default:
				cur = EndFlow;
				err = ClientErr(ClientErrCode::Unknown);
				return;
		}
		cur = GetAddresses;
	};
	auto getAddresses = [&]()->void {
		try {
			std::tie(gErr, replicas)
					= srv.RPCCall({masterAdd, masterPort}, "GetReplicas", handle)
					.get().as<std::tuple<GFSError, std::vector<std::string> /*Locations*/>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode == GFSErrorCode::NoSuchChunk) {
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK)
			throw;

		//Select one replica to read chunk data.
		primary = replicas[rand()%replicas.size()];
		cur = ReadChunk;
	};
	auto readChunk = [&]()->void {
		try {
			std::cerr << "Romote Read Length:" << length << std::endl;
			std::tie(gErr, data) =
					srv.RPCCall(LightDS::User::RPCAddress::from_string(primary), "ReadChunk", handle, offset%CHUNK_SIZE, length).get()
							.as<std::tuple<GFSError, std::string>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			cur = EndFlow;
			return;
		}
		static int repeatTime = 0;
		if (gErr.errCode == GFSErrorCode::OperationOverflow)
		{
			err = ClientErr(ClientErrCode::ReadEOF);
		}else
		if (gErr.errCode != GFSErrorCode::OK) {
			if (repeatTime < 3) {
				++repeatTime;
				cur = GetFileInfo;
			} else {
				err = ClientErrCode::Unknown;
				cur = EndFlow;
			}
			return ;
		}else
		{
			err = ClientErrCode::OK;
		}
		cur = EndFlow;
	};
	auto endFlow = [&]()->void {};

	std::vector<std::function<void()>> states
			= {getFileInfo, getHandle, getAddresses, readChunk, endFlow};


	while (cur != EndFlow) {
		states[cur]();
	}
	return err;
}

ClientErr Client::fileRead(const std::string &dir, const std::string &localDir, const std::uint64_t &offset, const std::uint64_t &fileLength)
{
	std::uint64_t nowOffset=offset;
	ClientErr er;
	std::string data;

	//Open file
	std::fstream fout;
	fout.open(localDir, std::ios::out | std::ios::binary | std::ios::trunc);

	//Spilit file into pieces
	std::uint64_t thisLength;
	while( nowOffset < fileLength )
	{
		thisLength = std::min( CHUNK_SIZE-nowOffset%CHUNK_SIZE, fileLength-nowOffset );

		er = fileRead_str( dir, data, offset, thisLength );
		if( er.code != ClientErrCode::OK )
			return er;
		fout.write( data.c_str(), thisLength );

		nowOffset += thisLength;
	}

	fout.close();
	return er;
}

void Client::Run( std::istream &in, std::ostream &out )
{
	std::string line, operation, dir, localDir;
	std::uint64_t offset, fileLength;
	ClientErr cError;
	std::stringstream ss;
//	std::vector<std::string>

	//Hint to tell you can input a new instruction.
	out << "> ";
	while (std::getline(in, line))
	{
		//Deal with the instruction
		ss.clear();
		ss.str(line);
		ss >> operation;
		if( operation == "" )
		{
			cError = { ClientErrCode::OK, ""};
		}else
		if( operation == "ls" )
		{
			ss >> dir;
			std::vector<std::string> names;
			std::tie(cError, names);
			for (auto &&item : names) {
				out << item << std::endl;
			}
			cError = { ClientErrCode::OK, ""};
		}else
		if( operation == "create" )
		{
			ss >> dir;
			cError = fileCreate( dir );
		}else
		if( operation == "mkdir" )
		{
			ss >> dir;
			cError = fileMkdir( dir );
		}else


		if( operation == "read" )
		{
			ss >> dir >> localDir
			   >> offset >> fileLength;
			cError = fileRead( dir, localDir, offset, fileLength);
		}else
		if( operation == "write" )
		{
			ss >> dir >> localDir >> offset;
			cError = fileWrite( dir, localDir, offset );
		}else
		if( operation == "append" )
		{
			ss >> dir >> localDir;
			cError = fileAppend( dir, localDir );
		}else


		if( operation == "exit")
		{
			break;
		}else

		{
			out << "Error: Operation[ "<<operation<<" ] not found." << std::endl;
			continue;
		}

		//Output the error
		if( cError.code != ClientErrCode::OK )
		{
			out << "Error " << (uint16_t)cError.code << ":\n\t" << cError.description
				<< std::endl;
		}

		//Hint to tell you can input a new instruction.
		out << "> ";
	}
}

GFSErrorCode Client::toGFSErrorCode(ClientErrCode err) {
	switch (int(err)) { // todo more err code
		case (int)ClientErrCode::OK:
			return GFSErrorCode::OK;
		default:
			break;
	}
	return GFSErrorCode::Unknown;
}

GFSError Client::toGFSError(ClientErr err) {
	return GFSError(toGFSErrorCode(err.code), err.description);
}

std::tuple<ClientErr, std::vector<std::string>> Client::listFile(const std::string &dir) {
	if(!checkMasterAddress())
		return std::make_tuple(ClientErr(ClientErrCode::MasterNotFound), std::vector<std::string>());
	std::tuple<GFSError, std::vector<std::string>> errVecstring;
	try {
		errVecstring = srv.RPCCall({masterAdd, masterPort}, "ListFile", dir)
				.get().as<std::tuple<GFSError, std::vector<std::string>>>();
	} catch (...) {
		return std::make_tuple(ClientErr(ClientErrCode::Unknown), std::vector<std::string>());
	}
	GFSError gErr = std::get<0>(errVecstring);
	ClientErrCode err;
	switch ((int)gErr.errCode) { // todo write a function to do the conversion
		case (int)GFSErrorCode::OK:
			err = ClientErrCode::OK;
			break;
		case (int)GFSErrorCode::FileDirAlreadyExists:
			err = ClientErrCode::FileDirAlreadyExists;
			break;
		case (int)GFSErrorCode::NoSuchFileDir:
			err = ClientErrCode::NoSuchFileDir;
			break;
		default:
			err = ClientErrCode::Unknown;
			break;
	}
	return std::make_tuple(err, std::get<1>(errVecstring));
}

GFSError Client::Create(const std::string &dir) {
	return toGFSError(fileCreate(dir));
}

GFSError Client::Mkdir(const std::string &dir) {
	return toGFSError(fileMkdir(dir));
}

std::tuple<GFSError, std::vector<std::string>> Client::List(const std::string &dir) {
	auto errAns = listFile(dir);
	return std::make_tuple(toGFSError(std::get<0>(errAns)), std::get<1>(errAns));
}

std::tuple<ClientErr, ChunkHandle> Client::getChunkHandle(const std::string &dir, size_t idx) {
	if(!checkMasterAddress())
		return std::make_tuple(ClientErr(ClientErrCode::MasterNotFound), ChunkHandle());
	std::tuple<GFSError, ChunkHandle> errHandle;
	try {
		std::cerr << "RPCCall...";
		errHandle = srv.RPCCall({masterAdd, masterPort}, "GetChunkHandle", dir, idx)
				.get().as<std::tuple<GFSError, ChunkHandle>>();
		std::cerr << "END";
	} catch (...) {
		return std::make_tuple(ClientErr(ClientErrCode::Unknown), ChunkHandle());
	}
	if (std::get<0>(errHandle).errCode == GFSErrorCode::OK) // todo err
		return std::make_tuple(ClientErr(ClientErrCode::OK), std::get<1>(errHandle));
	return std::make_tuple(ClientErr(ClientErrCode::Unknown), ChunkHandle());
}

std::tuple<GFSError, ChunkHandle> Client::GetChunkHandle(const std::string &dir, std::size_t idx) {
	auto errAns = getChunkHandle(dir, idx);
	return std::make_tuple(toGFSError(std::get<0>(errAns)), std::get<1>(errAns));
}


std::tuple<ClientErr, std::uint64_t>
Client::readChunk(const ChunkHandle &handle, const std::uint64_t &offset, std::vector<char> &data) {
	std::function<void()> state, getAddresses, read, endFlow;
	bool running = true;

	GFSError gErr;
	std::string primary;
	std::vector<std::string> replicas;

	ClientErr err(ClientErrCode::Unknown);

	getAddresses = [&]()->void {
		try {
			std::tie(gErr, replicas)
					= srv.RPCCall({masterAdd, masterPort}, "GetReplicas", handle)
					.get().as<std::tuple<GFSError, std::vector<std::string> /*Locations*/>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		if (gErr.errCode == GFSErrorCode::NoSuchChunk) {
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK)
			throw;

		//Select one replica to read chunk data.
		primary = replicas[rand()%replicas.size()];

		state = read;
	};
	read = [&]()->void {
		std::string tmp;
		try {
		std::tie(gErr, tmp) =
				srv.RPCCall(LightDS::User::RPCAddress::from_string(primary), "ReadChunk", handle, offset, data.size()).get()
						.as<std::tuple<GFSError, std::string>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		data = std::vector<char>(tmp.begin(), tmp.end());
		static int repeatTime = 0;
		if (gErr.errCode != GFSErrorCode::OK) {
			err = ClientErrCode::Unknown;
			state = endFlow;
		}
		err = ClientErrCode::OK;
		state = endFlow;
	};
	endFlow = [&]()->void {running = false;};

	state = getAddresses;
	while (running) {
		state();
	}
	return std::make_tuple(err, data.size());
}
std::tuple<ClientErr, std::uint64_t /*offset*/>
Client::appendChunk(const ChunkHandle &handle, const std::vector<char> &data) {
	if (data.size() > CHUNK_SIZE / 4) {
		return std::make_tuple(ClientErr(ClientErrCode::Unknown), 0);
	}
	std::function<void()> state, getAddresses, pushData, applyChunk, endFlow;
	bool running = true;

	GFSError gErr;
	std::uint64_t expire, id, offset;
	std::string primary;
	std::vector<std::string> secondaries;

	ClientErr err(ClientErrCode::Unknown);

	getAddresses = [&]()->void {
		try {
		std::tie(gErr, primary, secondaries, expire)
				= srv.RPCCall({masterAdd, masterPort}, "GetPrimaryAndSecondaries", handle)
				.get().as<std::tuple<GFSError,
						std::string /*Primary Address*/,
						std::vector<std::string> /*Secondary Addresses*/,
						std::uint64_t /*Expire Timestamp*/>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		if (gErr.errCode == GFSErrorCode::NoSuchChunk) {
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK)
			throw;
		state = pushData;
	};
	pushData = [&]()->void {
		static int repeatTime = 0;
		id = rand();
		try {
		gErr = srv.RPCCall({primary, chunkPort}, "PushData", id, data)
				.get().as<GFSError>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK) {
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				state = pushData;
			} else {
				state = endFlow;
			}
		}
		const auto & addrs = secondaries;
		auto iter = addrs.begin();
		for (; iter != addrs.end(); ++iter) {
			try {
				gErr = srv.RPCCall({*iter, chunkPort}, "PushData", id, data).get().as<GFSError>();
			} catch (...) {
				gErr.errCode = GFSErrorCode::TransmissionErr;
				err = ClientErr(ClientErrCode::Unknown);
				state = endFlow;
				return;
			}
			if (gErr.errCode != GFSErrorCode::OK)
				break;
		}
		if (iter != addrs.end()) {
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				state = pushData;
			} else {
				state = endFlow;
			}
		}
		repeatTime = 0;
		state = applyChunk;
	};
	applyChunk = [&]()->void {
		try {
			std::tie(gErr, offset)
					= srv.RPCCall({primary, chunkPort}, "AppendChunk", handle, id, secondaries).get()
					.as<std::tuple<GFSError, std::uint64_t>>();
			offset -= data.size();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		static int repeatTime = 0;
		if (gErr.errCode != GFSErrorCode::OK) {
			err = ClientErrCode::Unknown;
			state = endFlow;
			return ;
		}
		err = ClientErrCode::OK;
		state = endFlow;
	};
	endFlow = [&]()->void {running = false;};

	state = getAddresses;
	while (running) {
		state();
	}
	return std::make_tuple(err, offset);
}

std::tuple<GFSError, std::uint64_t /*size*/>
Client::ReadChunk(const ChunkHandle &handle, const std::uint64_t &offset, std::vector<char> &data) {
	auto errAns = readChunk(handle, offset, data);
	return std::make_tuple(toGFSError(std::get<0>(errAns)), std::get<1>(errAns));
}

std::tuple<GFSError, std::uint64_t /*offset*/>
Client::AppendChunk(const ChunkHandle &handle, const std::vector<char> &data) {
	auto errAns = appendChunk(handle, data);
	return std::make_tuple(toGFSError(std::get<0>(errAns)), std::get<1>(errAns));
}

ClientErr Client::writeChunk(const ChunkHandle &handle, const std::uint64_t &offset, const std::vector<char> &data) {
	std::function<void()> state, getAddresses, pushData, applyChunk, endFlow;
	bool running = true;

	GFSError gErr;
	std::uint64_t id, expire, length;
	std::string primary;
	std::vector<std::string> secondaries;

	ClientErr err(ClientErrCode::Unknown);
	length = data.size();

	getAddresses = [&]()->void {
		std::cerr << "getAddress" << std::endl;
		try {
			std::tie(gErr, primary, secondaries, expire)
					= srv.RPCCall({masterAdd, masterPort}, "GetPrimaryAndSecondaries", handle)
					.get().as<std::tuple<GFSError,
							std::string /*Primary Address*/,
							std::vector<std::string> /*Secondary Addresses*/,
							std::uint64_t /*Expire Timestamp*/>>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		if (gErr.errCode == GFSErrorCode::NoSuchChunk) {
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK) {
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		state = pushData;
	};
	pushData = [&]()->void {
		std::cerr <<"pushData" << std::endl;
		static int repeatTime = 0;
		id = rand();
		try {
			gErr = srv.RPCCall({primary, chunkPort}, "PushData", id, data)
					.get().as<GFSError>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		if (gErr.errCode != GFSErrorCode::OK) {
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				state = pushData;
			} else {
				state = endFlow;
			}
		}
		const auto & addrs = secondaries;
		auto iter = addrs.begin();
		for (; iter != addrs.end(); ++iter) {
			try {
				gErr = srv.RPCCall({*iter, chunkPort}, "PushData", id, data).get().as<GFSError>();
			} catch (...) {
				gErr.errCode = GFSErrorCode::TransmissionErr;
				err = ClientErr(ClientErrCode::Unknown);
				state = endFlow;
				return;
			}
			if (gErr.errCode != GFSErrorCode::OK)
				break;
		}
		if (iter != addrs.end()) {
			err =  ClientErr(ClientErrCode::Unknown);
			if (repeatTime < 3) {
				++repeatTime;
				state = pushData;
			} else {
				state = endFlow;
			}
		}
		repeatTime = 0;
		state = applyChunk;
	};
	applyChunk = [&]()->void {
		try {
			gErr = srv.RPCCall({primary, chunkPort}, "WriteChunk", handle, id, offset, secondaries).get()
					.as<GFSError>();
		} catch (...) {
			gErr.errCode = GFSErrorCode::TransmissionErr;
			err = ClientErr(ClientErrCode::Unknown);
			state = endFlow;
			return;
		}
		static int repeatTime = 0;
		if (gErr.errCode != GFSErrorCode::OK) {
			err = ClientErrCode::Unknown;
			state = endFlow;
			return ;
		}
		err = ClientErrCode::OK;
		state = endFlow;
	};
	endFlow = [&]()->void {running = false;};

	state = getAddresses;
	while (running) {
		state();
	}
	return err;
}

GFSError Client::WriteChunk(const ChunkHandle &handle, const std::uint64_t &offset, const std::vector<char> &data) {
	return toGFSError(writeChunk(handle, offset, data));
}

GFSError Client::Write(const std::string &dir, std::uint64_t offset, const std::vector<char> &data)
{
	std::uint64_t fileLength,nowOffset=offset;
	ClientErr er;

	//Get file length
	fileLength = data.size();

	//Spilit file into pieces
	std::uint64_t nowPosition = 0, thisLength;
	while( nowPosition < fileLength )
	{
		thisLength = std::min( CHUNK_SIZE-nowOffset%CHUNK_SIZE, fileLength-nowPosition );
		std::cerr << "Write+ : " << nowPosition << " " << thisLength << " " << nowOffset << std::endl;

		er = fileWrite_str( dir, std::string( data.begin()+nowPosition, data.begin()+nowPosition+thisLength ), nowOffset );
		if( er.code != ClientErrCode::OK )
			break;

		nowPosition += thisLength;
		nowOffset += thisLength;
	}

	return toGFSError(er);
}

std::tuple<GFSError, std::uint64_t /*size*/>
Client::Read(const std::string &dir, std::uint64_t offset, std::vector<char> &data)
{
	std::uint64_t nowOffset=offset, fileLength;
	ClientErr er;
	std::string buffer,tmp;

	//Open file
	fileLength = data.size();

	//Spilit file into pieces
	std::uint64_t nowPosition = 0, thisLength;
	while( nowPosition < fileLength )
	{
		thisLength = std::min( CHUNK_SIZE-nowOffset%CHUNK_SIZE, fileLength-nowPosition );
		std::cerr << "Read+ : " << nowPosition << " " << thisLength << std::endl;
		std::cerr << "nowOffset = " << nowOffset << std::endl;
		er = fileRead_str( dir, tmp, nowOffset, thisLength );
		if( er.code != ClientErrCode::OK && er.code != ClientErrCode::ReadEOF )return std::make_tuple( toGFSError(er), 0 );
		buffer = buffer + tmp;
		std::cerr << "Real tmp length: " << tmp.size() << std::endl;
		if( er.code == ClientErrCode::ReadEOF )
		{
			std::cerr << "Read EOF! Real length: " << buffer.size() << std::endl;
			er.code = ClientErrCode::OK;
			//nowPosition += std::stoull(er.description);
			break;
		}

		nowPosition += thisLength;
		nowOffset += thisLength;

	}

	std::cerr << "Real buffer length: " << tmp.size() << std::endl;
	data = std::vector<char>( buffer.begin(), buffer.end() );
	return std::make_tuple( toGFSError(er), buffer.size() );
}

std::tuple<GFSError, std::uint64_t /*offset*/>
Client::Append(const std::string &dir, const std::vector<char> &data)
{
	std::uint64_t fileLength, offset;

	//Get file length
	fileLength = data.size();

	if( fileLength > CHUNK_SIZE/4 )
		return std::make_tuple( toGFSError({ ClientErrCode::WrongOperation, "Append-data is out of excepted length."}), 0 );

	GFSError er = toGFSError(fileAppend_str( dir, std::string(data.begin(), data.end()), offset));
	return std::make_tuple( er, offset-data.size() );
}

}
