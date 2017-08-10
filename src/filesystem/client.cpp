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

Client::Client(LightDS::User &_srv, const Address &MasterAdd, const uint16_t &MasterPort, const uint16_t &ClientPort)
	: srv(_srv), masterAdd(MasterAdd), masterPort(MasterPort),clientPort(ClientPort)
{
	if( masterAdd == "" )
	{
		auto addr = srv.ListService("master")[0];
		masterAdd = addr.ip;
		masterPort = addr.port;
	}
}


void Client::setMaster(Address add, uint16_t port) {
	masterAdd = std::move(add);
	masterPort = std::move(port);
	throw ;
	// TODO???
}
ClientErr Client::fileCreate(const std::string &dir) {
	if(!checkMasterAddress())
		return ClientErr(ClientErrCode::MasterNotFound);
	auto err = srv.RPCCall({masterAdd, clientPort}, "CreateFile", dir).get().as<GFSError>();
	if (err.errCode == GFSErrorCode::OK)
		return ClientErr(ClientErrCode::OK);

	// handle err
	switch ((int)err.errCode) {
		case (int)GFSErrorCode::FileDirAlreadyExists:
			return ClientErr(ClientErrCode::FileDirAlreadyExists);
		case (int)GFSErrorCode::NoSuchFileDir:
			return ClientErr(ClientErrCode::NoSuchFileDir);
		default:
			throw ;
	}
	return ClientErr(ClientErrCode::Unknown);
}

ClientErr Client::fileMkdir(const std::string &dir) {
	if(!checkMasterAddress())
		return ClientErr(ClientErrCode::MasterNotFound);
	auto err = srv.RPCCall({masterAdd, clientPort}, "Mkdir", dir).get().as<GFSError>();
	if (err.errCode == GFSErrorCode::OK)
		return ClientErr(ClientErrCode::OK);

	// handle err
	switch ((int)err.errCode) {
		case (int)GFSErrorCode::FileDirAlreadyExists:
			return ClientErr(ClientErrCode::FileDirAlreadyExists);
		case (int)GFSErrorCode::NoSuchFileDir:
			return ClientErr(ClientErrCode::NoSuchFileDir);
		default:
			throw ;
	}
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
ClientErr Client::fileAppend_str(const std::string &dir, const std::string &data) {
	GFSError gErr;
	std::uint64_t chunkIdx, handle, id, expire, offset;
	std::string primary;
	std::vector<std::string> secondaries;

	ClientErr err(ClientErrCode::Unknown);
	enum {
		GetFileInfo = 0, GetHandle, GetAddresses, PushData, ApplyChunk,
		EndFlow
	};
	int cur = GetFileInfo;

	auto getFileInfo = [&]()->void {
		bool isDir;
		std::uint64_t len;
		std::tie(gErr, isDir, len, chunkIdx)
				= srv.RPCCall({masterAdd, masterPort}, "GetFileInfo", dir).get()
				.as<std::tuple<GFSError,
						bool /*IsDir*/,
						std::uint64_t /*Length*/,
						std::uint64_t /*Chunks*/>>();
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
		std::tie(gErr, handle) =
				srv.RPCCall({masterAdd, masterPort}, "GetChunkHandle", dir,
				            chunkIdx).get().as<std::tuple<GFSError, ChunkHandle>>();
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
		std::tie(gErr, primary, secondaries, expire)
				= srv.RPCCall({masterAdd, masterPort}, "GetPrimaryAndSecondaries", handle)
				.get().as<std::tuple<GFSError,
						std::string /*Primary Address*/,
						std::vector<std::string> /*Secondary Addresses*/,
						std::uint64_t /*Expire Timestamp*/>>();
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
		static int repeatTime = 0;
		id = rand();
		gErr = srv.RPCCall({primary, masterPort}, "PushData", id, data)
				.get().as<GFSError>();
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
			gErr = srv.RPCCall({*iter, masterPort}, "PushData", id, data).get().as<GFSError>();
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
		std::tie(gErr, offset)
				= srv.RPCCall({primary, masterPort}, "AppendChunk", handle, id, secondaries).get()
				.as<std::tuple<GFSError, std::uint64_t>>();
		static int repeatTime = 0;
		if (gErr.errCode == GFSErrorCode::OperationOverflow) {
			++chunkIdx;
			cur = GetHandle;
			return;
		}
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
ClientErr Client::fileAppend(const std::string &dir, const std::string &localDir) {
	std::uint64_t fileLength;

	//Get file length
	std::fstream fin;
	fin.open(localDir, std::ios::in | std::ios::binary);
	fin.seekg( 0, std::ios_base::end );
	fileLength = fin.tellg();
	fin.seekg( 0, std::ios_base::beg );

	if( fileLength > CHUNK_SIZE/4 )
		return { ClientErrCode::WrongOperation, "Append-data is out of excepted length."};

	fin.read( buffer, fileLength );
	return fileAppend_str( dir, std::string(buffer, fileLength) );
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
		bool isDir;
		std::uint64_t len;
		std::tie(gErr, isDir, len, chunkIdx)
				= srv.RPCCall({masterAdd, masterPort}, "GetFileInfo", dir).get()
				.as<std::tuple<GFSError,
						bool /*IsDir*/,
						std::uint64_t /*Length*/,
						std::uint64_t /*Chunks*/>>();
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
		chunkIdx = offset/CHUNK_SIZE;

		std::tie(gErr, handle) =
				srv.RPCCall({masterAdd, masterPort}, "GetChunkHandle", dir,
							chunkIdx).get().as<std::tuple<GFSError, ChunkHandle>>();
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
		std::tie(gErr, primary, secondaries, expire)
				= srv.RPCCall({masterAdd, masterPort}, "GetPrimaryAndSecondaries", handle)
				.get().as<std::tuple<GFSError,
						std::string /*Primary Address*/,
						std::vector<std::string> /*Secondary Addresses*/,
						std::uint64_t /*Expire Timestamp*/>>();
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
		static int repeatTime = 0;
		id = rand();
		gErr = srv.RPCCall({primary, masterPort}, "PushData", id, data)
				.get().as<GFSError>();
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
			gErr = srv.RPCCall({*iter, masterPort}, "PushData", id, data).get().as<GFSError>();
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
		gErr = srv.RPCCall({primary, masterPort}, "WriteChunk", handle, id, offset, secondaries).get()
				.as<GFSError>();
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
		std::tie(gErr, isDir, len, chunkIdx)
				= srv.RPCCall({masterAdd, masterPort}, "GetFileInfo", dir).get()
				.as<std::tuple<GFSError,
						bool /*IsDir*/,
						std::uint64_t /*Length*/,
						std::uint64_t /*Chunks*/>>();
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

		std::tie(gErr, handle) =
				srv.RPCCall({masterAdd, masterPort}, "GetChunkHandle", dir,
							chunkIdx).get().as<std::tuple<GFSError, ChunkHandle>>();
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
		std::tie(gErr, replicas)
				= srv.RPCCall({masterAdd, masterPort}, "GetReplicas", handle)
				.get().as<std::tuple<GFSError, std::vector<std::string> /*Locations*/>>();
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
		std::tie(gErr, data) =
			srv.RPCCall({primary, masterPort}, "ReadChunk", handle, offset, length).get()
				.as<std::tuple<GFSError, std::string>>();
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
			//TODO...
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
	auto errVecstring = srv.RPCCall({masterAdd, masterPort}, "ListFile", dir)
			.get().as<std::tuple<GFSError, std::vector<std::string>>>();
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
	auto errHandle = srv.RPCCall({masterAdd, masterPort}, "GetChunkHandle", dir, idx)
			.get().as<std::tuple<GFSError, ChunkHandle>>();
	if (std::get<0>(errHandle).errCode == GFSErrorCode::OK) // todo err
		return std::make_tuple(ClientErr(ClientErrCode::OK), std::get<1>(errHandle));
	return std::make_tuple(ClientErr(ClientErrCode::Unknown), ChunkHandle());
}

std::tuple<GFSError, ChunkHandle> Client::GetChunkHandle(const std::string &dir, std::size_t idx) {
	auto errAns = getChunkHandle(dir, idx);
	return std::make_tuple(toGFSError(std::get<0>(errAns)), std::get<1>(errAns));
}


GFSError Client::Write(const std::string &path, std::uint64_t offset, const std::vector<char> &data)
{

}

std::tuple<GFSError, std::uint64_t /*size*/>
Client::Read(const std::string &path, std::uint64_t offset, std::vector<char> &data)
{

}

std::tuple<GFSError, std::uint64_t /*offset*/>
Client::Append(const std::string &path, const std::vector<char> &data)
{

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
		std::tie(gErr, replicas)
				= srv.RPCCall({masterAdd, masterPort}, "GetReplicas", handle)
				.get().as<std::tuple<GFSError, std::vector<std::string> /*Locations*/>>();
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
		std::tie(gErr, tmp) =
				srv.RPCCall({primary, masterPort}, "ReadChunk", handle, offset, data.size()).get()
						.as<std::tuple<GFSError, std::string>>();
		data = std::vector<char>(data.begin(), data.end());
		static int repeatTime = 0;
		if (gErr.errCode != GFSErrorCode::OK) {
			err = ClientErrCode::Unknown;
			state = endFlow;
		}
		err = ClientErrCode::OK;
		state = endFlow;
	};
	endFlow = [&]()->void {running = false;};


	while (running) {
		state();
	}
	return std::make_tuple(err, data.size());
}

std::tuple<ClientErr, std::uint64_t /*offset*/>
Client::appendChunk(const ChunkHandle &handle, const std::vector<char> &data) {
	std::function<void()> state, getAddresses, pushData, applyChunk, endFlow;
	bool running = true;

	GFSError gErr;
	std::uint64_t expire, id, offset;
	std::string primary;
	std::vector<std::string> secondaries;

	ClientErr err(ClientErrCode::Unknown);

	getAddresses = [&]()->void {
		std::tie(gErr, primary, secondaries, expire)
				= srv.RPCCall({masterAdd, masterPort}, "GetPrimaryAndSecondaries", handle)
				.get().as<std::tuple<GFSError,
						std::string /*Primary Address*/,
						std::vector<std::string> /*Secondary Addresses*/,
						std::uint64_t /*Expire Timestamp*/>>();
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
		gErr = srv.RPCCall({primary, masterPort}, "PushData", id, data)
				.get().as<GFSError>();
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
			gErr = srv.RPCCall({*iter, masterPort}, "PushData", id, data).get().as<GFSError>();
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
		std::tie(gErr, offset)
				= srv.RPCCall({primary, masterPort}, "AppendChunk", handle, id, secondaries).get()
				.as<std::tuple<GFSError, std::uint64_t>>();
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
		std::tie(gErr, primary, secondaries, expire)
				= srv.RPCCall({masterAdd, masterPort}, "GetPrimaryAndSecondaries", handle)
				.get().as<std::tuple<GFSError,
						std::string /*Primary Address*/,
						std::vector<std::string> /*Secondary Addresses*/,
						std::uint64_t /*Expire Timestamp*/>>();
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
		gErr = srv.RPCCall({primary, masterPort}, "PushData", id, data)
				.get().as<GFSError>();
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
			gErr = srv.RPCCall({*iter, masterPort}, "PushData", id, data).get().as<GFSError>();
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
		gErr = srv.RPCCall({primary, masterPort}, "WriteChunk", handle, id, offset, secondaries).get()
				.as<GFSError>();
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

	while (running) {
		state();
	}
	return err;
}

}
