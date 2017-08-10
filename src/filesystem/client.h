#ifndef AFS_CLIENT_H
#define AFS_CLIENT_H

#include <vector>
#include <tuple>
#include <string>
#include <service.hpp>
#include "common.h"
#include "setting.hpp"

namespace AFS {

class Client {
public:
	explicit Client(LightDS::User &_srv, const std::string &MasterAdd="", const uint16_t &MasterPort=7777, const uint16_t &ClientPort=7778);

	void setMaster(Address add, uint16_t port);

protected:

	bool checkMasterAddress() const;

public:
	//You can use this to interactive with you by console.
	void Run( std::istream &in=std::cin, std::ostream &out=std::cout );

	ClientErr fileCreate(const std::string &dir);
	ClientErr fileMkdir(const std::string &dir);


	ClientErr fileRead(const std::string &dir, const std::string &localDir, const std::uint64_t &offset, const std::uint64_t &fileLength);
	ClientErr fileRead_str(const std::string &dir, std::string &data, const std::uint64_t &offset, const std::uint64_t &length);

	std::tuple<ClientErr, std::int64_t /*actual size*/>
	fileRead(const ChunkHandle & handle, const std::uint64_t & offset, std::vector<char> & data);;

	ClientErr fileWrite(const std::string &dir, const std::string &localDir, const std::uint64_t &offset);
	ClientErr fileWrite_str(const std::string &dir, const std::string &data, const std::uint64_t &offset);

	ClientErr fileAppend(const std::string &dir, const std::string &localDir);
	ClientErr fileAppend_str(const std::string &dir, const std::string &data);

//	ClientErr fileRead(const ChunkHandle & handle, const std::uint64_t & offset, std::vector<char> & data);
//	ClientErr fileWrite(const ChunkHandle & handle, const std::uint64_t & offset, const std::vector<char> & data);
//	ClientErr fileAppend(const ChunkHandle & handle, const std::uint64_t & offset, const std::vector<char> & data);

	std::tuple<ClientErr, std::vector<std::string>> listFile(const std::string & dir);

	std::tuple<ClientErr, ChunkHandle> getChunkHandle(const std::string & dir, size_t idx);

	/// chunk
	ClientErr writeChunk(const ChunkHandle &handle, const std::uint64_t &offset, const std::vector<char> &data);

	std::tuple<ClientErr, std::uint64_t>
	readChunk(const ChunkHandle & handle, const std::uint64_t & offset, std::vector<char> & data);

	std::tuple<ClientErr, std::uint64_t /*offset*/>
	appendChunk(const ChunkHandle & handle, const std::vector<char> & data);

protected:
	std::string masterAdd;
	uint16_t masterPort, clientPort;
	LightDS::User &srv;
	char buffer[CHUNK_SIZE];

private:
	static GFSErrorCode toGFSErrorCode(ClientErrCode err);

	static GFSError toGFSError(ClientErr err);

public: // test
	// create file
	GFSError Create(const std::string & dir);

	GFSError Mkdir(const std::string & dir);

	std::tuple<GFSError, std::vector<std::string>> List(const std::string & dir);

	std::tuple<GFSError, ChunkHandle>
			GetChunkHandle(const std::string & dir, std::size_t idx);

	/// chunk
	GFSError WriteChunk(const ChunkHandle & handle, const std::uint64_t & offset, const std::vector<char> & data);

	std::tuple<GFSError, std::uint64_t /*size*/>
	ReadChunk(const ChunkHandle & handle, const std::uint64_t & offset, std::vector<char> & data);

	std::tuple<GFSError, std::uint64_t /*offset*/>
	AppendChunk(const ChunkHandle & handle, const std::vector<char> & data);

	GFSError Write(const std::string &path, std::uint64_t offset, const std::vector<char> &data);


	/// file
	std::tuple<GFSError, std::uint64_t /*size*/>
	Read(const std::string &path, std::uint64_t offset, std::vector<char> &data);

	std::tuple<GFSError, std::uint64_t /*offset*/>
			Append(const std::string &path, const std::vector<char> &data);
};

}

#endif // CLIENT_H
