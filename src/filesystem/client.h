#ifndef AFS_CLIENT_H
#define AFS_CLIENT_H

#include <string>
#include <service.hpp>
#include "common.h"
#include "setting.hpp"

//#include "master.h"

namespace AFS {

class Client {
public:
	explicit Client(LightDS::Service &_srv, const Address &MasterAdd, const uint16_t &MasterPort=13208);

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

	ClientErr fileWrite(const std::string &dir, const std::string &localDir, const std::uint64_t &offset);
	ClientErr fileWrite_str(const std::string &dir, const std::string &data, const std::uint64_t &offset);

	ClientErr fileAppend(const std::string &dir, const std::string &localDir);
	ClientErr fileAppend_str(const std::string &dir, const std::string &data);
protected:
	Address masterAdd;
	uint16_t masterPort;
	LightDS::Service &srv;
	char buffer[CHUNK_SIZE];
};

}

#endif // CLIENT_H
