#ifndef AFS_ERROR_HPP
#define AFS_ERROR_HPP

#include <exception>

namespace AFS
{

class AFSError : std::exception
{
public:
	//AFSError(){}
};

class ServerIllegalOperation : public AFSError
{

};
class DirNotFound : public AFSError
{

};


class ChunkServerError : public AFSError
{
public:
	ChunkServerError(){}
};


class OperationOverflow : public ChunkServerError
{

};

class ChunkPoolEmpty : public AFSError
{

};


//class ClientError : public AFSError
//{
//public:
//	ClientError(){}
//};
//
//class MasterNotFind : public ClientError
//{
//
//};

}

namespace AFS {

enum class ClientErrCode {
	OK = 0, Unknown,
	MasterNotFound, ServerNotFound,
	NoSuchFileDir, FileDirAlreadyExists,
	WrongOperation,
};

struct ClientErr {
	ClientErrCode code;
	std::string   description;

	ClientErr(ClientErrCode _code = ClientErrCode::Unknown, std::string _des = "")
			: code(_code), description(std::move(_des)) {}
};

}


#endif // ERROR_HPP
