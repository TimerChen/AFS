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


class ClientError : public AFSError
{
public:
	ClientError(){}
};

class MasterNotFind : public ClientError
{

};


}


#endif // ERROR_HPP
