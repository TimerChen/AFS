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

class ChunckserverError : public AFSError
{
public:
	ChunckserverError(){}
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
