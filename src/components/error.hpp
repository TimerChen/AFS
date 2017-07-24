#ifndef AFS_ERROR_HPP
#define AFS_ERROR_HPP

namespace AFS
{

class Error
{
public:
	Error(){}
};

class ChunckError : public Error
{
public:
	ChunckError(){}
};


class ClientError : public Error
{
public:
	ClientError(){}
};

class MasterNotFind : public ClientError
{

};


}


#endif // ERROR_HPP
