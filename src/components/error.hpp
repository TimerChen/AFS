#ifndef ERROR_HPP
#define ERROR_HPP

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
