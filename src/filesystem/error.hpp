#ifndef ERROR_HPP
#define ERROR_HPP

namespace AFS
{

class Error
{
public:
	Error(){}
};

class ClientError : public Error
{
public:
	ClientError(){}
};

class ChunckError : public Error
{
public:
	ChunckError(){}
};

}


#endif // ERROR_HPP
