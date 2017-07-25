#include "servera.h"

ServerA::ServerA( LightDS::Service &Srv )
	:srv(Srv)
{

}


AFS::GFSError ServerA::PRCHello( std::string words )
{
	std::cout << words << std::endl;
	return {AFS::GFSErrorCode::OK, "OK!"};
}
