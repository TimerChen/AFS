#include "servera.h"

#include <functional>


ServerA::ServerA( LightDS::Service &Srv )
	:srv(Srv)
{
	srv.RPCBind<AFS::GFSError(std::string)>("Msg", std::bind(&ServerA::PRCHello, this, std::placeholders::_1));
}


AFS::GFSError ServerA::PRCHello( std::string words )
{
	//std::cerr << "Be called!" << std::endl;
	std::cout << srv.getRPCCaller() << ":" << words << std::endl;
	return {AFS::GFSErrorCode::OK, "OK!"};
}
void ServerA::sendMessage( std::string ip, std::uint16_t port, std::string words )
{
	//AFS::GFSError er =
	msgpack::object_handle er =
		srv.RPCCall( {ip,port}, "Msg", words );

	//std::cerr << er.get() << " " << er.get() << std::endl;
	AFS::GFSError reData;
	er.get().convert( reData );
	//std::cerr << reData.description << std::endl;
	//er.get().as<Type>();

}
