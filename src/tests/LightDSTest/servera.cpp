#include "servera.h"

#include <functional>


ServerA::ServerA( LightDS::Service &Srv )
	:srv(Srv)
{
	srv.RPCBind<AFS::GFSError(std::string)>("Msg", std::bind(&ServerA::PRCHello, this, std::placeholders::_1));
}


AFS::GFSError ServerA::PRCHello( std::string words )
{
	std::cerr << "Be called!" << std::endl;
	std::cout << srv.getRPCCaller() << ":\n\t" << words << std::endl;
	return {AFS::GFSErrorCode::OK, "OK! The same hello to you!"};
}
void ServerA::sendMessage( short port, std::string words )
{
	//AFS::GFSError er =
	msgpack::object_handle er =
		srv.RPCCall( {"192.168.1.105",static_cast<uint16_t>(port)}, "Msg", words );

	//std::cerr << er.get() << " " << er.get() << std::endl;
	AFS::GFSError reData;
	er.get().convert( reData );
	std::cerr << reData.description << std::endl;
	//er.get().as<Type>();

}
