# AFS
A simplified Google File System of the PPCA lesson of ACM class, which called ACM File System.

## How to use LightDS

Servera.cpp
```c++
ServerA::ServerA( LightDS::Service &Srv )
	:srv(Srv)
{
	srv.RPCBind<AFS::GFSError(std::string)>
	("Msg", std::bind(&ServerA::PRCHello, this, std::placeholders::_1));
}


AFS::GFSError ServerA::PRCHello( std::string words )
{
	std::cout << srv.getRPCCaller() << ":\n\t" << words << std::endl;
	return {AFS::GFSErrorCode::OK, "OK!"};
}
void ServerA::sendMessage( short port, std::string words )
{
	//AFS::GFSError er =
	msgpack::object_handle er =
		srv.RPCCall( {0,static_cast<uint16_t>(port)}, "Msg", words );
	std::cerr << er.get() << std::endl;
	//er.get().as<Type>();

}

```
main.hpp
```c++
ofstream logfile("test_log.txt",ios::out);
LightDS::Service srv("TimerServer", logfile, cin, cout, port);
srv.Run();
ServerA sa(srv);
std::string msg;
while( 1 ){
	cout << "Input...:\n\t";
	cin >> toPort >> msg;
	cout << "Sending....\n";
	sa.sendMessage( toPort, msg );
}
logfile.close();
```
