# AFS
A simplified Google File System of the PPCA lesson of ACM class, which called ACM File System.
## Remain to do

* Test all the code.

* (*)Try to unified the error type.

* (*)Review the code.(?)
## A simple sample to use LightDS
servera.h
```c++
#ifndef SERVERA_H
#define SERVERA_H

#include "service.hpp"
#include "log.hpp"

#include "common.h"


class ServerA
{
public:
	ServerA( LightDS::Service &srv );
	LightDS::Service &srv;
	void sendMessage( std::string ip, std::uint16_t port, std::string words );

protected:
	AFS::GFSError PRCHello( std::string words );
};

#endif // SERVERA_H

```
servera.cpp
```c++
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
	//er.get().convert( reData );
	reData = er.get().as<AFS::GFSError>();
	//std::cerr << reData.description << std::endl;

}

```
main.hpp
```c++
int main(int argc, char**argv)
{
	int i,j;
	std::uint16_t port, toPort;
	//cout << "Input your port:" ;
	//cin >> port;
	//cout << argc << endl;
	if(argc==1)
	{
		port = 12308;
	}else{
		//port = 12346;
		port = atoi(argv[1]);
	}

	ofstream logfile("test_log.txt",ios::out);
	stringstream ss;
	LightDS::Service srv("TimerServer", logfile, ss, ss, port);

	ServerA sa(srv);
	std::string msg;

	std::thread srvThd(&LightDS::Service::Run, &srv);

	std::string ip;
	cout << "Set your friend's IP and Port.\n";
	cin >> ip >> toPort;
	cout << "OK..\n";
	//getline(cin,msg);
	//cout << msg << " " << msg.size();

	while( 1 )
	{
		//cout << ">";
		while(1)
		{
			getline(cin,msg);
			if(msg.size() > 0)
				break;
		}
		sa.sendMessage( ip, toPort, msg );

		if(...)
			break;

	}


	srv.Stop();
	ss << "{\"type\":\"bye\"}\n";

	srvThd.join();
	logfile.close();

	return 0;
}
```
