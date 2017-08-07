#include <cstdio>
#include <iostream>
#include <fstream>

#include <tests/LightDSTest/servera.h>

#include <master.h>
#include <client.h>
#include <chunkserver.h>

using namespace std;
int main(int argc, char**argv)
{

	int i,j;
	std::uint16_t port, toPort;
	if(argc==1)
	{
		port = 12308;
	}else{
		port = atoi(argv[1]);
	}

	ofstream logfile("test_log.txt",ios::out);
	stringstream ss;
	LightDS::Service srv("AFSServer", logfile, ss, ss, port);

	ServerA sa(srv);
	std::thread srvThd(&LightDS::Service::Run, &srv);

#ifdef IS_MASTER
	//master
	AFS::Master master(srv,".");
#endif
#ifdef IS_CHUNKSERVER
	//chunkserver
	AFS::ChunkServer chunkServer(srv,".");
#endif
#ifdef IS_CLIENT
	//client
	std::string masterIP = "0.0.0.0";
	std::uint16_t masterPort = 12308;
	AFS::Client client(srv, masterIP, masterPort);
	client.Run();
#endif

	srv.Stop();
	ss << "{\"type\":\"bye\"}\n";

	srvThd.join();

	return 0;
}

/*
#include <gtest/gtest.h>

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}*/