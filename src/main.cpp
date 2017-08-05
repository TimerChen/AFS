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
	LightDS::Service srv("TimerServer", logfile, ss, ss, port);

	ServerA sa(srv);
	std::thread srvThd(&LightDS::Service::Run, &srv);




	srv.Stop();
	ss << "{\"type\":\"bye\"}\n";

	srvThd.join();

	return 0;
}

/*
#include <gtest/gtest.h>

int main(int argc, char **argv) {
	//::testing::InitGoogleTest(&argc, argv);
	//return RUN_ALL_TESTS();
}
*/
