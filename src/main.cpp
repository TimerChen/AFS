#include <cstdio>
#include <iostream>
#include <fstream>

#include <tests/LightDSTest/servera.h>

using namespace std;
int main(int argc, char**argv)
{
	int i,j;
	std::uint16_t port, toPort;
	//cout << "Input your port:" ;
	//cin >> port;
	//cout << argc << endl;
	if(argc==1)
	{
		port = 12345;
	}else{
		port = 12346;
		//port = atoi(argv[1]);
	}

	ofstream logfile("test_log.txt",ios::out);
	LightDS::Service srv("TimerServer", logfile, cin, cout, port);

	ServerA sa(srv);
	//srv.Run();
	std::string msg;
	if(argc==1)
	{
		//srv.LightDS::User::Run();
		std::thread srvThd(&LightDS::Service::Run, &srv);
		//srvThd.detach();
		srvThd.join();
		//sa.sendMessage( 12346, "Hello" );

	}else{
		//srv.LightDS::User::Run();
		std::thread srvThd(&LightDS::Service::Run, &srv);
		srvThd.detach();
		sa.sendMessage( 12345, "Hello" );
		sa.sendMessage( 12345, "Hello" );
		while( 1 ){
			std::this_thread::yield();
			/*
			cout << "Input...:\n\t";
			cin >> toPort >> msg;
			cout << "Sending....\n";
			sa.sendMessage( toPort, msg );*/
		}
	}

	logfile.close();

	return 0;
}

/*
#include <gtest/gtest.h>

int main(int argc, char **argv) {
	//::testing::InitGoogleTest(&argc, argv);
	//return RUN_ALL_TESTS();
}
*/
