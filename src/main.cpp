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
		port = 12308;
	}else{
		//port = 12346;
		port = atoi(argv[1]);
	}

	ofstream logfile("test_log.txt",ios::out);
	stringstream ss;
	LightDS::Service srv("TimerServer", logfile, ss, ss, port);

	ServerA sa(srv);
	//srv.Run();
	std::string msg;

	//srv.LightDS::User::Run();
	std::thread srvThd(&LightDS::Service::Run, &srv);

	srv.Run();

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

		//if(...)
		//	break;

	}


	srv.Stop();
	ss << "{\"type\":\"bye\"}\n";

	srvThd.join();
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
