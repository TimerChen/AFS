#include <cstdio>
#include <iostream>
#include <fstream>

#include <tests/LightDSTest/servera.h>

using namespace std;
int main(int argc, char**argv)
{
	/*
	using namespace LightDS;
	std::stringstream ss, ss2;
	std::uint16_t port;
	if(argc == 1)
	{
		port = 10086;
	}else{
		port = 10087;
	}
	Service srv("test", ss, std::cin, std::cout, port);

	std::thread worker([&srv]()
	{
		srv.Run();
	});
	srv.RPCBind<int(int,int)>("Plus", [](int a, int b)
	{
		std::cerr << "Plus " << a << " + " << b << std::endl;
		return a + b;
	});
	srv.RPCBind<void(int)>("Print", [](int a)
	{
		std::cerr << "Print " << a << std::endl;
	});
	srv.RPCBind<int()>("Get", []()
	{
		std::cerr << "Get" << std::endl;
		return 10010;
	});
	srv.RPCBind<void(std::vector<int>)>("Vector", [](std::vector<int> vec)
	{
		std::cerr << "Vector of ";
		for (int x : vec)
			std::cerr << x << " ";
		std::cerr << std::endl;
	});
	if(port != 10086)
	{
		int ret = srv.RPCCall(Service::RPCAddress{ "127.0.0.1", 10086 }, "Plus", 233, 666).get().as<int>();
		std::cerr << "Result is " << ret << std::endl;
		ret = srv.RPCCall(Service::RPCAddress{ "127.0.0.1", 10086 }, "Plus", 100, -101).get().as<int>();
		std::cerr << "Result is " << ret << std::endl;
		srv.RPCCall(Service::RPCAddress{ "127.0.0.1", 10086 }, "Print", 10086);
		srv.RPCCall(Service::RPCAddress{ "127.0.0.1", 10086 }, "Print", 10010);
		ret = srv.RPCCall(Service::RPCAddress{ "127.0.0.1", 10086 }, "Get").get().as<int>();
		std::cerr << "Result is " << ret << std::endl;

		srv.RPCCall(Service::RPCAddress{ "127.0.0.1", 10086 }, "Vector", std::vector<int>{3, 1, 4, 1, 5, 9});
	}
	//srv.Stop();
	//srv.Stop();
	cerr << "End" << endl;
	worker.join();




	return 0;*/



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
