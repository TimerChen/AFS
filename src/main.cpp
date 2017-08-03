#include <iostream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <vector>
#include <fstream>

#include "tests/LightDSTest/servera.h"
#include "boost/filesystem.hpp"
#include "service.hpp"

using namespace std;

vector<string> vs;


void getMsg( LightDS::Service &srv )
{

}

int main(int argc, char *argv[])
{
	//using namespace ;

	return 0;

	ofstream logfile("test_log.txt",ios::out);

	int port, toPort;
	cout << "Your Port:";
	cin >> port;
	//port = 12345;
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
	//std::thread recive = std::thread( getMsg, srv );

	logfile.close();
	std::cout << "Closed" << std::endl;
	return 0;
}
