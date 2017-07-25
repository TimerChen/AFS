#include <iostream>
<<<<<<< HEAD
#include <string>
#include <boost/algorithm/string.hpp>
#include <vector>
#include <fstream>

#include "tests/LightDSTest/servera.h"
#include "service.hpp"

using namespace std;

vector<string> vs;


int main(int argc, char *argv[])
{
	ofstream logfile("test_log.txt",ios::out);
	LightDS::Service srv("TimerServer", logfile);
	srv.Run();
	ServerA sa(srv);
	logfile.close();
	std::cout << "Closed" << std::endl;
	return 0;
}
