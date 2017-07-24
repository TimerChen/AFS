#include <iostream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <vector>

using namespace std;

vector<string> vs;


int main(int argc, char *argv[])
{
	string s = "Hello,World deep,dark,fantasy,, fantasy";
	cout << s << endl;
	boost::split(vs,s,boost::is_any_of(", "),boost::token_compress_off);

	for( auto i : vs )
		cout << i << endl;
		
	return 0;
}
