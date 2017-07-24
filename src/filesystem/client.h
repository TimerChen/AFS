#ifndef CLIENT_H
#define CLIENT_H

#include <string>

//#include "master.h"

namespace AFS {



class Master;

class Client
{
public:
	Client();

	void setMaster( Master *Address );

private:
	Master *masterAdd;

	void fileCreate	( const std::string &dir );
	void fileRead	( const std::string &dir, const std::string &localDir );
	void fileWrite	( const std::string &dir, const std::string &localDir );
	void fileAppend	( const std::string &dir, const std::string &localDir );
	void fileMkdir	( const std::string &dir );
};

}

#endif // CLIENT_H
