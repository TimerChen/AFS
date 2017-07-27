#ifndef AFS_SERVER_HPP
#define AFS_SERVER_HPP

#include <service.hpp>
#include "common.h"

namespace AFS {

class Server
{

public:
	Server( LightDS::Service &Srv, std::string RootDir )
		: srv(Srv), rootDir(RootDir), running(0)
	{ }
	virtual ~Server()
	{
		if(running)
			this->Shutdown();
	}

protected:
	//Find recover files
	virtual void load()=0;
	//Save server files
	virtual void save()=0;

public:
	void Restart()
	{
		if(running)
			this->Shutdown();
		this->Start();
	}

	//Your Start() should use this one at first.
	virtual void Start()
	{
		if(!running)
			throw( AFS::ServerIllegalOperation() );
		running = true;
	}

	//Your Shutdown() should use this one at end.
	virtual void Shutdown()
	{
		running = false;
	}

protected:
	LightDS::Service &srv;
	std::string rootDir;
	bool running;
};

}

#endif // SERVER_H
