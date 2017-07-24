#ifndef AFS_SERVER_H
#define AFS_SERVER_H

namespace AFS {

class Server
{

public:
	//Server();

protected:
	//Find recover files
	virtual void load()=0;
	//Save server files
	virtual void save()=0;

public:
	//Stop server
	virtual void stop()=0;
	//Restart server
	virtual void restart()=0;

};

}

#endif // SERVER_H
