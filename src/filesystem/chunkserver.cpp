#include "chunkserver.h"
namespace AFS {

/*ChunkServer::ChunkData::ChunkData()
{}
*/

ChunkServer::ChunkServer(LightDS::Service &Srv, const std::string &RootDir)
	:Server(Srv, RootDir)//srv(Srv), rootDir(RootDir)
{


}

ChunkServer::~ChunkServer()
{
	//Shutdown();
}

void ChunkServer::Start()
{

}
void ChunkServer::Shutdown()
{

}

}
