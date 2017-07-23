#ifndef CHUNKSERVER_H
#define CHUNKSERVER_H

#include "server.h"
#include "chunk.h"

namespace AFS {

class ChunkServer : public Server
{

	class ChunkData
	{

	public:

		long long name, version;
		short end;
		char data[Chunk::CHUNKSIZE];

		ChunkData();

	};


public:
	ChunkServer();

};

}


#endif // CHUNKSERVER_H
