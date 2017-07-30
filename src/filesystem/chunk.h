#ifndef AFS_CHUNK_H
#define AFS_CHUNK_H

#include <vector>
#include <common.h>


namespace AFS {

class ChunkPool;

struct Chunk
{
	typedef std::uint32_t ChunkLength;

	Chunk( ChunkPool *CP = NULL );
	Chunk( const Chunk &c );
	~Chunk();
	Chunk& operator = ( const Chunk &c );
	void setCP( ChunkPool *CP = NULL );

	//ChunkHandle handle;

	bool isPrimary;
	std::uint64_t expireTime;
	ChunkVersion version;
	ChunkLength length;
	char *data;
	ChunkPool *cp;
private:
	ReadWriteMutex lock;
};

}

#endif // CHUNK_H
