#ifndef AFS_CHUNK_H
#define AFS_CHUNK_H

#include <vector>
#include <common.h>


namespace AFS {

class ChunkPool;

struct Chunk
{
	Chunk( ChunkPool *CP = NULL );
	Chunk( const Chunk &c );
	~Chunk();
	Chunk& operator = ( const Chunk &c );
	void setCP( ChunkPool *CP = NULL );

	ChunkHandle handle;
	ChunkVersion version;
	std::uint32_t length;
	char *data;
	ChunkPool *cp;
};

}

#endif // CHUNK_H
