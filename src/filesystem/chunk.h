#ifndef AFS_CHUNK_H
#define AFS_CHUNK_H

#include <vector>
#include <common.h>


namespace AFS {

class ChunkPool;

struct Chunk
{
	typedef std::uint32_t ChunkLength;

	Chunk( );
	//Chunk( const Chunk &c );
	~Chunk();

	void setPrimary();

	ChunkVersion version;
	ChunkLength length;
private:
	bool isPrimary;
	std::uint64_t expireTime;
};

}

#endif // CHUNK_H
