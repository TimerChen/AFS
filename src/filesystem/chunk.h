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
	void update( const ChunkVersion &newVersion);
	bool primary();
	ChunkVersion version;
	ChunkLength length;
	bool isPrimary;
	std::uint64_t expireTime;
	std::uint32_t finished;
};

}

#endif // CHUNK_H
