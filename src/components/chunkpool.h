#ifndef AFS_CHUNKPOOL_H
#define AFS_CHUNKPOOL_H

#include <common.h>
#include <queue>

namespace AFS {


class ChunkPool
{
public:
	ChunkPool( std::uint32_t PoolSize = CHUNKSERVER_CACHE_SIZE, std::uint32_t DataSize = CHUNK_SIZE );
	~ChunkPool();
	void	Delete( char* &data );
	char*	newData();
	void	copy( char* data );
	bool	empty();
private:
	static const std::uint32_t		subPoolSize = 3;
	std::uint32_t					dataSize;
	std::uint32_t					poolSize;
	std::map<char*, std::uint32_t>	freshed;
	std::queue<char*>				garbage;

};

}


#endif // CHUNKPOOL_H
