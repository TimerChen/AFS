#include "chunkpool.h"
#include "error.hpp"
namespace AFS
{

ChunkPool::ChunkPool( std::uint32_t PoolSize, std::uint32_t DataSize )
	:dataSize(DataSize) ,poolSize(PoolSize)
{
	WriteLock cLock(lock);
	for(int i=0; i<(int)poolSize; ++i)
	{
		char *str = new char[dataSize];
		garbage.push( str );
	}
}

ChunkPool::~ChunkPool()
{
	WriteLock cLock(lock);
	for( auto itr : freshed )
	{
		garbage.push(itr.first);
	}

	while( !garbage.empty() )
	{
		char* p = garbage.front();
		garbage.pop();
		delete [] p;
	}
	cLock.unlock();
}
void ChunkPool::Delete(char* &data)
{
	WriteLock cLock(lock);
	auto i = freshed.find( data );
	if((--i->second)<=0)
	{
		garbage.push( i->first );
		freshed.erase( i );
	}
	data = NULL;
}
char* ChunkPool::newData()
{
	WriteLock cLock(lock);
	if( garbage.empty() )
		throw( ChunkPoolEmpty() );
	char *re = garbage.front();
	garbage.pop();
	++freshed[re];
	return re;
}
void ChunkPool::copy(char* data)
{
	WriteLock cLock(lock);
	++freshed[data];
}

bool ChunkPool::empty()
{
	//bool re = garbage.size() <= subPoolSize;
	ReadLock cLock(lock);
	return garbage.size() <= subPoolSize;
}
void ChunkPool::clear()
{
	WriteLock cLock(lock);
	for( auto itr : freshed )
	{
		garbage.push(itr.first);
	}
	freshed.clear();
	cLock.unlock();
}

}




