#include "chunkpool.h"
#include "error.hpp"
namespace AFS
{

ChunkPool::ChunkPool( std::uint32_t PoolSize, std::uint32_t DataSize )
	:dataSize(DataSize) ,poolSize(PoolSize)
{
	for(int i=0; i<(int)poolSize; ++i)
	{
		char *str = new char[dataSize];
		garbage.push( str );
	}
}

ChunkPool::~ChunkPool()
{
	lock.lock();
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
	lock.unlock();
}
void ChunkPool::Delete(char* &data)
{
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
	if( garbage.empty() )
		throw( ChunkPoolEmpty() );
	char *re = garbage.front();
	garbage.pop();
	return re;
}
void ChunkPool::copy(char* data)
{
	++freshed[data];
}

bool ChunkPool::empty()
{
	//bool re = garbage.size() <= subPoolSize;
	return garbage.size() <= subPoolSize;
}

}




