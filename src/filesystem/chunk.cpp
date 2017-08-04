#include "chunk.h"

#include <chunkpool.h>
#include <time.h>

namespace AFS
{
Chunk::Chunk()
	:version(0), length(0),
	 isPrimary(0), needExtensions(0),
	 expireTime(0), finished(0)
{

}
void Chunk::update( const ChunkVersion &newVersion)
{
	if( version != newVersion)
		finished = 0;
	version = newVersion;
	needExtensions = 0;
}
bool Chunk::primary()
{
	if(time(0) > expireTime)
		isPrimary = 0;
	return isPrimary;
}

/*
Chunk::Chunk( const Chunk &c )
{

}
Chunk& Chunk::operator = ( const Chunk &c )
{
	version = c.version;
	length = c.length;
	return *this;
}*/

Chunk::~Chunk()
{

}


}
