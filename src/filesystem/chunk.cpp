#include "chunk.h"

#include <chunkpool.h>

namespace AFS
{
Chunk::Chunk()
	:version(0), length(0),
	 isPrimary(0), expireTime(0)
{

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
