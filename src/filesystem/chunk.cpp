#include "chunk.h"

#include <chunkpool.h>

namespace AFS
{
Chunk::Chunk( ChunkPool *CP )
	:version(0), length(0), cp(CP)
{
	if(cp!=NULL)
		data = cp->newData();
	else
		data = NULL;
}
Chunk::Chunk( const Chunk &c )
	:version(c.version),
	 length(c.length), data(c.data)
{
	if(cp && data)
		cp->copy(c.data);
}
Chunk& Chunk::operator = ( const Chunk &c )
{
	if(cp && data)
		cp->Delete( data );
	version = c.version;
	length = c.length;
	data = c.data;
	cp = c.cp;
	if(cp && data)
		cp->copy(c.data);
	return *this;
}

Chunk::~Chunk()
{
	if(cp && data)
		cp->Delete( data );
}

void Chunk::setCP( ChunkPool *CP )
{
	if(cp && data)
		cp->Delete( data );
	cp = CP;
	data = cp->newData();
}

}
