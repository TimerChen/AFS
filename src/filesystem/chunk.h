#ifndef CHUNK_H
#define CHUNK_H

#include <vector>


namespace AFS {

class Chunk
{

public:

    static const int CHUNKSIZE = 64*1024;
    long long name, version;
	short end;
	std::vector<long long> ServerAdd;

    Chunk();

};

}

#endif // CHUNK_H
