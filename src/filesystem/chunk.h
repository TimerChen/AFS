#ifndef CHUNK_H
#define CHUNK_H

namespace AFS {

class Chunk
{

public:

    static const int CHUNKSIZE = 64*1024;
    long long name, version;
    short end;
    char data[CHUNKSIZE];

    Chunk();

};

}

#endif // CHUNK_H
