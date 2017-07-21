//
// Created by Aaron Ren on 22/07/2017.
//

#ifndef AFS_METADATA_H
#define AFS_METADATA_H

#include "extrie.hpp"
#include "setting.h"
#include <string>
#include <map>

// metadata
namespace AFS {

struct Metadata {
    struct ChunkData {
        Handle        handle;
        ServerAddress address;
    } chunk_data;

    enum class Type {
        folder, file, system
    } type;
    // todo
};

class MetadataContainer {
private:
    extrie<PathType, Metadata> ext;

public:
    template <class Md>
    bool mkdir(const Path &path, Md && md) {
        return ext.insert(path, std::forward(md));
    }

    template <class Md>
    bool create(const Path & path, Md && md) {
        return ext.insert(path, std::forward(md));
    }

    bool checkData(const Path & path) const {
        return ext.check(path);
    }

    Metadata getData(const Path & path) const {
        return ext[path];
    }
};

}

// chunk server data
namespace AFS {

struct ChunkServerData {

};

class ChunkServerDataContainer {
private:
    std::map<ServerAddress, ChunkServerData> csd;
public:

};

}


#endif //AFS_METADATA_H
