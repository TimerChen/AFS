//
// Created by Aaron Ren on 22/07/2017.
//

#ifndef AFS_METADATA_H
#define AFS_METADATA_H

#include "extrie.hpp"
#include "setting.h"
#include <string>

namespace AFS {

class Metadata {
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

    Metadata get_data(const Path & path) const {
        return ext[path];
    }
};

}


#endif //AFS_METADATA_H
