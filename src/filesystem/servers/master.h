//
// Created by Aaron Ren on 21/07/2017.
//

#ifndef AFS_MASTER_H
#define AFS_MASTER_H

#include "setting.h"
#include "data.h"
#include "server.h"
#include <map>

/// ACM File System!
namespace AFS {

/** Master
 *  · 主体即为MetadataContainer mdc以及
 *    chunk server相关的信息
 *  · 保存的数据包括路径到元数据的映射（mdc）
 *  · 元数据中除了一般意义的元数据，还包括对应
 *    chunk的信息等。
 */
class Master {

private:
    MetadataContainer        mdc;
    ChunkServerDataContainer csdc;
private:
    // createFolder

    // fileRead & fileWrite & append
    Metadata::ChunkData
    getFileChunk(const Path & path) const {
        if (mdc.checkData(path))
            throw NonexistentFile();
        return mdc.getData(path).chunk_data;
    }

public:

    void backgroundActivities();


};

}


#endif //AFS_MASTER_H
