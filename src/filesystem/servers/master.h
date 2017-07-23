//
// Created by Aaron Ren on 21/07/2017.
//

#ifndef AFS_MASTER_H
#define AFS_MASTER_H

#include "setting.h"
#include "data.h"
#include "server.h"
#include "log.h"
#include <map>

/// ACM File System!
namespace AFS {

/** Master
 *  · 主体即为MetadataContainer mdc以及
 *    chunk server相关的信息
 *  · 保存的数据包括路径到元数据的映射（mdc）
 *  · 元数据中除了一般意义的元数据，还包括对应
 *    chunk的信息等。
 *  · Background Activities
 */
class Master {
private:
	MetadataContainer        mdc;
    ChunkServerDataContainer csdc;
	LogContainer             lc;

private:
    // createFolder

    // fileRead & fileWrite & append
//    ? getFileChunk(const Path & path) const;

	void collectGarbage();

	void detectExpiredChunk();

	void loadBalance();

	void checkpoint();

	/** reReplication:
	 *  · 检查当前的副本情况，如果有文件的（某个chunk的）
	 *    副本数不满足复制因子，则复制该chunk。
	 *  · Master会选择某个含有该chunk的chunk服务器，以及
	 *    新的chunk将处于的chunk服务器。命令前者传输一份chunk
	 *    到后者上。
	 *  · 此操作作为后台活动的一部分，同样是利用extrie的iterate
	 *    来完成的。
	 */
	void reReplication(Metadata & md) {
		if (md.type != Metadata::Type::file)
			return;

	}

public:

    void backgroundActivities();


};

}


#endif //AFS_MASTER_H
