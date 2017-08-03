#ifndef AFS_COMMON_H
#define AFS_COMMON_H

#include <msgpack.hpp>
#include <error.hpp>

#include <boost/thread.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/mutex.hpp>

namespace AFS {

const std::uint32_t CHUNK_SIZE = 64*1024*1024;
const std::uint32_t CHUNKSERVER_CACHE_SIZE = 20;

typedef std::uint64_t ChunkHandle;
typedef std::uint64_t ChunkVersion;
typedef boost::shared_mutex ReadWriteMutex;
typedef boost::shared_lock<boost::shared_mutex> ReadLock;
typedef boost::unique_lock<boost::shared_mutex> WriteLock;

enum class GFSErrorCode : std::uint32_t
{
	OK = 0,
	InvalidOperation = 1,
	ChunkVersionExpired = 2,
	OperationOverflow = 3,
	OperateFailed = 4,
};


struct GFSError
{
	GFSErrorCode errCode;
	std::string description;

	MSGPACK_DEFINE(errCode, description);
};



}

MSGPACK_ADD_ENUM(AFS::GFSErrorCode);

#endif
