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

enum class GFSErrorCode : std::uint32_t {
	OK = 0, Unknown=1,
	//Error returned by ChunkServer
	InvalidOperation = 2,
	ChunkVersionExpired = 3,
	OperationOverflow = 4,
	OperateFailed = 5,
	//Error returned by Master
	NoSuchFileDir, FileDirAlreadyExists,
	WrongOperation, // 对于文件夹进行文件操作等
	NoSuchChunk, PermissionDenied,
	ChunkFull
};


struct GFSError
{
	GFSErrorCode errCode{GFSErrorCode::Unknown};
	std::string description;

	MSGPACK_DEFINE(errCode, description);

	GFSError() = default;
	GFSError(GFSErrorCode _errCode, std::string _des = "")
			: errCode(_errCode), description(std::move(_des)) {}

	friend std::ostream & operator<<(std::ostream & out, const GFSError & code) {
		out << (std::uint32_t)code.errCode;
		return out;
	}
	bool operator==(const GFSError & other) const {
		return errCode == other.errCode && description == other.description;
	}
};

constexpr size_t ChunkNumPerServer = 20;
constexpr time_t LeaseExpiredTime = 60; // a lease will become expired after 60s


}

MSGPACK_ADD_ENUM(AFS::GFSErrorCode);

#endif
