#ifndef AFS_COMMON_H
#define AFS_COMMON_H

#include <msgpack.hpp>
#include <error.hpp>

namespace AFS {

typedef std::uint64_t ChunkHandle;
typedef std::uint64_t ChunkVersion;

enum class GFSErrorCode : std::uint32_t {
	OK = 0, Unknown,

	// err returned by master
	NoSuchFileDir, FileDirAlreadyExists,
	WrongOperation, // 对于文件夹进行文件操作等
	NoSuchChunk, PermissionDenied
};


struct GFSError
{
	GFSErrorCode errCode{GFSErrorCode::Unknown};
	std::string description;

	MSGPACK_DEFINE(errCode, description);

	GFSError() = default;
	GFSError(GFSErrorCode _errCode, std::string _des = "")
			: errCode(_errCode), description(std::move(_des)) {}
};

constexpr size_t ChunkNumPerServer = 20;
constexpr time_t LeaseExpiredTime = 60; // a lease will become expired after 60s

}

MSGPACK_ADD_ENUM(AFS::GFSErrorCode);

#endif
