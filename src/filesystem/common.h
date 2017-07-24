#ifndef AFS_COMMON_H
#define AFS_COMMON_H

#include <msgpack.hpp>

namespace AFS {

typedef std::uint64_t ChunkHandle;
typedef std::uint64_t ChunkVersion;

enum class GFSErrorCode : std::uint32_t
{
	OK = 0,
};

struct GFSError
{
	GFSErrorCode errCode;
	std::string description;

//	MSGPACK_DEFINE(errCode, description);
};

}

#endif
