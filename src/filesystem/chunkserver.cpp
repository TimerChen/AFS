#include "chunkserver.h"

#include <fstream>
#include "boost/filesystem.hpp"
#include <ctime>

namespace AFS {

/*ChunkServer::ChunkData::ChunkData()
{}
*/




ChunkServer::ChunkServer(LightDS::Service &Srv, const std::string &RootDir)
	:Server(Srv, RootDir)
{
	MaxCacheSize = 20;

	bindFunctions();


}

ChunkServer::~ChunkServer()
{
	//Shutdown();
}

void ChunkServer::bindFunctions()
{
	//a Sample
	//srv.RPCBind<AFS::GFSError(std::string)>("Msg", std::bind(&ServerA::PRCHello, this, std::placeholders::_1));

	//RPCCreateChunk(ChunkHandle handle);
	srv.RPCBind<GFSError
			(ChunkHandle)>
			("CreateChunk", std::bind(&ChunkServer::RPCCreateChunk, this, std::placeholders::_1));

	//RPCReadChunk(ChunkHandle handle, std::uint64_t offset, std::uint64_t length);
	srv.RPCBind<std::tuple<GFSError, std::string /*Data*/>
			(ChunkHandle, std::uint64_t, std::uint64_t)>
			("ReadChunk", std::bind(&ChunkServer::RPCReadChunk, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

	//RPCWriteChunk(ChunkHandle handle, std::uint64_t dataID, std::uint64_t offset, std::vector<std::string> secondaries);
	srv.RPCBind<GFSError
			(ChunkHandle, std::uint64_t, std::uint64_t, std::vector<std::string>)>
			("WriteChunk", std::bind(&ChunkServer::RPCWriteChunk, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));

	//RPCAppendChunk(ChunkHandle handle, std::uint64_t dataID, std::vector<std::string> secondaries);
	srv.RPCBind<std::tuple<GFSError, std::uint64_t /*offset*/>
			(ChunkHandle, std::uint64_t, std::vector<std::string>)>
			("AppendChunk", std::bind(&ChunkServer::RPCAppendChunk, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

	//RPCApplyCopy(ChunkHandle handle, ChunkVersion version, std::string data, std::uint64_t serialNo);
	srv.RPCBind<GFSError
			(ChunkHandle, std::uint64_t, MutationType, std::uint64_t, std::uint64_t, std::uint64_t)>
			("ApplyMutation", std::bind(&ChunkServer::RPCApplyMutation, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5, std::placeholders::_6));

	//RPCSendCopy(ChunkHandle handle, std::string addr);
	srv.RPCBind<GFSError
			(ChunkHandle, std::string)>
			("SendCopy", std::bind(&ChunkServer::RPCSendCopy, this, std::placeholders::_1, std::placeholders::_2));

	//RPCApplyCopy(ChunkHandle handle, ChunkVersion version, std::string data, std::uint64_t serialNo);
	srv.RPCBind<GFSError
			(ChunkHandle, ChunkVersion, std::string, std::uint64_t)>
			("ApplyCopy", std::bind(&ChunkServer::RPCApplyCopy, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));

	//RPCGrantLease(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>>)
	srv.RPCBind<GFSError
			(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/> > ) >
			("GrantLease", std::bind(&ChunkServer::RPCGrantLease, this, std::placeholders::_1));


	//RPCUpdateVersion(ChunkHandle handle, ChunkVersion newVersion);
	srv.RPCBind<GFSError
			(ChunkHandle, ChunkVersion)>
			("UpdateVersion", std::bind(&ChunkServer::RPCUpdateVersion, this, std::placeholders::_1, std::placeholders::_2));

	//RPCPushData(std::uint64_t dataID, std::string data);
	srv.RPCBind<GFSError
			(std::uint64_t, std::string)>
			("PushData", std::bind(&ChunkServer::RPCPushData, this, std::placeholders::_1, std::placeholders::_2));
}

void ChunkServer::Start()
{
	Server::Start();

	load();
}
void ChunkServer::load()
{
	chunks.clear();
	loadSettings();
	loadFiles( rootDir );
}
void ChunkServer::loadSettings()
{
	std::ifstream fin((rootDir / ".setting").native());
	//Load master ip
	fin >> masterIP >> masterPort;
	fin.close();
}

void ChunkServer::loadFiles( const boost::filesystem::path &Path )
{
	using namespace boost::filesystem;
	std::string fname;
	if( !exists( Path ) || !is_directory( Path ) )
		throw( 0 );
	directory_iterator end_itr;
	for ( directory_iterator itr( Path );
			itr != end_itr;
			++itr ) {

		if ( is_directory(itr->status()) )
		{
			//loadFiles( itr->path() );
			//TODO

			//ignore..?
		} else {
			fname = itr->path().filename().native();
			//std::cerr << itr->path().filename().c_str() << endl;
			std::cerr << itr->path().native() << std::endl;
			//continue;
			if( fname.size() > 4 && fname.substr( fname.size()-4, 4 ) == ".chk" )
			{
				fname = fname.substr(0, fname.size()-4);
				std::cerr << "Find file: " << fname << ".chk" << std::endl;
				ChunkHandle handle = std::stoull(fname,0,10);
				/*
				lock_ss.lock();
				ss.clear();ss.str("");
				ss << fname;ss >> handle;
				lock_ss.unlock();*/

				Chunk c = loadChunkInfo( handle );

				lock_chunks.lock();
				chunks[handle] = c;
				lock_chunks.unlock();
			}else{
				//TODO

				//ignore..?
			}
		}

	}
}
Chunk ChunkServer::loadChunkInfo( const ChunkHandle &handle )
{

	Chunk c;
	char buffer[64];
	std::fstream fin;

	//Open file
	fin.open( (rootDir / std::to_string(handle)).native(), std::ios::in | std::ios::binary );
	if( !fin.is_open() )
	{
		throw( DirNotFound() );
	}
	lock_chunkMutex.lock();
	ReadLock cLock(chunkMutex[handle]);
	lock_chunkMutex.unlock();

	//Read
	fin.read( buffer, sizeof(c.version) );
	c.version = *(ChunkVersion*)(buffer);
	fin.read( buffer, sizeof(c.length) );
	c.length = *(Chunk::ChunkLength*)(buffer);

	//End
	fin.close();
	cLock.unlock();

	return c;
}

std::string ChunkServer::loadChunkData( const ChunkHandle &handle, const std::uint64_t &offset, const std::uint64_t &length )
{
	std::fstream fin;

	//Open file
	fin.open( (rootDir / std::to_string(handle)).native(), std::ios::in | std::ios::binary );
	if( !fin.is_open() )
	{
		throw( DirNotFound() );
	}
	lock_chunkMutex.lock();
	ReadLock cLock(chunkMutex[handle]);
	lock_chunkMutex.unlock();
	char *buffer = chunkPool.newData();


	//Read
	fin.seekg( sizeof(ChunkVersion)+sizeof(Chunk::ChunkLength)+offset );
	fin.read( buffer, length );

	//End
	fin.close();
	cLock.unlock();
	chunkPool.Delete( buffer );

	return std::string(buffer,length);
}

void ChunkServer::save()
{

}

void ChunkServer::saveChunkInfo( const ChunkHandle &handle, const Chunk &c )
{
	std::string fname = std::to_string(handle) + ".chk";
	std::fstream fout;
	char buffer[64];

	//Open file
	fout.open( fname.c_str(), std::ios::out | std::ios::binary );
		//if( !fout.is_open() )
	lock_chunkMutex.lock();
	WriteLock cLock(chunkMutex[handle]);
	lock_chunkMutex.unlock();

	//Write
	*(ChunkVersion*)(buffer) = c.version;
	fout.write( buffer, sizeof(c.version) );
	*(Chunk::ChunkLength*)(buffer) = c.length;
	fout.write( buffer, sizeof(c.length) );

	//End
	fout.close();
	cLock.unlock();
}
void ChunkServer::saveChunkData( const ChunkHandle &handle, char *data, const std::uint64_t &offset, const std::uint64_t &length )
{
	std::string fname = std::to_string(handle) + ".chk";
	std::fstream fout;

	//Open file
	fout.open( fname.c_str(), std::ios::out | std::ios::binary );
	lock_chunkMutex.lock();
	WriteLock cLock(chunkMutex[handle]);
	lock_chunkMutex.unlock();

	//Read
	fout.seekp( sizeof(ChunkVersion) + sizeof(Chunk::ChunkLength) + offset );
	fout.write( data, length );

	//End
	fout.close();
	cLock.unlock();
}

void ChunkServer::Shutdown()
{


	//At End
	Server::Shutdown();
}

void ChunkServer::Heartbeat()
{

}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
GFSError
	ChunkServer::RPCCreateChunk(ChunkHandle handle)
{
	Chunk c;

	//Save chunks info in chunks.
	c.version = 0;
	c.length = 0;
	lock_chunks.lock();
	chunks[handle] = c;
	lock_chunks.unlock();

	//Create a file
	saveChunkInfo( handle, c );
	char *buffer = chunkPool.newData();
	saveChunkData( handle, buffer );
	chunkPool.Delete( buffer );

}

// RPCReadChunk is called by client, read chunk data and return
std::tuple<GFSError, std::string /*Data*/>
	ChunkServer::RPCReadChunk(ChunkHandle handle, std::uint64_t offset, std::uint64_t length)
{
	std::tuple<GFSError, std::string /*Data*/> reData;
	Chunk c = loadChunkInfo( handle );
	if(offset + length > c.length)
	{
		reData = std::make_tuple( GFSError({GFSErrorCode::InvalidOperation, "ReadOverflow"}), "" );
	}else{
		reData = std::make_tuple( GFSError({GFSErrorCode::OK, ""}), loadChunkData( handle, offset, length ) );
	}
	return reData;
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
GFSError
	ChunkServer::RPCWriteChunk(ChunkHandle handle, std::uint64_t dataID, std::uint64_t offset, std::vector<std::string> secondaries)
{
	GFSError reData = {GFSErrorCode::OK, ""};
	//Check if it holds the lease.
	lock_chunks.lock();
	if(chunks[handle].isPrimary)
	{
		reData = GFSError({GFSErrorCode::OK, ""});
	}
	else
	{
		reData = GFSError({GFSErrorCode::InvalidOperation, "Not primary"});
	}
	lock_chunks.unlock();
	Chunk c = loadChunkInfo( handle );
	if(offset + length > c.length)
	{
		reData = GFSError({GFSErrorCode::InvalidOperation, "ReadOverflow"});
	}else{
		//TODO...
		reData = GFSError({GFSErrorCode::OK, ""});
	}
	return reData;

}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
std::tuple<GFSError, std::uint64_t /*offset*/>
	ChunkServer::RPCAppendChunk(ChunkHandle handle, std::uint64_t dataID, std::vector<std::string> secondaries)
{

}

// RPCApplyMutation is called by primary to apply mutations
GFSError
	ChunkServer::RPCApplyMutation(ChunkHandle handle, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset, std::uint64_t length)
{

}

// RPCSendCopy is called by master, send the whole copy to given address
GFSError
	ChunkServer::RPCSendCopy(ChunkHandle handle, std::string addr)
{

}

// RPCApplyCopy is called by another replica
// rewrite the local version to given copy data
GFSError
	ChunkServer::RPCApplyCopy(ChunkHandle handle, ChunkVersion version, std::string data, std::uint64_t serialNo)
{

}

// RPCGrantLease is called by master
// mark the chunkserver as primary
GFSError
	ChunkServer::RPCGrantLease(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>> arg )
{
	GFSError reData={GFSErrorCode::OK, ""};
	ChunkHandle handle;
	ChunkVersion newVersion;
	std::uint64_t expireTime;
	Chunk c;


	for( auto ii : arg )
	{
		std::tie(handle, newVersion, expireTime) = ii;

		lock_chunks.lock();
		auto itr = chunks.find(handle);
		if( itr == chunks.end() )
		{
			reData.errCode = GFSErrorCode::InvalidOperation;
			reData.description = reData.description
								 + to_string( handle ) + " "
								 + to_string(GFSErrorCode::OperationOverflow) + "\n";
		}
		else if( itr->second.version == newVersion-1 )
		{
			//Write Infomation
			itr->second.version = newVersion;
			saveChunkInfo( handle, itr->second );
		}
		else if( itr->second.version < newVersion )
		{
			reData.errCode = GFSErrorCode::InvalidOperation;
			reData.description = reData.description
								 + to_string( handle ) + " "
								 + to_string(GFSErrorCode::ChunkVersionExpired) + "\n";
		}
		else // itr->version > newVersion
		{
			reData.errCode = GFSErrorCode::InvalidOperation;
			reData.description = reData.description
								 + to_string( handle ) + " "
								 + to_string(GFSErrorCode::InvalidOperation) + "\n";
		}
		lock_chunks.unlock();
	}

	return reData;
}

// RPCUpdateVersion is called by master
// update the given chunks' version to 'newVersion'
GFSError
	ChunkServer::RPCUpdateVersion(ChunkHandle handle, ChunkVersion newVersion)
{
	GFSError reData;
	Chunk c;

	lock_chunks.lock();
	auto itr = chunks.find(handle);
	itr->second.version = newVersion;
	c = itr->second;
	if( c.version == newVersion-1 )
	{
		reData = {GFSErrorCode::OK, ""};
	}
	else if( c.version < newVersion )
	{
		reData = {GFSErrorCode::ChunkVersionExpired, ""};
	}
	else // itr->version > newVersion
	{
		reData = {GFSErrorCode::InvalidOperation, ""};
	}

	lock_chunks.unlock();

	if( reData.errCode == GFSErrorCode::OK )
		saveChunkInfo( handle, c );

	return reData;
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer.
// This should be replaced by a chain forwarding.
GFSError
	ChunkServer::RPCPushData(std::uint64_t dataID, std::string data)
{
	char *cdata = chunkPool.newData();

	strncpy( cdata, data.c_str(), data.size() );
	lock_dataCache.lock();
	dataCache[dataID] = cdata;
	lock_dataCache.unlock();

	lock_cacheQueue.lock();
	cacheQueue.push( dataID );
	lock_cacheQueue.unlock();

	//when chunkPool.empty() == true it is using the sub-pool of it.
	lock_dataCache.lock();
	lock_cacheQueue.lock();
	while( cacheQueue.size() > MaxCacheSize || chunkPool.empty() )
	{
		dataID = cacheQueue.front();
		auto idc = dataCache.find( dataID );
		chunkPool.Delete( idc->second );
		dataCache.erase( idc );
		cacheQueue.pop();
	}
	lock_cacheQueue.unlock();
	lock_dataCache.unlock();

	return { GFSErrorCode::OK, "" };
}


}
