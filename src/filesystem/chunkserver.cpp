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

				Chunk c = loadChunk( handle );

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
Chunk ChunkServer::loadChunk( const ChunkHandle &handle )
{
	Chunk c;
	char buffer[64];
	//c.handle = Handle;
	std::fstream fin;
	fin.open( (rootDir / std::to_string(handle)).native(), std::ios::in | std::ios::binary );
	fin.read( buffer, sizeof(c.version) );
	c.version = *(ChunkVersion*)(buffer);
	fin.read( buffer, sizeof(c.length) );
	c.length = *(Chunk::ChunkLength*)(buffer);
	/*
	if(needDetail)
	{
		c.setCP( &chunkPool );
		fin.read( c.data, CHUNK_SIZE );
	}
	*/
	fin.close();
	/*
	std::cerr << c.handle << " "
				<< c.version << " "
				<< c.length << " "
				<< c.data << std::endl;
	*/
	return c;
}

void ChunkServer::save()
{

}

void ChunkServer::saveChunk( const ChunkHandle &handle, const Chunk &c )
{
	std::string fname = std::to_string(handle) + ".chk";
	std::fstream fout;
	fout.open( fname.c_str(), std::ios::out | std::ios::binary );
	char buffer[64];
	*(ChunkVersion*)(buffer) = c.version;
	fout.write( buffer, sizeof(c.version) );
	*(Chunk::ChunkLength*)(buffer) = c.length;
	fout.write( buffer, sizeof(c.length) );
	//strncpy( buffer, c.data.c_str(), CHUNK_SIZE );
	fout.write( c.data, CHUNK_SIZE );
	fout.close();
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
	//c.handle = handle;
	c.version = 0;
	c.length = 0;

	lock_chunks.lock();
	chunks[handle] = c;
	lock_chunks.unlock();
	//TODO Create a file?
}

// RPCReadChunk is called by client, read chunk data and return
std::tuple<GFSError, std::string /*Data*/>
	ChunkServer::RPCReadChunk(ChunkHandle handle, std::uint64_t offset, std::uint64_t length)
{
	std::tuple<GFSError, std::string /*Data*/> reData;
	std::fstream fin;
	Chunk::ChunkLength maxLength;
	chunkPool.lock.lock();
	char *buffer = chunkPool.newData();
	chunkPool.lock.unlock();

	fin.open( (rootDir / std::to_string(handle)).native(), std::ios::in | std::ios::binary );
	fin.seekg( sizeof(ChunkVersion) );
	fin.read( buffer, sizeof(Chunk::ChunkLength) );
	maxLength = *((Chunk::ChunkLength*)buffer);
	if(offset + length > maxLength)
	{
		reData = std::make_tuple( GFSError({GFSErrorCode::InvalidOperation, "ReadOverflow"}), "" );
	}else{
		fin.seekg( sizeof(ChunkVersion)+sizeof(Chunk::ChunkLength)+offset );
		fin.read( buffer, length );
		reData = std::make_tuple( GFSError({GFSErrorCode::OK, ""}), std::string(buffer,length) );
	}
	fin.close();
	chunkPool.lock.lock();
	chunkPool.Delete( buffer );
	chunkPool.lock.unlock();
	return reData;
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
GFSError
	ChunkServer::RPCWriteChunk(ChunkHandle handle, std::uint64_t dataID, std::uint64_t offset, std::vector<std::string> secondaries)
{


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
	ChunkServer::RPCGrantLease(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>>)
{

}

// RPCUpdateVersion is called by master
// update the given chunks' version to 'newVersion'
GFSError
	ChunkServer::RPCUpdateVersion(ChunkHandle handle, ChunkVersion newVersion)
{

}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer.
// This should be replaced by a chain forwarding.
GFSError
	ChunkServer::RPCPushData(std::uint64_t dataID, std::string data)
{
	//Chunk c( &chunkPool );
	chunkPool.lock.lock();
	char *cdata = chunkPool.newData();
	chunkPool.lock.unlock();

	strncpy( cdata, data.c_str(), data.size() );
	lock_dataCache.lock();
	dataCache[dataID] = cdata;
	lock_dataCache.unlock();

	lock_cacheQueue.lock();
	cacheQueue.push( dataID );
	lock_cacheQueue.unlock();

	//when chunkPool.empty() == true it is using the sub-pool of it.
	chunkPool.lock.lock();
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
	chunkPool.lock.unlock();
}


}
