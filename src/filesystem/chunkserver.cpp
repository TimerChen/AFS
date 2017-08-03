/*
 * Noticed:
 *	8-1:
 *		Removed the "length" argument of RPCApplyMutation function,
 *  because we can know the length of data when recived the data.
 *
 * Remained to do:
 *	8-1:
 *		If I want to change the infomation of a chunk, I have to
 *	make a read lock of map::chunks, and then make a write lock
 *	of this chunk.
 *
 *
 *
 *
 *
 */
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

	//RPCApplyMutation(ChunkHandle handle, ChunkVersion version, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset);
	srv.RPCBind<GFSError
			(ChunkHandle, ChunkVersion, std::uint64_t, MutationType, std::uint64_t, std::uint64_t)>
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
	loadSettings();
	loadFiles( rootDir/chunkFolder );
}
void ChunkServer::loadSettings()
{
	std::ifstream fin((rootDir / chunkFolder / ".setting").native());
	std::string tmp;
	//Load master ip
	fin >> masterIP >> masterPort;
	//Load chunk floder name
	fin >> tmp;
	chunkFolder = tmp;
	//Load other..

	//Close file
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
				WriteLock cmLock(lock_chunkMutex);
				//???
				chunkMutex[handle];

				cmLock.unlock();
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
	fin.open( (rootDir / chunkFolder / std::to_string(handle)).native(), std::ios::in | std::ios::binary );
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
	fin.open( (rootDir / chunkFolder / std::to_string(handle)).native(), std::ios::in | std::ios::binary );
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
void ChunkServer::saveChunkData( const ChunkHandle &handle, const char *data, const std::uint64_t &offset, const std::uint64_t &length )
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
void ChunkServer::addData()
{

}

void ChunkServer::Shutdown()
{


	//At End
	Server::Shutdown();
}


void ChunkServer::deleteData(const std::uint64_t &dataID)
{
	/*
	lock_dataCache.lock();
	lock_cacheQueue.lock();
	auto di = dataCache.find( dataID );
	chunkPool.Delete( std::get<0>(di->second) );
	dataCache.erase( di );
	for( auto itr = cacheQueue.begin(); itr!=cacheQueue.end(); ++itr )
	if( *itr == dataID )
	{
		cacheQueue.erase( itr );
	}

	lock_cacheQueue.unlock();
	lock_dataCache.unlock();
	*/
	WriteLock cqLock(lock_cacheQueue);
	for( auto i=cacheQueue.begin(); i!=cacheQueue.end(); ++i )
	if( dataID == *i ){
		cacheQueue.erase(i);
		break;
	}
	cqLock.unlock();
	WriteLock dcLock(lock_dataCache);
	dataCache.erase( dataID );
	dcLock.unlock();
}

void ChunkServer::deleteChunks(const ChunkHandle &handle)
{
	using namespace boost::filesystem;



	WriteLock cmLock_r(lock_chunkMutex);
	WriteLock cLock(chunkMutex[handle]);
	chunkMutex.erase( handle );
	cmLock_r.unlock();

	remove( rootDir / chunkFolder / (std::to_string(handle)+".chk") );

	cLock.unlock();


}

void ChunkServer::garbageCollect( const std::vector<ChunkHandle> &gbg )
{
	for( auto itr : gbg )
	{
		deleteChunks( itr );
	}
}

bool ChunkServer::checkChunk( const std::int64_t &handle )
{
	//Check existence of this file
	using namespace boost::filesystem;
	if( directory_iterator( rootDir / chunkFolder / (std::to_string(handle)+".chk") ) == directory_iterator() )
		return false;

	//Add CheckSum...

	return true;
}

GFSError ChunkServer::heartBeat()
{
	//std::tuple<GFSError, std::vector<ChunkHandle> /*Garbage Chunks*/>
	//RPCHeartbeat(std::vector<ChunkHandle> leaseExtensions, std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks, std::vector<ChunkHandle> failedChunks);
	std::vector<ChunkHandle> leaseExtensions;
	std::vector<std::tuple<ChunkHandle, ChunkVersion>> allChunks;
	std::vector<ChunkHandle> failedChunks;
	GFSError err;
	ReadLock cLock( lock_chunks );
	for( auto chk : chunks )
	{
		if( checkChunk( chk.first ) )
		{
			allChunks.push_back( std::make_tuple( chk.first, chk.second.version) );
			if( chk.second.primary() /*&& chk be modified(TODO)*/ )
			{
				leaseExtensions.push_back( chk.first );
			}
		}else{
			failedChunks.push_back( chk.first );
		}
	}
	cLock.unlock();
	msgpack::object_handle msg =
			srv.RPCCall( {masterIP,masterPort/*???*/}, "Heartbeat",
							leaseExtensions, allChunks, failedChunks );

	auto re_msg =
			msg.get().as< std::tuple<GFSError, std::vector<ChunkHandle>> >();
	std::tie(err, failedChunks) = re_msg;
	garbageCollect( failedChunks );
	return err;
}

void ChunkServer::Heartbeat()
{
	GFSError err;
	while(running)
	{
		std::this_thread::sleep_for(std::chrono::seconds(5));
		do{
			try{
				err = heartBeat();
			}
			catch(...)
			{
				//TODO...
			}
		}while( err.errCode != GFSErrorCode::OK && running );
	}

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

	return {GFSErrorCode::OK, ""};
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
	GFSError reData = {GFSErrorCode::OK, ""}, secData;
	std::uint64_t length;
	//Check if it holds the lease.

	ReadLock cLock( lock_chunks );
	ReadLock dLock( lock_dataCache );
	auto cItr = chunks.find(handle);
	cItr->second.finished++;
	if( cItr->second.primary() )
	{
		reData = GFSError({GFSErrorCode::OK, ""});
		auto dcItr = dataCache.find(dataID);
		if(dcItr != dataCache.end())
		{
			length = std::get<1>(dcItr->second);
			if( offset + length <= CHUNK_SIZE )
			{
				//local apply
				if( offset + length > cItr->second.length )
				{
					cItr->second.length = offset + length;
					saveChunkInfo( handle, cItr->second );
				}
				saveChunkData(handle, std::get<0>(dcItr->second), offset, length);

				//remote apply
				std::vector< std::future<msgpack::object_handle> >
						msgs( secondaries.size() );
				msgpack::object_handle msg;
				for( std::uint32_t ii = 0; ii < secondaries.size(); ++ii )
				{
					msgs[ii] = srv.RPCAsyncCall( {secondaries[ii],masterPort/*???*/}, "ApplyMutation",
												 handle, cItr->second.finished, MutationWrite, dataID, offset );
				}
				for( std::uint32_t ii = 0; ii < secondaries.size(); ++ii )
				{
					msg = msgs[ii].get();
					secData = msg.get().as<GFSError>();
					if( secData.errCode != GFSErrorCode::OK )
						reData = GFSError({GFSErrorCode::OperateFailed, "Partly failed"});
				}

			}
			else
				reData = GFSError({GFSErrorCode::OperationOverflow, "Read overflow"});
		}
		else
			reData = GFSError({GFSErrorCode::InvalidOperation, "No such data"});
	}
	else
	{
		reData = GFSError({GFSErrorCode::InvalidOperation, "Not primary"});
	}
	dLock.unlock();
	cLock.unlock();


	return reData;

}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
std::tuple<GFSError, std::uint64_t /*offset*/>
	ChunkServer::RPCAppendChunk(ChunkHandle handle, std::uint64_t dataID, std::vector<std::string> secondaries)
{
	GFSError reData = {GFSErrorCode::OK, ""}, secData;
	std::uint64_t offset, length;
	//Check if it holds the lease.

	ReadLock cLock( lock_chunks );
	ReadLock dLock( lock_dataCache );
	auto cItr = chunks.find(handle);
	offset = cItr->second.length;
	cItr->second.finished++;

	if( cItr->second.primary() )
	{
		reData = GFSError({GFSErrorCode::OK, ""});
		auto dcItr = dataCache.find( dataID );
		if(dcItr != dataCache.end())
		{
			length = std::get<1>(dcItr->second);

			if( offset + length <= CHUNK_SIZE )
			{
				//Local apply
				cItr->second.length += length;
				saveChunkInfo(handle, cItr->second);
				saveChunkData(handle, std::get<0>(dcItr->second), offset, length);
			}
			else
			{
				cItr->second.length = CHUNK_SIZE;
				saveChunkInfo( handle, cItr->second );
				reData = GFSError({GFSErrorCode::OperationOverflow, "Append overflow"});
			}

			//Remote apply
			std::vector< std::future<msgpack::object_handle> >
					msgs( secondaries.size() );
			msgpack::object_handle msg;
			for( std::uint32_t ii = 0; ii < secondaries.size(); ++ii )
			{
				if( reData.errCode == GFSErrorCode::OK )
					msgs[ii] = srv.RPCAsyncCall( {secondaries[ii],masterPort/*???*/}, "ApplyMutation",
												 handle, cItr->second.finished, MutationAppend, dataID, offset );
				else
					msgs[ii] = srv.RPCAsyncCall( {secondaries[ii],masterPort/*???*/}, "ApplyMutation",
												 handle, cItr->second.finished, MutationPad, dataID, offset );
			}
			for( std::uint32_t ii = 0; ii < secondaries.size(); ++ii )
			{
				msg = msgs[ii].get();
				if( reData.errCode == GFSErrorCode::OK )
				{
					secData = msg.get().as<GFSError>();
					if( secData.errCode != GFSErrorCode::OK )
						reData = GFSError({GFSErrorCode::OperateFailed, "Partly failed"});
				}else{
					;
				}

			}


		}
		else
			reData = GFSError({GFSErrorCode::InvalidOperation, "No such data"});
	}
	else
	{
		reData = GFSError({GFSErrorCode::InvalidOperation, "Not primary"});
	}
	dLock.unlock();
	cLock.unlock();


	return std::make_tuple(reData, offset+length);
}

// RPCApplyMutation is called by primary to apply mutations
GFSError
	ChunkServer::RPCApplyMutation(ChunkHandle handle, ChunkVersion version, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset)
{
	GFSError reData = {GFSErrorCode::OK, ""};
	std::uint64_t length;

	while(1)
	{
		ReadLock cLock_r( lock_chunks );
		if(serialNo == 1 || serialNo == chunks[handle].finished+1)
			break;
		cLock_r.unlock();
	}
	WriteLock cLock( lock_chunks );
	auto cItr = chunks.find(handle);
	if( !cItr->second.primary() || cItr->second.version != version )
	{

		reData = GFSError({GFSErrorCode::OK, ""});
		auto dcItr = dataCache.find( dataID );
		if(dcItr != dataCache.end())
		{
			length = std::get<1>(dcItr->second);
			if( type == MutationPad )
			{
				cItr->second.length = CHUNK_SIZE;
				saveChunkInfo( handle, cItr->second );
			}else{
				if( offset + length <= CHUNK_SIZE )
				{

					//Local apply
					if( offset + length > cItr->second.length )
					{
						cItr->second.length = offset + length;
						saveChunkInfo( handle, cItr->second );
					}
					saveChunkData(handle, std::get<0>(dcItr->second), offset, length);


				}
				else
					reData = GFSError({GFSErrorCode::OperationOverflow, "Read overflow"});
			}

		}
		else
			reData = GFSError({GFSErrorCode::InvalidOperation, "No such data"});
	}
	else
	{
		reData = GFSError({GFSErrorCode::InvalidOperation, "Not secondary/Operation expired"});
	}
	cItr->second.finished++;
	cLock.unlock();

	return reData;
}

// RPCSendCopy is called by master, send the whole copy to given address
GFSError
	ChunkServer::RPCSendCopy(ChunkHandle handle, std::string addr)
{
	GFSError reData = GFSError({GFSErrorCode::OK, ""});
	char *data = chunkPool.newData();
	ReadLock cLock( lock_chunks );


	auto cItr = chunks.find( handle );
	if( cItr != chunks.end() )
	{
		std::string str = loadChunkData( handle, 0, cItr->second.length );
		msgpack::object_handle msg =
				srv.RPCCall( {addr,masterPort/*???*/}, "ApplyCopy",
								handle, cItr->second.version, str, cItr->second.finished );
		chunkPool.Delete(data);
		reData = msg.get().as<GFSError>();
	}else
		reData = GFSError({GFSErrorCode::InvalidOperation, "No such chunk"});

	cLock.unlock();

	return reData;
}

// RPCApplyCopy is called by another replica
// rewrite the local version to given copy data
GFSError
	ChunkServer::RPCApplyCopy(ChunkHandle handle, ChunkVersion version, std::string data, std::uint64_t serialNo)
{
	GFSError reData = GFSError({GFSErrorCode::OK, ""});
	Chunk c;
	c.version = version;
	c.length = data.size();
	c.finished = serialNo;

	WriteLock cLock( lock_chunks );
	auto cItr = chunks.find(handle);
	cItr->second = c;
	saveChunkInfo( handle, c );
	saveChunkData( handle, data.c_str(), data.size() );

	cLock.unlock();

	return reData;
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
								 + std::to_string( handle ) + " "
								 + std::to_string((std::uint32_t)GFSErrorCode::OperationOverflow) + "\n";
		}
		else if( ( itr->second.version == newVersion-1 /*&& itr->second.isPrimary == 0*/ )
			|| ( itr->second.version == newVersion /*&& itr->second.isPrimary == 1*/ ) )
		{
			//Write Infomation
			itr->second.isPrimary = 1;
			itr->second.expireTime = expireTime;
			itr->second.update(newVersion);
			saveChunkInfo( handle, itr->second );
		}
		else if( itr->second.version < newVersion )
		{
			reData.errCode = GFSErrorCode::InvalidOperation;
			reData.description = reData.description
								 + std::to_string( handle ) + " "
								 + std::to_string((std::uint32_t)GFSErrorCode::ChunkVersionExpired) + "\n";
		}
		else // itr->version > newVersion
		{
			reData.errCode = GFSErrorCode::InvalidOperation;
			reData.description = reData.description
								 + std::to_string( handle ) + " "
								 + std::to_string((std::uint32_t)GFSErrorCode::InvalidOperation) + "\n";
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
	if( itr->second.version == newVersion-1 )
	{
		itr->second.update(newVersion);
		itr->second.isPrimary = 0;
		c = itr->second;


		reData = {GFSErrorCode::OK, ""};
	}
	else if( itr->second.version < newVersion )
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
	GFSError reData = { GFSErrorCode::OK, "" };
	char *cdata;
	//lock_dataCache.lock();
	WriteLock dcLock( lock_dataCache );
	if( !dataCache.count( dataID ) )
		reData = { GFSErrorCode::InvalidOperation, "DataId existence" };
	else
	{
		cdata = chunkPool.newData();
		strncpy( cdata, data.c_str(), data.size() );
		dataCache[dataID] = std::make_tuple(cdata, data.size());
	}
	dcLock.unlock();
	//lock_dataCache.unlock();

	if( reData.errCode == GFSErrorCode::OK )
	{
		lock_cacheQueue.lock();
		cacheQueue.push_back( dataID );
		lock_cacheQueue.unlock();

		//when chunkPool.empty() == true it is using the sub-pool of it.
		lock_dataCache.lock();
		lock_cacheQueue.lock();
		while( cacheQueue.size() > MaxCacheSize || chunkPool.empty() )
		{
			dataID = cacheQueue.front();
			auto idc = dataCache.find( dataID );
			chunkPool.Delete( std::get<0>( idc->second ) );
			dataCache.erase( idc );
			cacheQueue.pop_front();
		}
		lock_cacheQueue.unlock();
		lock_dataCache.unlock();
	}


	return reData;
}


/*
GFSError
	ChunkServer::RPCPushDataAndForward(std::uint64_t dataID, std::string data, std::vector<std::string> elseAdd)
{

}
*/

}
