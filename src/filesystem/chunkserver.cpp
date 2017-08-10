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
 * Questions:
 * 1.Does a not continuous chunk's lock work proper?(in function RPCWriteChunk..)
 * 2.Do we need a atomic 'primary()' function in class chunk?
 * 3.Where is the right position of "*.finishing++"?
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

	std::thread( &ChunkServer::Heartbeat, this ).detach();
}
void ChunkServer::load()
{
	loadSettings();
	loadFiles( rootDir/chunkFolder );
}
void ChunkServer::loadSettings()
{
	/*
	std::ifstream fin((rootDir / chunkFolder / ".setting").native());
	if( !fin.is_open() )
	{
		std::ofstream fout( (rootDir / chunkFolder / ".setting").native() );
		//IP and Port
		fout << "127.0.0.1" << " " << "12308" << '\n';
		//Chunks Folder
		fout << 'data\n';
		fout.close();
		fin.open( (rootDir / chunkFolder / ".setting").native() );
	}
	std::string tmp;
	//Load master ip
	fin >> masterIP >> masterPort;
	//Load chunk floder name
	fin >> tmp;
	chunkFolder = tmp;
	//Load other..

	//Close file
	fin.close();*/
}

void ChunkServer::loadFiles( const boost::filesystem::path &Path )
{
	using namespace boost::filesystem;
	std::string fname;
	if( !exists( Path ) )
		boost::filesystem::create_directory(Path);
	if( !exists( Path ) || !is_directory( Path ) )
		throw( DirNotFound() );
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

			//continue;
			if( fname.size() > 4 && fname.substr( fname.size()-4, 4 ) == ".chk" )
			{
				std::clog << "Loading chunk("<< itr->path().filename().native() <<") from disk...." ;//<< std::endl;

				fname = fname.substr(0, fname.size()-4);

				ChunkHandle handle = std::stoull(fname,0,10);
				Chunk c = loadChunkInfo( handle );

				WriteLock cmLock(lock_chunkMutex);
				//???
				chunkMutex[handle];
				cmLock.unlock();

				WriteLock cLock( lock_chunks );
				chunks[handle] = c;
				cLock.unlock();

				std::clog << "OK" << std::endl;
			}else{
				//TODO

				//ignore..?
			}
		}

	}
}
Chunk ChunkServer::loadChunkInfo( const ChunkHandle &handle )
{

	ReadLock cmLock( lock_chunkMutex );
	ReadLock cLock(chunkMutex[handle]);
	cmLock.unlock();

	return std::move( loadChunkInfo_noLock(handle) );
}

Chunk ChunkServer::loadChunkInfo_noLock( const ChunkHandle &handle )
{

	Chunk c;
	std::fstream fin;

	//Open file
	fin.open( (rootDir / chunkFolder / (std::to_string(handle)+".chk")).native() );
	if( !fin.is_open() )
	{
		throw( DirNotFound() );
	}
	//lock_chunkMutex.lock();

	//Read
	fin.read( (char*)&c.version, sizeof(c.version) );
	fin.read( (char*)&c.length , sizeof(c.length ) );

	//End
	fin.close();

	return c;
}
std::string ChunkServer::loadChunkData( const ChunkHandle &handle, const std::uint64_t &offset, const std::uint64_t &length )
{
	ReadLock cmLock( lock_chunkMutex );
	ReadLock cLock(chunkMutex[handle]);
	cmLock.unlock();

	return std::move( loadChunkData_noLock( handle, offset, length ) );
}

std::string ChunkServer::loadChunkData_noLock( const ChunkHandle &handle, const std::uint64_t &offset, const std::uint64_t &length )
{
	std::fstream fin;
	std::string str;

	//Open file
	fin.open( (rootDir / chunkFolder / (std::to_string(handle)+".chk")).native() );
	if( !fin.is_open() )
	{
		throw( DirNotFound() );
	}

	char *buffer = chunkPool.newData();

	//Read
	fin.seekg( sizeof(ChunkVersion)+sizeof(Chunk::ChunkLength)+offset );
	fin.read( buffer, length );

	//End
	fin.close();
	str = std::string(buffer,length);
	chunkPool.Delete( buffer );

	return std::move(str);
}
void ChunkServer::save()
{
	//Checksum...?
}

void ChunkServer::saveChunkInfo( const ChunkHandle &handle, const Chunk &c )
{
	ReadLock cmLock(lock_chunkMutex);
	WriteLock cLock(chunkMutex[handle]);
	cmLock.unlock();

	saveChunkInfo_noLock( handle, c );

	cLock.unlock();
}

void ChunkServer::saveChunkInfo_noLock( const ChunkHandle &handle, const Chunk &c )
{
	std::fstream fout;

	//Open file
	fout.open( (rootDir / chunkFolder / (std::to_string(handle)+".chk")).native() );
	if( !fout.is_open() )
	{
		throw( DirNotFound() );
	}
	//Write
	fout.write( (char*)&c.version, sizeof(c.version) );
	fout.write( (char*)&c.length , sizeof(c.length ) );

	//End
	fout.close();
}
void ChunkServer::saveChunkData( const ChunkHandle &handle, const char *data, const std::uint64_t &offset, const std::uint64_t &length )
{
	ReadLock cmLock(lock_chunkMutex);
	WriteLock cLock(chunkMutex[handle]);
	cmLock.unlock();

	saveChunkData_noLock( handle, data, offset, length );

	cLock.unlock();
}

void ChunkServer::saveChunkData_noLock( const ChunkHandle &handle, const char *data, const std::uint64_t &offset, const std::uint64_t &length )
{
	std::fstream fout;

	//Open file
	fout.open( (rootDir / chunkFolder / (std::to_string(handle)+".chk")).native() );
	if( !fout.is_open() )
	{
		throw( DirNotFound() );
	}
	//Read
	fout.seekp( sizeof(ChunkVersion) + sizeof(Chunk::ChunkLength) + offset );
	fout.write( data, length );

	//End
	fout.close();
}

void ChunkServer::Shutdown()
{

	//srv.Stop();

	//At End
	//TODO???
	Server::Shutdown();
}


void ChunkServer::deleteData(const std::uint64_t &dataID)
{

	WriteLock cqLock(lock_cacheQueue);
	for( auto i=cacheQueue.begin(); i!=cacheQueue.end(); ++i )
	if( dataID == *i ){
		cacheQueue.erase(i);
		break;
	}
	cqLock.unlock();

	WriteLock dcLock(lock_dataCache);
	auto di = dataCache.find( dataID );
	chunkPool.Delete( std::get<0>(di->second) );
	dataCache.erase( dataID );
	dcLock.unlock();
}

void ChunkServer::deleteChunks(const ChunkHandle &handle)
{
	using namespace boost::filesystem;

	WriteLock cLock(lock_chunks);
	WriteLock cmLock(lock_chunkMutex);
	WriteLock cLock0(chunkMutex[handle]);

	chunks.erase( handle );
	cLock.unlock();

	remove( rootDir / chunkFolder / (std::to_string(handle)+".chk") );

	cLock0.unlock();
	chunkMutex.erase( handle );
	cmLock.unlock();

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
		ReadLock cmLock( lock_chunkMutex );
		ReadLock cLock0( chunkMutex[chk.first] );
		cmLock.unlock();
		if( checkChunk( chk.first ) )
		{
			allChunks.push_back( std::make_tuple( chk.first, chk.second.version) );
			if( chk.second.primary() && chk.second.needExtensions )
			{
				chk.second.needExtensions = 0;
				leaseExtensions.push_back( chk.first );
			}
		}else{
			failedChunks.push_back( chk.first );
		}
		cLock0.unlock();
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
		auto masterList = srv.ListService("master");
		for( auto addr : masterList )
		{
			masterIP = addr.ip;
			masterPort = addr.port;
			while( err.errCode != GFSErrorCode::OK && running )
			{
				try{
					err = heartBeat();
				}
				catch(...)
				{
					//TODO...

				}
			}
		}

	}

}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
GFSError
	ChunkServer::RPCCreateChunk(ChunkHandle handle)
{
	GFSError reData = {GFSErrorCode::OK, ""};
	Chunk c;
	char *buffer = chunkPool.newData();
	//The version start form zero ?????
	c.version = 0;
	c.length = 0;

	//Save chunks info in chunks.
	WriteLock cLock( lock_chunks );
	WriteLock cmLock( lock_chunkMutex );
	WriteLock cLock0(chunkMutex[handle]);
	cmLock.unlock();
	if(chunks.count(handle))
	{
		reData = {GFSErrorCode::InvalidOperation, "File existence."};
	}else
		chunks[handle] = c;
	cLock.unlock();

	std::ofstream fout;
	fout.open( (rootDir / chunkFolder / (std::to_string(handle)+".chk")).native() );
	fout.close();

	if( reData == GFSErrorCode::OK )
	{
		try{
			saveChunkInfo_noLock( handle, c );
			saveChunkData_noLock( handle, buffer );
		}catch(...){
			reData = {GFSErrorCode::Unknown, ""};
		}
	}

	cLock0.unlock();
	chunkPool.Delete( buffer );

	return reData;
}

// RPCReadChunk is called by client, read chunk data and return
std::tuple<GFSError, std::string /*Data*/>
	ChunkServer::RPCReadChunk(ChunkHandle handle, std::uint64_t offset, std::uint64_t length)
{
	std::tuple<GFSError, std::string /*Data*/> reData;
	Chunk c = loadChunkInfo( handle );
	std::get<0>(reData) = GFSError({GFSErrorCode::OK, ""});

	if(offset + length > c.length)
	{
		reData = std::make_tuple( GFSError({GFSErrorCode::OperationOverflow, "ReadOverflow"}), "" );
		length = c.length - offset;
	}
	if( offset > CHUNK_SIZE )
	{
		std::get<1>(reData) = "";
	}else{
		try{
			std::get<1>(reData) = std::move( loadChunkData( handle, offset, length ) );
		}
		catch(...)
		{
			reData = std::make_tuple( GFSError({GFSErrorCode::InvalidOperation, "???"}), "" );
		}
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
	std::vector< std::future<msgpack::object_handle> >
			msgs( secondaries.size() );
	//Check if it holds the lease.

	ReadLock cLock( lock_chunks );
	ReadLock dLock( lock_dataCache );
	auto dcItr = dataCache.find(dataID);
	if(dcItr != dataCache.end())
	{
		length = std::get<1>(dcItr->second);
		auto cItr = chunks.find(handle);

		ReadLock cmLock( lock_chunkMutex );
		WriteLock cLock0( chunkMutex[handle] );
		cmLock.unlock();
		if( cItr->second.primary() )
		{
			if( offset + length <= CHUNK_SIZE )
			{
				cItr->second.needExtensions = 1;
				reData = GFSError({GFSErrorCode::OK, ""});
				//Local memory
				cItr->second.finished++;
				if( offset + length > cItr->second.length )
					cItr->second.length = offset + length;

				//Remote apply
				for( std::uint32_t ii = 0; ii < secondaries.size(); ++ii )
				{
					msgs[ii] = srv.RPCAsyncCall( {secondaries[ii],masterPort/*???*/}, "ApplyMutation",
												 handle, cItr->second.finished, MutationWrite, dataID, offset );
				}

				//Local disk
				saveChunkData_noLock(handle, std::get<0>(dcItr->second), offset, length);
				saveChunkInfo_noLock( handle, cItr->second );
				/*
				Chunk tmp = loadChunkInfo_noLock( handle );
				std::cerr << "tmp:" << tmp.length;
				*/
			}
			else
				reData = GFSError({GFSErrorCode::OperationOverflow, "Read overflow"});
		}
		else
			reData = GFSError({GFSErrorCode::InvalidOperation, "Not primary"});
		cLock0.unlock();

		msgpack::object_handle msg;
		if( reData.errCode == GFSErrorCode::OK )
		for( std::uint32_t ii = 0; ii < secondaries.size(); ++ii )
		{
			msg = msgs[ii].get();
			secData = msg.get().as<GFSError>();
			if( secData.errCode != GFSErrorCode::OK )
				reData = GFSError({GFSErrorCode::OperateFailed, "Partly failed"});
		}
	}
	else
	{
		reData = GFSError({GFSErrorCode::InvalidOperation, "No such data"});
	}
	dLock.unlock();
	cLock.unlock();

	if(reData.errCode == GFSErrorCode::OK)
		deleteData( dataID );

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
	std::vector< std::future<msgpack::object_handle> >
			msgs( secondaries.size() );
	//Check if it holds the lease.

	ReadLock cLock( lock_chunks );
	ReadLock dLock( lock_dataCache );
	auto dcItr = dataCache.find( dataID );
	if(dcItr != dataCache.end())
	{
		length = std::get<1>(dcItr->second);
		auto cItr = chunks.find(handle);

		ReadLock cmLock( lock_chunkMutex );
		WriteLock cLock0( chunkMutex[handle] );
		cmLock.unlock();
		offset = cItr->second.length;
		if( cItr->second.primary() )
		{
			cItr->second.needExtensions = 1;
			//Local memory
			cItr->second.finished++;
			if( offset + length <= CHUNK_SIZE )
			{

				cItr->second.length += length;
				reData = GFSError({GFSErrorCode::OK, ""});
			}
			else
			{
				cItr->second.length = CHUNK_SIZE;
				reData = GFSError({GFSErrorCode::OperationOverflow, "Append overflow"});
			}


			//Remote apply
			for( std::uint32_t ii = 0; ii < secondaries.size(); ++ii )
			{
				if( reData.errCode == GFSErrorCode::OK )
					msgs[ii] = srv.RPCAsyncCall( {secondaries[ii],masterPort/*???*/}, "ApplyMutation",
												 handle, cItr->second.finished, MutationAppend, dataID, offset );
				else
					msgs[ii] = srv.RPCAsyncCall( {secondaries[ii],masterPort/*???*/}, "ApplyMutation",
												 handle, cItr->second.finished, MutationPad, dataID, offset );
			}

			//Local disk
			saveChunkInfo_noLock(handle, cItr->second);
			if( reData.errCode == GFSErrorCode::OK )
				saveChunkData_noLock(handle, std::get<0>(dcItr->second), offset, length);
		}
		else
			reData = GFSError({GFSErrorCode::InvalidOperation, "Not primary"});
		cLock0.unlock();

		msgpack::object_handle msg;
		if( reData.errCode == GFSErrorCode::OK || reData.errCode == GFSErrorCode::OperationOverflow )
		for( std::uint32_t ii = 0; ii < secondaries.size(); ++ii )
		{
			msg = msgs[ii].get();
			if( reData.errCode == GFSErrorCode::OK )
			{
				secData = msg.get().as<GFSError>();
				if( secData.errCode != GFSErrorCode::OK )
					reData = GFSError({GFSErrorCode::OperateFailed, "Partly failed"});
			}else{
				//TODO...???
				;
			}

		}
	}
	else
	{
		reData = GFSError({GFSErrorCode::InvalidOperation, "No such data"});
	}
	dLock.unlock();
	cLock.unlock();

	if(reData.errCode == GFSErrorCode::OK)
		deleteData( dataID );

	return std::make_tuple(reData, offset+length);
}

// RPCApplyMutation is called by primary to apply mutations
GFSError
	ChunkServer::RPCApplyMutation(ChunkHandle handle, ChunkVersion version, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset)
{
	GFSError reData = {GFSErrorCode::OK, ""};
	std::uint64_t length;
	//Wait for order
	while(1)
	{
		ReadLock cLock_r( lock_chunks );
		if(serialNo == 1 || serialNo == chunks[handle].finished+1)
			break;
		cLock_r.unlock();
		std::this_thread::yield();
	}

	ReadLock cLock( lock_chunks );
	ReadLock dLock( lock_dataCache );
	auto cItr = chunks.find(handle);
	if( !cItr->second.primary() && cItr->second.version == version )
	{

		auto dcItr = dataCache.find( dataID );
		if(dcItr != dataCache.end())
		{
			reData = GFSError({GFSErrorCode::OK, ""});
			length = std::get<1>(dcItr->second);
			ReadLock cmLock( lock_chunkMutex );
			WriteLock cLock0( chunkMutex[handle] );
			cmLock.unlock();
			if( type == MutationPad )
			{
				cItr->second.length = CHUNK_SIZE;
				saveChunkInfo_noLock( handle, cItr->second );
			}else{
				if( offset + length <= CHUNK_SIZE )
				{

					//Local apply
					if( offset + length > cItr->second.length )
					{
						cItr->second.length = offset + length;
					}
					saveChunkInfo_noLock( handle, cItr->second );
					saveChunkData_noLock(handle, std::get<0>(dcItr->second), offset, length);


				}
				else
				{
					reData = GFSError({GFSErrorCode::OperationOverflow, "Write overflow"});
				}
			}
			cLock0.unlock();

		}
		else
			reData = GFSError({GFSErrorCode::InvalidOperation, "No such data"});
	}
	else
	{
		reData = GFSError({GFSErrorCode::InvalidOperation, "Not secondary/Operation expired"});
	}
	cItr->second.finished++;
	dLock.unlock();
	cLock.unlock();


	if(reData.errCode == GFSErrorCode::OK)
		deleteData( dataID );

	return reData;
}

// RPCSendCopy is called by master, send the whole copy to given address
GFSError
	ChunkServer::RPCSendCopy(ChunkHandle handle, std::string addr)
{
	GFSError reData = GFSError({GFSErrorCode::OK, ""});
	ReadLock cLock( lock_chunks );

	auto cItr = chunks.find( handle );
	if( cItr != chunks.end() )
	{
		ReadLock cmLock( lock_chunkMutex );
		ReadLock cLock0( chunkMutex[handle] );
		cmLock.unlock();
		Chunk c = cItr->second;
		std::string str = loadChunkData_noLock( handle, 0, c.length );
		cLock0.unlock();

		msgpack::object_handle msg =
				srv.RPCCall( {addr,masterPort/*???*/}, "ApplyCopy",
								handle, c.version, str, c.finished );
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

	RPCCreateChunk( handle );

	ReadLock cLock( lock_chunks );
	ReadLock cmLock( lock_chunkMutex );
	WriteLock cLock0( chunkMutex[handle] );
	cmLock.unlock();

	auto cItr = chunks.find(handle);
	cItr->second = c;
	saveChunkInfo_noLock( handle, c );
	saveChunkData_noLock( handle, data.c_str(), data.size() );

	cLock0.unlock();
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

		//lock_chunks.lock();
		ReadLock cLock( lock_chunks );
		ReadLock cmLock( lock_chunkMutex );
		WriteLock cLock0( chunkMutex[handle] );
		cmLock.unlock();

		auto itr = chunks.find(handle);
		if( itr == chunks.end() )
		{
			reData.errCode = GFSErrorCode::OperationOverflow;
			reData.description = reData.description
								 + std::to_string( handle ) + " "
								 + "OperationOverflow" + "\n";
		}
		else if( ( itr->second.version == newVersion-1 && !itr->second.primary() )
			|| ( itr->second.version == newVersion && itr->second.primary() ) )
		{
			//Write Infomation
			itr->second.isPrimary = 1;
			itr->second.expireTime = expireTime;
			itr->second.update(newVersion);
			saveChunkInfo_noLock( handle, itr->second );
		}
		else if( itr->second.version < newVersion )
		{
			reData.errCode = GFSErrorCode::ChunkVersionExpired;
			reData.description = reData.description
								 + std::to_string( handle ) + " "
								 + "ChunkVersionExpired" + "\n";
		}
		else // itr->version > newVersion
		{
			reData.errCode = GFSErrorCode::InvalidOperation;
			reData.description = reData.description
								 + std::to_string( handle ) + " "
								 + "InvalidOperation" + "\n";
		}
		cLock0.unlock();
		cLock.unlock();
	}

	return reData;
}

// RPCUpdateVersion is called by master
// update the given chunks' version to 'newVersion'
GFSError
	ChunkServer::RPCUpdateVersion(ChunkHandle handle, ChunkVersion newVersion)
{
	GFSError reData;

	ReadLock cLock( lock_chunks );
	ReadLock cmLock( lock_chunkMutex );
	WriteLock cLock0( chunkMutex[handle] );
	cmLock.unlock();

	auto itr = chunks.find(handle);
	if( itr->second.version == newVersion-1 )
	{
		itr->second.expireTime = 0;
		itr->second.update(newVersion);
		itr->second.isPrimary = 0;
		saveChunkInfo_noLock( handle, itr->second );

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
	cLock0.unlock();
	cLock.unlock();



	return reData;
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer.
// This should be replaced by a chain forwarding.
GFSError
	ChunkServer::RPCPushData(std::uint64_t dataID, std::string data)
{
	std::cerr << "RPC...";
	GFSError reData = { GFSErrorCode::OK, "" };
	char *cdata;
	WriteLock dcLock( lock_dataCache );
	if( dataCache.count( dataID ) )
		reData = { GFSErrorCode::InvalidOperation, "DataId existence" };
	else
	{
		cdata = chunkPool.newData();
		std::cerr << "copying...";
		strncpy( cdata, data.c_str(), data.size() );
		std::cerr << "push_in_cache...";
		dataCache[dataID] = std::make_tuple(cdata, data.size());
	}


	if( reData.errCode == GFSErrorCode::OK )
	{
		WriteLock cqLock(lock_cacheQueue);
		std::cerr << "push_in_queue...";
		cacheQueue.push_back( dataID );


		//when chunkPool.empty() == true it is using the sub-pool of it.
		while( cacheQueue.size() > MaxCacheSize || chunkPool.empty() )
		{
			std::cerr << "pop_old...";
			dataID = cacheQueue.front();
			auto idc = dataCache.find( dataID );
			chunkPool.Delete( std::get<0>( idc->second ) );
			dataCache.erase( idc );
			cacheQueue.pop_front();
		}
		cqLock.unlock();
	}
	dcLock.unlock();

	return reData;
}


/*
GFSError
	ChunkServer::RPCPushDataAndForward(std::uint64_t dataID, std::string data, std::vector<std::string> elseAdd)
{

}
*/

}
