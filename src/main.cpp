#include <cstdio>
#include <iostream>
#include <fstream>

#include <master.h>
#include <client.h>
#include <chunkserver.h>

#include <tests/LightDSTest/servera.h>

using namespace std;

std::string generateData( )
{
	std::string str;
	str.resize(AFS::CHUNK_SIZE/2);
	for( uint32_t i = 0; i < AFS::CHUNK_SIZE/2; ++i )
	{
		str[i] = 'a'+i%26;
	}
	return std::move( str );
}

int main(int argc, char**argv)
{
	using namespace AFS;
	int i,j;
	std::uint16_t port, toPort;
	std::string masterIP = "127.0.0.1";
	std::uint64_t offset;
	if(argc==1)
	{
		port = 12308;
	}else{
		port = atoi(argv[1]);
	}

	std::string str = generateData();

	ofstream logfile("test_log.txt",ios::out);
	stringstream sin,sout;
	LightDS::Service srv("AFSServer", logfile, sin, sout, port);

	std::thread srvThd(&LightDS::Service::Run, &srv);


	//chunkserver
//	AFS::ChunkServer cs(srv,".");
//	cerr << "Chunk Server Starting....";
//	cs.Start();
//	cerr << "OK.\n";

//	std::string data = std::move( generateData() ), data_recive;
//	AFS::GFSError re;
//	uint64_t dataID = rand();
//	ChunkHandle handle = rand();
//	std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>>
//			glArgs;
//	glArgs.push_back( std::make_tuple( handle, 1, time(0)+60*60 ) );

//	cerr << "CreateChunk...";
//	re = srv.RPCCall({masterIP,port}, "CreateChunk", handle).get().as<GFSError>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	cerr << "PushData...";
//	re = srv.RPCCall({masterIP,port}, "PushData", dataID, data).get().as<GFSError>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	cerr << "GrantLease...";
//	re = srv.RPCCall({masterIP,port}, "GrantLease", glArgs).get().as<GFSError>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	/*
//	cerr << "WriteChunk...";
//	re = srv.RPCCall({masterIP,port}, "WriteChunk", handle, dataID, 0, std::vector<std::string>()).get().as<GFSError>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	*/
//	cerr << "AppendChunk...";
//	std::tie(re,offset) = srv.RPCCall({masterIP,port}, "AppendChunk", handle, dataID, std::vector<std::string>()).get().as<std::tuple<GFSError, std::uint64_t /*offset*/>>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	cerr << "offset: " << offset << '\n';

//	cerr << "PushData...";
//	re = srv.RPCCall({masterIP,port}, "PushData", dataID, data).get().as<GFSError>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	cerr << "GrantLease...";
//	re = srv.RPCCall({masterIP,port}, "GrantLease", glArgs).get().as<GFSError>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	/*
//	cerr << "WriteChunk...";
//	re = srv.RPCCall({masterIP,port}, "WriteChunk", handle, dataID, 0, std::vector<std::string>()).get().as<GFSError>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	*/
//	cerr << "AppendChunk...";
//	std::tie(re,offset) = srv.RPCCall({masterIP,port}, "AppendChunk", handle, dataID, std::vector<std::string>()).get().as<std::tuple<GFSError, std::uint64_t /*offset*/>>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	cerr << "offset: " << offset << '\n';

//	cerr << "ReadChunk...";
//	std::tie(re,data_recive) = srv.RPCCall({masterIP,port}, "ReadChunk", handle, CHUNK_SIZE/2, CHUNK_SIZE/2).get().as<std::tuple<GFSError, std::string /*Data*/>>();
//	cerr << "OK.\n\t" << (uint16_t)re.errCode << " " << re.description << "\n";
//	cerr << (data_recive == data) << endl;
//	cs.Shutdown();

//	ServerA sra(srv);

//	sra.sendMessage( masterIP, port, str );

	Master master(srv,".");
	srv.Stop();
	sin << "{\"type\":\"bye\"}\n";

	srvThd.join();
	cerr << "Hello";
	return 0;
}

/*
#include <gtest/gtest.h>

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}*/
