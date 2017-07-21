/*
 * MasterServer
 *
 *
 *
 */
//ACM File System!
namespace AFS
{
	class Server
	{
		Server();
	protected:
		//Find recover files
		void load();
		//Save server files
		void save();
	public:
		//Stop server
		void stop();
		//Restart server
		void restart();


	}
	class Master : public Server {
	public:
		Master();

		std::pair< Message, AFSError > call_fileOperation	( Message msg );
		std::pair< Message, AFSError > call_chunkUpdate		( Message msg );

	private:
		vector<?> chunkServer;

		//File opreations
		std::pair< ?, AFSError > fileCreate();
		std::pair< ?, AFSError > fileRead();
		std::pair< ?, AFSError > fileWrite();
		std::pair< ?, AFSError > fileAppend();
		//Floder opreations
		sdt::pair< ?, AFSError > fileMkdir();
	}


	class Master_Dup : public Master {
	public:
		Master_Dup();
	}

	class ChunkServer : public Server {
	public:
		std::string nameCode;
		ChunkServer();
		//with Master
		std::pair< Message, AFSError > call_giveLease		( Message msg );
		std::pair< Message, AFSError > call_withdrawLease	( Message msg );


		//with Client
		std::pair< Message, AFSError > call_transData		( Message msg );
		//std::pair< Message, AFSError > call_forwardData		( Message msg );
		std::pair< Message, AFSError > call_applyModify		( Message msg );
		std::pair< Message, AFSError > call_fileOperation	( Message msg );
		std::pair< Message, AFSError > call_chunkUpdate		( Message msg );

		//with ChunckServers
		//std::pair< Message, AFSError > call_transData		( Message msg );
		std::pair< Message, AFSError > call_applyModify_secondly( Message msg );
	}
	class Chunk
	{
		long long version;
		std::vector <?> serverAdd;
	};
	class Client
	{
	public:
		Client();

		//File opreations
		std::pair< ? ,AFSError > fileCreate
			( const std::string &dir );
		std::pair< ? ,AFSError > fileRead
			( const std::string &dir, const std::string &localDir );
		std::pair< ? ,AFSError > fileWrite
			( const std::string &dir, const std::string &localDir );
		std::pair< ? ,AFSError > fileAppend
			( const std::string &dir, const std::string &localDir );
		//Floder opreations
		sdt::pair< ? ,AFSError > fileMkdir
			( const std::string &dir );


	private:
		AFSError connectMaster();
		AFSError connectChunckServer();
		//divide your local file into several chuncks.
		sdt::pair< ? ,AFSError > fileCutter( const std::string &localDir );
	}
}
