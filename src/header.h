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
	class Master : public Server
	{
	public:
		Master();

		//File opreations
		std::pair< ? ,AFSError > fileCreate();
		std::pair< ? ,AFSError > fileRead();
		std::pair< ? ,AFSError > fileWrite();
		std::pair< ? ,AFSError > fileAppend();
		//Floder opreations
		sdt::pair< ? ,AFSError > fileMkdir();


	}

	class Master_Dup : public Master
	{
	public:
		Master_Dup();
	}

	class ChunkServer : public Server
	{
	public:
		ChunkServer();

	}

	class Client
	{
	public:
		Client();


		//File opreations
		std::pair< ? ,AFSError > fileCreate	( std::string dir );
		std::pair< ? ,AFSError > fileRead	( std::string dir );
		std::pair< ? ,AFSError > fileWrite	( std::string dir );
		std::pair< ? ,AFSError > fileAppend	( std::string dir );
		//Floder opreations
		sdt::pair< ? ,AFSError > fileMkdir	( std::string dir, std::string content );

	private:
		AFSError connectMaster();
		AFSError connectChunckServer();
	}
}
