#ifndef GFS_TEST_GRAYBOX_ENV_H
#define GFS_TEST_GRAYBOX_ENV_H

#include "common_headers.h"

class Environment
{
protected:
	struct Service
	{
		std::mutex mtx;
		std::condition_variable cv;

		bool running, inited;

		std::string srvName;
		std::string		rpcAddr;
		std::uint16_t	rpcPort;
		std::thread threadWorker;
		std::thread threadComm;

		pipe_stream psIn, psOut;

		LightDS::PipeService pipe;
		LightDS::RPCServer pipeServer;
		LightDS::RPCClient pipeClient;

		Service(pipe_device pipe_stdin, pipe_device pipe_stdout) : 
			running(false), inited(false),
			psIn(pipe_stdin), psOut(pipe_stdout),
			pipe(psOut, psIn) {}
	};

public:
	Environment();

	void StartMaster(size_t idx);
	void StartChunkServer(size_t idx);
	void Stop(size_t idx);
	std::vector<std::string> ListService(const std::string &srvName);
	LightDS::Service::RPCAddress GetNodeServerAddr();

	void ResetMaster(size_t idx);
	void ResetChunkServer(size_t idx);

protected:
	void Start(size_t idx, std::function<void(pipe_device, pipe_device, Service *)> func);
	static void CreateWorkDir(std::string dirName);
	static void RemoveWorkDir(std::string dirName);

protected:
	std::map<size_t, std::unique_ptr<Service>> mapSrv;
	std::mutex mtxSrv;

	LightDS::NetServer netServerNode;
	LightDS::RPCServer rpcServerNode;

	static const std::uint16_t portMaster;
	static const std::uint16_t portChunkServer;
};

#endif