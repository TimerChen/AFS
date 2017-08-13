#include "common_headers.h"
#include "graybox_env.h"
using namespace AFS;
using boost::format;

const std::uint16_t Environment::portMaster = 7777;
const std::uint16_t Environment::portChunkServer = 7778;

Environment::Environment() : netServerNode(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0))
{
	rpcServerNode.bind("ListService", std::function<std::vector<std::string>(std::string)>(
		[this](std::string srvName)
	{
		return ListService(srvName);
	}));
	netServerNode.setReqHandler([this](const std::string &msg, const std::string &ip, std::uint16_t port)->std::string
	{
		return rpcServerNode.onRequest(msg);
	});
	for (size_t i = 0; i < std::thread::hardware_concurrency(); i++)
	{
		std::thread threadWorker([this]()
		{
			netServerNode.Run();
		});
		threadWorker.detach();
	}
}

void Environment::StartMaster(size_t idx)
{
	Start(idx, [idx](pipe_device pipe_stdin, pipe_device pipe_stdout, Service *srv)
	{
		CreateWorkDir("master_" + std::to_string(idx));
		std::ofstream flog("master_" + std::to_string(idx) + ".log", std::ios::app);
		pipe_stream ps_in(pipe_stdin), ps_out(pipe_stdout);
		LightDS::Service masterSrv("master", flog, ps_in, ps_out, portMaster, (format("127.0.0.%d") % (idx + 10)).str());
		Master master(masterSrv, "./master_" + std::to_string(idx) + "/");
		masterSrv.setOnStart(std::bind(&Master::Start, &master));
		masterSrv.setOnStop(std::bind(&Master::Shutdown, &master));
		masterSrv.Run();
	});
}

void Environment::StartChunkServer(size_t idx)
{
	Start(idx, [idx](pipe_device pipe_stdin, pipe_device pipe_stdout, Service *srv)
	{
		CreateWorkDir("chunkserver_" + std::to_string(idx));
		std::ofstream flog("chunkserver_" + std::to_string(idx) + ".log", std::ios::app);
		pipe_stream ps_in(pipe_stdin), ps_out(pipe_stdout);
		LightDS::Service csSrv("chunkserver", flog, ps_in, ps_out, portChunkServer, (format("127.0.0.%d") % (idx + 10)).str());
		ChunkServer cs(csSrv, "./chunkserver_" + std::to_string(idx) + "/");
		csSrv.setOnStart(std::bind(&ChunkServer::Start, &cs));
		csSrv.setOnStop(std::bind(&ChunkServer::Shutdown, &cs));
		csSrv.Run();
	});
}

void Environment::Stop(size_t idx)
{
	std::lock_guard<std::mutex> lock(mtxSrv);
	if (!mapSrv.count(idx) || !mapSrv[idx]->running)
		return;
	mapSrv[idx]->pipe.Stop();
	mapSrv[idx]->threadWorker.join();
}

std::vector<std::string> Environment::ListService(const std::string &srvName)
{
	std::vector<std::string> ret;
	std::lock_guard<std::mutex> lock(mtxSrv);
	for (auto &kv : mapSrv)
	{
		std::lock_guard<std::mutex> lockService(kv.second->mtx);
		if (kv.second->running && kv.second->srvName == srvName)
			ret.push_back(LightDS::Service::RPCAddress{ kv.second->rpcAddr, kv.second->rpcPort }.to_string());
	}
	return ret;
}

LightDS::Service::RPCAddress Environment::GetNodeServerAddr()
{
	return LightDS::Service::RPCAddress{ "127.0.0.1", netServerNode.getLocalEndpoint().port() };
}

void Environment::Start(size_t idx, std::function<void(pipe_device, pipe_device, Service *)> func)
{
	std::lock_guard<std::mutex> lock(mtxSrv);
	if (mapSrv.count(idx) && mapSrv[idx]->running)
		throw std::invalid_argument("Service " + std::to_string(idx) + " is running");

	pipe_device pipe_stdin, pipe_stdout;
	std::unique_ptr<Service> srv(new Service(pipe_stdin, pipe_stdout));

	srv->pipeServer.bind("RegisterRPCService", std::function<void(std::string, std::string, std::uint16_t)>(
		[srv = srv.get()](std::string srvName, std::string rpcAddr, std::uint16_t rpcPort)
	{
		std::lock_guard<std::mutex> lock(srv->mtx);
		srv->srvName = srvName;
		srv->rpcAddr = rpcAddr == "" ? "127.0.0.1" : rpcAddr;
		srv->rpcPort = rpcPort;
		srv->inited = true;
		srv->cv.notify_all();
	}));
	srv->pipeServer.bind("ListService", std::function<std::vector<std::string>(std::string)>(
		[this](std::string srvName) -> std::vector<std::string>
	{
		return ListService(srvName);
	}));

	srv->pipe.setReqHandler([srv = srv.get()](std::string request)->std::string
	{
		return srv->pipeServer.onRequest(std::move(request));
	});
	srv->pipeClient.setSyncReq([srv = srv.get()](const std::string &msg)->std::string
	{
		return srv->pipe.sendRequest(msg);
	});
	srv->pipeClient.setAsyncSend([srv = srv.get()](const std::string &msg, std::function<void(std::string, std::string)> callback)
	{
		srv->pipe.sendAsyncRequest(msg, callback);
	});

	srv->threadComm = std::thread([srv = srv.get()]()
	{
		srv->pipe.Run();
	});
	srv->threadWorker = std::thread([pipe_stdin, pipe_stdout, srv = srv.get(), func]() mutable
	{
		std::unique_lock<std::mutex> lockService(srv->mtx);
		srv->running = true;

		lockService.unlock();

		func(pipe_stdin, pipe_stdout, srv);
		pipe_stdout.close();
		srv->threadComm.join();

		lockService.lock();
		srv->running = false;
		srv->inited = true;
		srv->cv.notify_all();
	});

	std::unique_lock<std::mutex> lockService(srv->mtx);
	if(!srv->inited)
		srv->cv.wait(lockService);

	mapSrv[idx] = std::move(srv);
}

void Environment::CreateWorkDir(std::string dirName)
{
	namespace fs = boost::filesystem;
	fs::path path = fs::current_path() / dirName;
	if (!fs::exists(path))
		fs::create_directory(path);
}

void Environment::RemoveWorkDir(std::string dirName)
{
	namespace fs = boost::filesystem;
	fs::path path = fs::current_path() / dirName;
	if (fs::exists(path))
		fs::remove_all(path);
}

void Environment::ResetMaster(size_t idx)
{
	RemoveWorkDir("master_" + std::to_string(idx));
	CreateWorkDir("master_" + std::to_string(idx));
}

void Environment::ResetChunkServer(size_t idx)
{
	RemoveWorkDir("chunkserver_" + std::to_string(idx));
	CreateWorkDir("chunkserver_" + std::to_string(idx));
}
