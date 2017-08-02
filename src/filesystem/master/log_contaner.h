//
// Created by Aaron Ren on 28/07/2017.
//

#ifndef AFS_LOG_CONTANER_H
#define AFS_LOG_CONTANER_H

#include "common.h"
#include "setting.hpp"
#include <functional>
#include <queue>
#include <string>
#include <vector>
#include <mutex>
#include <fstream>

namespace AFS {

enum class OpCode {
	GetChunkHandle = 0, ListFile, Mkdir, DeleteFile,
	CreateFile, GetFileInfo, GetPrimaryAndSecondaries,
	HeartBeat
};

const std::string opName[] = {
		"GetChunkHandle", "ListFile", "Mkdir", "DeleteFile",
		"CreateFile", "GetFileInfo", "GetPrimaryAndSecondaries",
		"HeartBeat"
};

struct MasterLog {
	bool        opLog;
	OpCode      op;
	std::vector<std::string> args;
	GFSError    result;
};

class LogContainer {
private:
	std::function<void(std::ofstream &)> checkPoint;
	std::string logPath;
	size_t      sz;
	std::mutex  m;

	struct LogPtr {
		MasterLog log;
		std::function<void(const MasterLog &)> addLog;

		MasterLog & operator*() {
			return log;
		}
		MasterLog * operator->() {
			return &log;
		}

		~LogPtr() {
			addLog(log);
		}
	};

	void addLog(const MasterLog & log) {
		std::lock_guard<std::mutex> lk(m);
		std::ofstream fout(logPath);
		fout << log.opLog << ' ';
		if (!log.opLog) {
			fout << log.args.front() << std::endl;
			return;
		}
		fout << (int)log.op << ' ' << opName[(int)log.op] << ' ';
		for (auto &&arg : log.args) {
			fout << arg << ' ';
		}
//		fout << result
		fout << std::endl;
	}

public:
	LogContainer(std::string path, std::function<void(std::ofstream &)> cp)
			: logPath(std::move(path)), checkPoint(std::move(cp)), sz(0) {}


	LogPtr getPtr(bool opLog, OpCode op, std::vector<std::string> args) {
		LogPtr ptr;
		ptr.addLog = std::bind(&LogContainer::addLog, this, std::placeholders::_1);
		ptr->opLog = opLog;
		ptr->op = op;
		ptr->args = std::move(args);
//		ptr->result =
		return ptr;
	}
};

}


#endif //AFS_LOG_CONTANER_H
