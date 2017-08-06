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
	Unknown,
	createFile, Mkdir, // args: path
	AddChunk           // args: path, handle
};

const std::string opName[] = {
		"Unknown", "createFile", "Mkdir", "addChunk"
};

struct MasterLog {
	OpCode      op{OpCode::Unknown};
	std::vector<std::string> args;
	bool        success{false};
};

class LogContainer {
private:
	std::function<void(std::ofstream &)> checkPoint;
	std::string rootPath;
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
			if (log.success)
				addLog(log);
		}
	};

	void addLog(const MasterLog & log) {
		std::lock_guard<std::mutex> lk(m);
		std::ofstream fout(rootPath + "log.dat", std::ios::app);
		fout << (int)log.op << ' ' << opName[(int)log.op] << ' ';
		for (auto &&arg : log.args) {
			fout << arg << ' ';
		}
		fout << std::endl;
		++sz;
		if (sz < CheckPointSize)
			return;
		fout.close();
		fout.open(rootPath + "checkpoint.dat");
		checkPoint(fout);
		fout.open(rootPath + "log.dat"); // 清空log
		sz = 0;
	}

public:
	LogContainer(std::string path, std::function<void(std::ofstream &)> cp)
			: rootPath(std::move(path)), checkPoint(std::move(cp)), sz(0) {}

	LogPtr getPtr(OpCode op) {
		LogPtr ptr;
		ptr.addLog = std::bind(&LogContainer::addLog, this, std::placeholders::_1);
		ptr->op = op;
		ptr->success = false;
		return ptr;
	}
};

}


#endif //AFS_LOG_CONTANER_H
