//
// Created by Aaron Ren on 22/07/2017.
//

#ifndef AFS_LOG_H
#define AFS_LOG_H

#include <fstream>
#include <string>
#include <list>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>

namespace AFS {

struct Log {
	// test version
	std::thread::id threadid;
	int a;
	explicit Log(std::thread::id t, int a = -1) : threadid(t), a(a) {}

	friend std::ostream& operator<<(std::ostream & fout,const Log & log) {
		fout << "thread: " << log.threadid << ", val: " << log.a;
		return fout;
	}
	// todo
};

class LogContainer {
private:
	/* LogContainer
	 * logs用于保存日志，location为checkpoint文件的存储位置，
	 * 当logs的大小达到checkpoint_size时候，触发checkpoint。
	 * 接口仅包括添加日志和返回checkpoint存储位置。
	 * 在添加日志前，会获得logs_m，接着检查logs的大小和save_m（具体见后），
	 * 如果需要并且可以进行checkpoint，则会把原来的logs拷贝到tmplogs中，
	 * 再重新开一个logs，由于这里使用的是指针，所以这一步开销极小，在完成
	 * 拷贝后立刻释放logs_m,这样可以使得checkpoint和后续的写入日志可以
	 * 同时进行。之后再进行保存日志的工作。
	 * save_m用于保护保存日志的操作。考虑如下情况，由于日志添加速度过快，
	 * 导致再下一次checkpoint时，上一次checkpoint还没有完成（可能性
	 * 极小）。此时如果同时保存日志，会损坏checkpoint文件。所以在开始
	 * 保存之前，需要获得save_m，如果无法获得，则即使logs大小超过了
	 * checkpoint_size，依然不会进行checkpoint。
	 */
	std::mutex      save_m;
	std::string     location;
	int             checkpoint_size;
	static constexpr int
			        default_cpsz = 50;
	std::unique_ptr<std::queue<Log>>
			        logs;
	std::mutex      logs_m;

	void checkAndSave(std::unique_lock<std::mutex> & lk) {
		if (logs->size() < checkpoint_size || !save_m.try_lock())
			return;
		std::unique_lock<std::mutex> slk(save_m, std::adopt_lock);
		auto tmplogs = std::unique_ptr<std::queue<Log>>(logs.release());
		logs = std::make_unique<std::queue<Log>>();
		lk.unlock();

		std::ofstream fout(location, std::ios::app);
//		fout << "debug: save" << std::endl;
		while (!tmplogs->empty()) {
			fout << tmplogs->front() << std::endl;
			tmplogs->pop();
		}
	}

public:
	explicit LogContainer(std::string loc, int cpsz = default_cpsz)
			: location(std::move(loc)), checkpoint_size(cpsz) {
		logs = std::make_unique<std::queue<Log>>();
	}

	void addLog(const Log & log) {
		auto lk = std::unique_lock<std::mutex>(logs_m);
		logs->push(log);
		checkAndSave(lk);
	}
};

}


#endif //AFS_LOG_H
