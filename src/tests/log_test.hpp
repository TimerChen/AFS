//
// Created by Aaron Ren on 22/07/2017.
//

#ifndef AFS_LOG_TEST_HPP
#define AFS_LOG_TEST_HPP

#include <iostream>
#include <thread>
#include <vector>
#include "log.h"

using namespace std;

namespace AFS {

class LogContainerTest {
private:
	LogContainer lc;
	LogContainerTest() : lc("/Users/aaronren/Desktop/GFS/test/checkpoint.txt") {}

	void add_log(int a = 0, int b = 1000) {
		for (int i = a; i < b; ++i) {
			lc.addLog(Log(this_thread::get_id(), i));
		}
	}

public:
	static LogContainerTest& instance() {
		static LogContainerTest in;
		return in;
	}

	void test() {
		cout << "Constructing LogContainer" << endl;
		cout << "Complete" << endl;
		cout << "Start adding logs" << endl;
		add_log();
		cout << "Complete" << endl;
	}

	void concurrency_test() {
		vector<thread> t;
		for (int i = 0; i < 5; ++i) {
			t.emplace_back(thread(&LogContainerTest::add_log, this, 0, 100));
		}
		for (int i = 0; i < 5; ++i) {
			t[i].join();
		}
	}
};

}

#endif //AFS_LOG_TEST_HPP
