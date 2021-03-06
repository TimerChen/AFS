//
// Created by Aaron Ren on 24/07/2017.
//

#ifndef AFS_EXTRIE_TEST_HPP
#define AFS_EXTRIE_TEST_HPP

#include "extrie.hpp"
#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <functional>
#include <vector>
#include <memory>
#include <map>
#include <set>
#include <utility>

//#define PRINT_CHECK
//#define PRINT_GET

using namespace std;
using AFS::extrie;

class ExtrieTest {
	static constexpr int Mod = 127;

	using mapViI = map<vector<int>, int>;

	unique_ptr<vector<int>>
	get_index(int len = 5) {
		auto result = make_unique<vector<int>>();
		for (int i = 0; i < len; ++i)
			result->emplace_back(rand() % Mod);
		return result;
	}
	bool check_equal(const extrie<int, int> & t, const mapViI & stdt) {
		for (auto &&item : stdt) {
#ifdef PRINT_CHECK
			cout << "index: ";
			for (int i = 0; i < (int)item.first.size(); ++i)
				cout << item.first[i] << ' ';
			cout << endl;
			cout << "std value = " << item.second << endl;
			cout << "t   value = " << t[item.first] << endl;
#endif
			if (t[item.first] != item.second)
				return false;
		}
		return true;
	}

	void insert_something(extrie<int, int> & t,
	                      map<vector<int>, int> * mp = nullptr,
	                      int len = 5) {
		auto index = get_index(len);
		for (auto iter = index->begin() + 1; iter <= index->end(); ++iter) {
			int val = rand() % Mod;
			t.insert(index->begin(), iter, val);
			if (mp) {
				mp->insert(make_pair(
						vector<int>(index->begin(), iter),
				        val));
			}
		}
	}

	void test_inesrt() {
		cout << "test_insert: ";

		extrie<int, int> t;
		map<vector<int>, int> stdt;
		for (int i = 0; i < 100; ++i)
			insert_something(t, &stdt);
		if (!check_equal(t, stdt))
			cout << "WA" << endl;
		else
			cout << "AC" << endl;
	}

	void test_iterate() {
		cout << "start test_iterate\n";
		extrie<int, int> t;
		map<vector<int>, int> stdt;
		for (int i = 0; i < 100; ++i)
			insert_something(t, &stdt);

		auto fc1 = [](int & x) {
			x *= 2;
		};
		auto fc2 = [](int & x) {
			x++;
		};
		vector<function<void(int&)>> fcs;
		fcs.push_back(fc1);
		fcs.push_back(fc2);

		// apply fc on every element in stdt
		for (auto &&item : stdt) {
			for (auto &&fc : fcs) {
				fc(item.second);
			}
		}

		t.iterate(vector<int>(), fcs);

		if (check_equal(t, stdt))
			cout << "AC" << endl;
		else
			cout << "WA" << endl;
	}

	void test_remove() {
//#define ERASE_FILE

		cout << "start test_remove: \n";
		extrie<int, int> t;
		mapViI stdt;
		vector<vector<int>> indexes;
		vector<vector<int>> vals;

		tie(indexes, vals) = get_something_to_insert();
		for (int i = 0; i < (int)indexes.size(); ++i) {
			for (int j = 0; j < (int)indexes[i].size(); ++j)
				stdt.insert(make_pair(
						vector<int>(&indexes[i][0], &indexes[i][j + 1]),
						vals[i][j]));
		}
		auto ins_t = [&](int beg, int end) {
			for (; beg < end; ++beg) {
				for (int i = 0; i < (int)indexes[beg].size(); ++i)
					t.insert(vector<int>(&indexes[beg][0], &indexes[beg][i + 1]),
					         vals[beg][i]);
			}
		};
		ins_t(0, (int)indexes.size());
		cout << "Insert completed\n";
#ifdef ERASE_FILE
		for (auto &&idx : indexes) {
			stdt.erase(stdt.find(idx));
			t.remove(idx);
		}
#else
		for (auto &&idx : indexes) {
			for (auto iter = idx.begin(); iter != idx.end(); ++iter) {
				int flag = rand() % 5;
				if (flag)
					continue;
				t.remove(idx.begin(), iter + 1);

				for (; iter != idx.end(); ++iter) {
					auto nowidx = vector<int>(idx.begin(), iter + 1);
//					for (auto &&item : nowidx)
//						cout << item << ' ';
//					cout << endl;
					stdt.erase(stdt.find(nowidx));
				}
				break;
			}
		}
#endif
		cout << "removal completed\n";

		cout << "test_remove ";
		if (check_equal(t, stdt))
			cout << "AC" << endl;
		else
			cout << "WA" << endl;
	}

public:
	static ExtrieTest & instance() {
		static ExtrieTest in;
		return in;
	}

	void test() {
		test_inesrt();
		test_iterate();
		test_remove();
	}

private: // concurrency_test
	pair<vector<vector<int>>, vector<vector<int>>> // index, val
	get_something_to_insert(int sz = 10000, int len = 20) {
		set<vector<int>> vis;
		vector<vector<int>> indexes;
		vector<vector<int>> vals;
		for (int i = 0; i < sz; ++i) {
			auto index = get_index(len);
			bool flag = false;
			for (auto iter = index->begin() + 1; iter <= index->end(); ++iter) {
				vector<int> idx(index->begin(), iter);
				if (vis.find(idx) != vis.end()) {
					flag = true;
					break;
				}
			}
			if (flag) continue;
			vector<int> val;
			for (auto iter = index->begin() + 1; iter <= index->end(); ++iter) {
				vector<int> idx(index->begin(), iter);
				vis.insert(idx);
				val.emplace_back(rand() % Mod);
			}
			indexes.push_back(*index);
			vals.push_back(val);
		}
#ifdef PRINT_GET
		for (int i = 0; i < (int)indexes.size(); ++i) {
			cout << "idx: ";
			for (auto &&item : indexes[i])
				cout << item << ' ';
			cout << "\nval: ";
			for (auto &&item : vals[i])
				cout << item << ' ';
			cout << endl;
		}
#endif
		return make_pair(indexes, vals);
	}

	void con_test_insert() {
		cout << "start con_test_insert: \n";
		extrie<int, int> t;
		mapViI stdt;

		vector<vector<int>> indexes;
		vector<vector<int>> vals;
		tie(indexes, vals) = get_something_to_insert();

		// insert in stdt
		for (int i = 0; i < (int)indexes.size(); ++i) {
			for (int j = 0; j < (int)indexes[i].size(); ++j)
				stdt.insert(make_pair(
						vector<int>(&indexes[i][0], &indexes[i][j + 1]),
						vals[i][j]));
		}

		// insert in t
		auto ins_t = [&](int beg, int end) {
			for (; beg < end; ++beg) {
				for (int i = 0; i < (int)indexes[beg].size(); ++i)
					t.insert(vector<int>(&indexes[beg][0], &indexes[beg][i + 1]),
					         vals[beg][i]);
			}
		};

		int beg = 0;
		vector<thread> ts;
		int thread_num = 5/*+1*/;
		int chunk_size = (int)indexes.size() / thread_num;
		for (int i = 0; i < thread_num; ++i) {
			cout << "thread " << i << " is processing tasks " << beg << " to "
			     << beg + chunk_size << endl;
			ts.emplace_back(thread(ins_t, beg, beg + chunk_size));
			beg += chunk_size;
		}
		cout << "thread " << thread_num << " is processing tasks " << beg << " to "
			 << vals.size() << endl;
		ts.emplace_back(thread(ins_t, beg, (int)vals.size()));
		for (auto && item : ts)
			item.join();

		// = ?
		cout << "start check_equal" << endl << "con_insert_test ";
		if (check_equal(t, stdt))
			cout << "AC" << endl;
		else
			cout << "WA" << endl;
	}

	void con_test_at() {
		cout << "start con_test_at\n";
		extrie<int, int> t;
		map<vector<int>, int> stdt;
		for (int i = 0; i < 100; ++i)
			insert_something(t, &stdt);

		vector<vector<int>> indexes;
		for (auto &&item : stdt)
			indexes.push_back(item.first);

		auto at_t = [&](int beg, int end) {
			for (; beg < end; ++beg) {
				if (t[indexes[beg]] != stdt[indexes[beg]]) {
					cout << "WA" << endl;
				}
			}
		};

		int beg = 0;
		vector<thread> ts;
		int thread_num = 5/*+1*/;
		int chunk_size = (int)indexes.size() / thread_num;
		for (int i = 0; i < thread_num; ++i) {
			cout << "thread " << i << " is processing tasks " << beg << " to "
			     << beg + chunk_size << endl;
			ts.emplace_back(thread(at_t, beg, beg + chunk_size));
			beg += chunk_size;
		}
		cout << "thread " << thread_num << " is processing tasks " << beg << " to "
		     << indexes.size() << endl;
		ts.emplace_back(thread(at_t, beg, (int)indexes.size()));
		for (auto && item : ts)
			item.join();
		cout << "con_test_at AC" << endl;
	}

	void con_test_remove() {
		cout << "start con_test_remove: \n";
		extrie<int, int> t;
		mapViI stdt;
		vector<vector<int>> indexes;
		vector<vector<int>> vals;

		tie(indexes, vals) = get_something_to_insert();
		for (int i = 0; i < (int)indexes.size(); ++i) {
			for (int j = 0; j < (int)indexes[i].size(); ++j)
				stdt.insert(make_pair(
						vector<int>(&indexes[i][0], &indexes[i][j + 1]),
						vals[i][j]));
		}
		auto ins_t = [&](int beg, int end) {
			for (; beg < end; ++beg) {
				for (int i = 0; i < (int)indexes[beg].size(); ++i)
					t.insert(vector<int>(&indexes[beg][0], &indexes[beg][i + 1]),
					         vals[beg][i]);
			}
		};
		ins_t(0, (int)indexes.size());
		cout << "Insert completed\n";
		// todo
	}

public:
	void concurrency_test() {
		con_test_insert();
		cout << endl;
		con_test_at();
	}
};

#endif //AFS_EXTRIE_TEST_HPP
