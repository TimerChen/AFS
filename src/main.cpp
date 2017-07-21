#include <iostream>
#include "extrie.hpp"
#include <map>
#include <memory>

class TestExtrie {
private:
    static auto get_index(int len = 5, int bound = 5) {
        auto result = std::make_unique<std::vector<int>>();
        for (int i = 0; i < len; ++i)
            result->emplace_back(rand() % bound);
        return result;
    }
    constexpr static int Mod = 127;

    static AFS::extrie<int, int> t1;
    static std::map<std::vector<int>, int> t2;
    static std::vector<std::vector<int>> idxs;

    static void testInsert() {
        std::cout << "TestInsert starts: " << std::endl;
        for (int i = 0; i < 100; ++i) {
            auto idx = get_index();
            std::cout << "idx: ";
            for (auto &&item : *idx) {
                std::cout << item << ' ';
            }
            std::cout << std::endl;
            int val = rand() % Mod;
            std::cout << "value: " << val << std::endl;

            std::vector<int> tmpidx;
            for (auto &&item : *idx) {
                tmpidx.push_back(item);
                t1.insert(tmpidx, val);
                bool flag = t2.insert(std::make_pair(tmpidx, val)).second;
                if (flag)
                    idxs.emplace_back(tmpidx);
            }
        }
        std::cout << "Start checking" << std::endl;
        for (auto &&idx : idxs) {
            auto v1 = t1[idx];
            auto v2 = t2.find(idx);
            if (v1 != v2->second) {
                std::cout << "WA" << std::endl;
                throw ;
            }
        }
        std::cout << "TestInsert AC" << std::endl;
    }
    static void testRemove() {
        std::cout << "TestRemove start" << std::endl;
        for (int i = 0; i < (int)idxs.size() / 2; ++i) {
            t1.remove(idxs[i]);
            t2.erase(t2.find(idxs[i]));
            if (t1.check(idxs[i])) {
                std::cout << "TestRemove WA" << std::endl;
                return;
            }
        }
        for (int i = (int)idxs.size() / 2; i < (int)idxs.size(); ++i) {
            auto v1 = t1[idxs[i]];
            auto v2 = t2.find(idxs[i]);
            if (v1 != v2->second) {
                std::cout << "TestRemove WA" << std::endl;
                throw ;
            }
        }
        std::cout << "TestRemove AC" << std::endl;
    }
    static void testCopyMove() {
        std::cout << "testCopy ";
        auto t3 = t1;
        for (auto &&item : t2) {
            if (t3[item.first] != item.second) {
                std::cout << "WA" << std::endl;
                throw ;
            }
        }
        std::cout << "AC" << std::endl;

        std::cout << "testMove ";
        auto t4 = std::move(t3);
        for (auto &&item : t2) {
            if (t4[item.first] != item.second) {
                std::cout << "WA" << std::endl;
                throw ;
            }
        }
        std::cout << "AC" << std::endl;
    };

public:
    static void test() {
        testInsert();
        testRemove();
        testCopyMove();
    }
};
AFS::extrie<int, int> TestExtrie::t1;
std::map<std::vector<int>, int> TestExtrie::t2;
std::vector<std::vector<int>> TestExtrie::idxs;


int main() {
//    std::cin.get();
    srand((unsigned int)time(0));
    TestExtrie::test();



    return 0;
}