/*
#include <iostream>

int main() {
    std::cout << "Hello, World!" << std::endl;
    return 0;
}
*/

#include "extrie_test.hpp"

int main() {
//	ExtrieTest::instance().test();
	ExtrieTest::instance().concurrency_test();
	return 0;
}