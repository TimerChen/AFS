//
// Created by Aaron Ren on 02/08/2017.
//

#ifndef AFS_MASTERTEST_H
#define AFS_MASTERTEST_H

#include "master.h"
#include "parser.hpp"
#include <gtest/gtest.h>
#include <service.hpp>
#include <fstream>
#include <tuple>
#include <string>
#include <vector>
#include <memory>
#include <algorithm>

using std::tuple;
using std::string;
using std::vector;
using std::cerr;
using std::endl;
using std::make_unique;
using std::unique_ptr;
using std::move;
using std::uint64_t;
using std::cout;

namespace AFS {

class masterTest : public ::testing::Test {
protected:
	GFSError ok{GFSErrorCode::OK, ""};
	GFSError no{GFSErrorCode::NoSuchFileDir, ""};
	GFSError al{GFSErrorCode::FileDirAlreadyExists, ""};

	LightDS::Service srv;
	Master m;

	masterTest() : srv("", std::cout), m(srv, "") {}

	void someMkdir() {
		/* NULL - a - b - c
		 *    \ _ b - c
		 */
		m.RPCMkdir("a");
		m.RPCMkdir("a/b");
		m.RPCMkdir("a/b/c");
		m.RPCMkdir("b");
		m.RPCMkdir("b/c");
	}

	// after someMkdir
	void someCreateFile() {
		/* NULL - a - b - c --- d
		 *    \ _ b - c    \ __ e
		 *             \_ d \ _ f
		 *
		 */
		m.RPCCreateFile("a/b/c/d");
		m.RPCCreateFile("a/b/c/e");
		m.RPCCreateFile("a/b/c/f");
		m.RPCCreateFile("b/c/d");
	}

	// after someCreateFile
	void someDelete() {
		m.RPCDeleteFile("a/b/c/d");
	}
};

TEST_F(masterTest, throwTest) {
	// throw
	ASSERT_NO_THROW(m.RPCMkdir("a"));
	ASSERT_NO_THROW(m.RPCMkdir("a/b"));
	ASSERT_NO_THROW(m.RPCMkdir("b/c"));
}

TEST_F(masterTest, Mkdir) {
	EXPECT_EQ(ok, m.RPCMkdir("a"));
	EXPECT_EQ(ok, m.RPCMkdir("a/b"));
	EXPECT_EQ(no, m.RPCMkdir("b/c"));
	EXPECT_EQ(al, m.RPCMkdir("a/b"));
	EXPECT_EQ(no, m.RPCMkdir("a/b/c/d"));
	EXPECT_EQ(ok, m.RPCMkdir("a/b/c"));
	EXPECT_EQ(ok, m.RPCMkdir("b"));
	EXPECT_EQ(ok, m.RPCMkdir("b/c"));
}

TEST_F(masterTest, createFile) {
	someMkdir();
	// todo maybe
}

TEST_F(masterTest, listFile) {
	someMkdir();
	someCreateFile();
	vector<string> ans = {"d", "e", "f"};
	EXPECT_EQ(ans, std::get<1>(m.RPCListFile("a/b/c")));
	ans = {"c"};
	EXPECT_EQ(ans, std::get<1>(m.RPCListFile("b")));
	ans = {"d"};
	EXPECT_EQ(ans, std::get<1>(m.RPCListFile("b/c")));
}

TEST_F(masterTest, deleteFile) {
	someMkdir();
	someCreateFile();
	EXPECT_EQ(ok, m.RPCDeleteFile("a/b/c/d"));
	vector<string> ans = {"e", "f"};
	EXPECT_EQ(ans, std::get<1>(m.RPCListFile("a/b/c")));
	EXPECT_EQ(no, m.RPCDeleteFile("a/b/c"));
}

TEST_F(masterTest, saveAndLoad) {
	someMkdir();
	someCreateFile();
	someDelete();

	std::ofstream fout("/Users/aaronren/Desktop/1/1.dat");
	std::ifstream fin("/Users/aaronren/Desktop/1/1.dat");

	m.write(fout);
	fout.close();
	m.read(fin);
	vector<string> ans = {"e", "f"};
	EXPECT_EQ(ans, std::get<1>(m.RPCListFile("a/b/c")));

	// todo: now, pfdm is ok
}

}


#endif //AFS_MASTERTEST_H
