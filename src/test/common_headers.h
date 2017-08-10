#ifndef GFS_TEST_COMMON_HEADERS_H
#define GFS_TEST_COMMON_HEADERS_H

#include <cassert>

#include <iostream>
#include <fstream>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/categories.hpp>

#include <boost/format.hpp>

#include <boost/filesystem.hpp>

#include <thread>
#include <future>
#include <mutex>
#include <atomic>

#include <deque>
#include <vector>
#include <string>

#include <memory>

#include <chrono>

#include <random>

#include <stdexcept>

#include <pipeservice.hpp>
#include <service.hpp>
#include <master.h>
#include <chunkserver.h>
#include <client.h>

#include "pipestream.hpp"

#endif