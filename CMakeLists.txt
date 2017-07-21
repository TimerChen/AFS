cmake_minimum_required(VERSION 3.8)
project(AFS)

set(CMAKE_CXX_STANDARD 14)

set(SOURCE_FILES main.cpp src/extrie.hpp src/sugar.hpp)
add_executable(AFS ${SOURCE_FILES})