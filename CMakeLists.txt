cmake_minimum_required(VERSION 3.8)
project(AFS)

set(CMAKE_CXX_STANDARD 14)

set(SOURCE_FILES
        main.cpp
        src/components/extrie.hpp
        src/components/sugar.hpp src/Filesystem/master.cpp src/Filesystem/master.h src/Filesystem/setting.h)
add_executable(AFS ${SOURCE_FILES})