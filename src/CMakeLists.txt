cmake_minimum_required(VERSION 3.8)
project(AFS)

set(CMAKE_CXX_STANDARD 14)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.64.0 REQUIRED COMPONENTS system thread)

if(Boost_FOUND)
    set(SOURCE_FILES
            main.cpp
            ## ryw begin 14
            components/extrie.hpp
            components/sugar.hpp
            filesystem/servers/master.cpp filesystem/servers/master.h
            filesystem/message_and_setting/setting.h
            filesystem/message_and_setting/data.cpp filesystem/message_and_setting/data.h
            filesystem/message_and_setting/rpc_structs.h
            filesystem/message_and_setting/log.cpp filesystem/message_and_setting/log.h
            tests/log_test.hpp








            ## ryw end 31
            ## cjx begin 32
            filesystem/server.h
            filesystem/chunkserver.h filesystem/chunkserver.cpp
            filesystem/chunk.h filesystem/chunk.cpp















            ## cjx end 51
			)
    include_directories(
            ## ryw begin 54
            ${CMAKE_SOURCE_DIR}
            ${Boost_INCLUDE_DIRS}
            ${CMAKE_SOURCE_DIR}/components
            ${CMAKE_SOURCE_DIR}/filesystem/message_and_setting
            ${CMAKE_SOURCE_DIR}/filesystem/servers
            ${CMAKE_SOURCE_DIR}/tests




            ## ryw end 65
            ## cjx begin 66






            # cjx end 73
             )
    add_executable(AFS ${SOURCE_FILES})
    target_link_libraries(AFS ${Boost_LIBRARIES})
endif()