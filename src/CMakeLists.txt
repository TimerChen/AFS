cmake_minimum_required(VERSION 3.8)
project(AFS)

set(CMAKE_CXX_STANDARD 14)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.64.0 REQUIRED COMPONENTS system thread)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)


#SET(CMAKE_BUILD_TYPE "Debug")
#SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
#SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")


if(Boost_FOUND)
    set(SOURCE_FILES
            main.cpp
            ## ryw begin 14
            components/extrie.hpp
            components/sugar.hpp
            filesystem/master.cpp filesystem/master.h
            filesystem/message_and_setting/setting.h
            filesystem/message_and_setting/data.cpp filesystem/message_and_setting/data.h
            filesystem/message_and_setting/rpc_structs.h
            filesystem/message_and_setting/log.cpp filesystem/message_and_setting/log.h
            tests/log_test.hpp
            filesystem/common.h
            tests/extrie_test.hpp
            components/parser.hpp


            ## ryw end 31
            ## cjx begin 32
            filesystem/server.h
            filesystem/chunkserver.h filesystem/chunkserver.cpp
            filesystem/chunk.h filesystem/chunk.cpp





            ## ryw end
            ## cjx begin
            components/error.hpp
            components/message.hpp
            filesystem/server.h
            filesystem/chunkserver.h        filesystem/chunkserver.cpp
            filesystem/chunk.h              filesystem/chunk.cpp
            filesystem/client.h             filesystem/client.cpp

            LightDS/include/log.hpp
            LightDS/include/netclient.hpp
            LightDS/include/netmsg.hpp
            LightDS/include/netserver.hpp
            LightDS/include/pool.hpp
            LightDS/include/rpc.hpp
            LightDS/include/service.hpp

            JsonCPP/jsoncpp.cpp

            tests/LightDSTest/servera.h     tests/LightDSTest/servera.cpp
            tests/LightDSTest/serverb.h     tests/LightDSTest/serverb.cpp




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
            ${CMAKE_SOURCE_DIR}/filesystem
            ${CMAKE_SOURCE_DIR}/LightDS/include
            ${CMAKE_SOURCE_DIR}/JsonCPP/json





            # cjx end 73
             )
    add_executable(AFS ${SOURCE_FILES})
    target_link_libraries(AFS ${Boost_LIBRARIES})


    target_link_libraries(AFS ${CMAKE_THREAD_LIBS_INIT})
    set_property(TARGET AFS PROPERTY COMPILE_OPTIONS "-pthread")

endif()
