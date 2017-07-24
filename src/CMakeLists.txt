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
			## ryw begin
			components/extrie.hpp
			components/sugar.hpp
			filesystem/master.cpp filesystem/master.h
			filesystem/setting.h












			## ryw end
			## cjx begin
                        components/error.hpp
                        components/message.hpp
			filesystem/server.h
                        filesystem/chunkserver.h    filesystem/chunkserver.cpp
                        filesystem/chunk.h          filesystem/chunk.cpp
                        filesystem/client.h         filesystem/client.cpp

















			## cjx end
			)
	include_directories(
			## ryw begin
			${CMAKE_SOURCE_DIR}
			${Boost_INCLUDE_DIRS}
			${CMAKE_SOURCE_DIR}/components
			${CMAKE_SOURCE_DIR}/filesystem







			## ryw end
			## cjx begin










			# cjx end
			 )
	add_executable(AFS ${SOURCE_FILES})
	target_link_libraries(AFS ${Boost_LIBRARIES})
endif()
