cmake_minimum_required(VERSION 3.6)
project(app_gw)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        app_gw.cpp
        app_gw.h)

include_directories(
        ../../../common_library/include
        ../../include
        ../../include/E15_inc
        ../../include/strategy_inc
        ../include
)

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -rdynamic \
-Wl,--unresolved-symbols=ignore-in-shared-libs")

link_directories(
        ../../../common_library/lib64
        ../../so/E15_so
)

set(EXECUTABLE_OUTPUT_PATH ../../runtime/app_gw)

add_executable(app_gw ${SOURCE_FILES})

target_link_libraries(app_gw
        kbase
        libE15_c_objectD.so
        libE15_cxx_objectD.so)