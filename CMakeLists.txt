cmake_minimum_required(VERSION 3.22)
set(SC_MODULE cassandra)
project(simply-cpp-${SC_MODULE} HOMEPAGE_URL "https://github.com/roelofrossouw/simply-cpp-cassandra" LANGUAGES CXX)
include(cmake/sc_bootstrap.cmake)

option(SC_CASSANDRA_BUILD_TESTS "Build simply-cpp-cassandra tests" ${PROJECT_IS_TOP_LEVEL})

get_sc_version()
message(STATUS "SimplyCPP ${SC_MODULE} Version: ${SC_VERSION}")

# sc-core: ip_endpoints, and params for the demo. No other dependency: the CQL native protocol
# (v4) is spoken directly over a socket.
find_or_install_package(sc-core simply-cpp-dev simply-cpp 1.20.0)

set(SOURCE_OBJECTS include/cassandra.h)
set(SOURCE_LIBRARIES)
set(SOURCE_LINK_LIBRARIES)

add_sc_object(cassandra
        PUBLIC_LINK_LIBRARIES sc::sc-core)

add_sc_libraries(DESCRIPTION "simply-cpp Cassandra (CQL) client")
target_compile_features(sc-cassandra PUBLIC cxx_std_20)
target_compile_features(sc-cassandra-shared PUBLIC cxx_std_20)

install_sc_module(PATH_VARS CMAKE_INSTALL_INCLUDEDIR)
install(FILES cmake/SimplyCppFunctions.cmake cmake/sc_bootstrap.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sc-cassandra
        COMPONENT development)

if (SC_CASSANDRA_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests sc-cassandra-test)
endif ()

add_sc_example(sc-cassandra-demo)

package_sc_module(DEV_DEPENDS "simply-cpp-dev (>= 1.20.0~)")
