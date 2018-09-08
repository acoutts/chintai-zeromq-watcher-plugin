## load in pkg-config support
find_package(PkgConfig REQUIRED)
## use pkg-config to get hints for 0mq locations
pkg_check_modules(PC_ZeroMQ REQUIRED libzmq)

## use the hint from above to find where 'zmq.hpp' is located
find_path(ZeroMQ_INCLUDE_DIR
  NAMES zmq.hpp
  PATHS ${PC_ZeroMQ_INCLUDE_DIRS}
  )

## use the hint from about to find the location of libzmq
find_library(ZeroMQ_LIBRARY
  NAMES zmq
  PATHS ${PC_ZeroMQ_LIBRARY_DIRS}
  )

file(GLOB HEADERS "include/eosio/watcher_plugin/*.hpp")
add_library( watcher_plugin
             watcher_plugin.cpp
             ${HEADERS} include/eosio/watcher_plugin/watcher_plugin.hpp )

target_link_libraries( watcher_plugin chain_plugin eosio_chain appbase fc ${ZeroMQ_LIBRARY} )
target_include_directories( watcher_plugin PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )
