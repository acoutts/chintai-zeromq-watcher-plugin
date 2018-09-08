# EOSIO Watcher Plugin + ZeroMQ
This is a modified version of the EOSIO Watcher Plugin (https://github.com/eosauthority/eosio-watcher-plugin) with the ZeroMQ Plugin (https://github.com/cc32d9/eos_zmq_plugin). It uses the approach in the watcher plugin where only transactions that make it into an accepted block are processed, but uses ZeroMQ to avoid http endpoint saturation when replaying the blockchain. Additionally since ZeroMQ push is a blocking operation, it is guaranteed that events (blocks) are processed in order at the work receiver endpoint performing the pull.

## Requirements
- You'll need to install `libzmq` for your system as well as `pkg-config`. Please see installation instructions here: http://zeromq.org/intro:get-the-software
- Additionally you'll need to install the C++ bindings for your system: https://github.com/zeromq/cppzmq

## Building the plugin [Install on your nodeos server]
1. Copy watcher_plugin folder to `<eosio-source-dir>/plugins/` You should now have `<eosio-source-dir>/plugins/watcher_plugin`
2. Add the following line to `<eosio-source-dir>/plugins/CMakeLists.txt` with other `add_subdirectory` items
  ```
  add_subdirectory(watcher_plugin)
  ```

3. Add the following line to the bottom of `<eosio-source-dir>/programs/nodeos/CMakeLists.txt`
  ```
  target_link_libraries( nodeos PRIVATE -Wl,${whole_archive_flag} watcher_plugin -Wl,${no_whole_archive_flag})
  ```

4. Install dependencies:
### macOS
  ```
  brew install pkg-config
  brew link pkg-config
  brew install zmq
  ```
  Download the C++ bindings for ZeroMQ by cloning this repository: `https://github.com/zeromq/cppzmq`, and copy `zmq.hpp` to `/usr/local/include` putting it next to `zmq.h`
  
4. Build and install nodeos with `eosio_build.sh` and `eosio_install.sh`. You could even just `cd <eosio-source-dir>/build` and then `sudo make install`

# How to setup on your nodeos

Enable this plugin using `--plugin` option to nodeos or in your config.ini. Use `nodeos --help` to see options used by this plugin.

# Configuration
Add the following to `config.ini` to enable the plugin:
```
#Enable watcher plugin
plugin = eosio::watcher_plugin

#Set account:action so eosauthority:spaceinvader or just eosauthority: for all actions on eosauthority
watch=chintaitest1:

#Age limit in seconds for blocks to send notifications. No age limit if set to negative. Used to prevent old actions from trigger HTTP request while on replay (seconds)
watch-age-limit = -1

#ZMQ sender socket binding
zmq-sender-bind = tcp://127.0.0.1:3001
```
