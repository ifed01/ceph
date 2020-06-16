set(kv_srcs
  KeyValueDB.cc
  MemDB.cc
  pmemkv.cc
  ceph_pmemkv.cc
  RocksDBStore.cc
  rocksdb_cache/ShardedCache.cc
  rocksdb_cache/BinnedLRUCache.cc)

if (WITH_LEVELDB)
  list(APPEND kv_srcs LevelDBStore.cc)
endif (WITH_LEVELDB)

add_library(kv STATIC ${kv_srcs}
  $<TARGET_OBJECTS:common_prioritycache_obj>)

target_link_libraries(kv ${LEVELDB_LIBRARIES}
  RocksDB::RocksDB
  pmemobj
  heap_profiler)
