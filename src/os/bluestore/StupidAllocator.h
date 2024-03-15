// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_STUPIDALLOCATOR_H
#define CEPH_OS_BLUESTORE_STUPIDALLOCATOR_H

#include <mutex>

#include "Allocator.h"
#include "include/btree_map.h"
#include "include/interval_set.h"
#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"
#include "common/ceph_mutex.h"

class StupidAllocator : public Allocator {

  std::atomic<int64_t> num_free = 0;     ///< total bytes in freelist

  template <typename K, typename V> using allocator_t =
    mempool::bluestore_alloc::pool_allocator<std::pair<const K, V>>;
  template <typename K, typename V> using btree_map_t =
    btree::btree_map<K, V, std::less<K>, allocator_t<K, V>>;
  using interval_set_t = interval_set<uint64_t, btree_map_t>;
  std::vector<interval_set_t> free;  ///< leading-edge copy

  uint64_t last_alloc = 0;

  unsigned _choose_bin(uint64_t len);
  void _insert_free(uint64_t offset, uint64_t len);

  int64_t allocate_int(
    uint64_t want_size, uint64_t alloc_unit, int64_t hint,
    uint64_t* offset, uint32_t* length);

protected:
  int64_t allocate_raw(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, PExtentVector* extents) override;
  void release_raw(size_t count, const bluestore_pextent_t* to_release) override;
  uint64_t get_free_raw() const override {
    return num_free;
  }

public:
  StupidAllocator(CephContext* cct,
                  int64_t size,
                  int64_t block_size,
                  bool with_cache,
		  std::string_view name);
  ~StupidAllocator() override;
  const char* get_type() const override
  {
    return "stupid";
  }

  double get_fragmentation() override;

  void dump() override;
  void foreach(std::function<void(uint64_t offset, uint64_t length)> notify) override;

  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;

  void shutdown() override;
};

#endif
