// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_BITMAPFASTALLOCATOR_H
#define CEPH_OS_BLUESTORE_BITMAPFASTALLOCATOR_H

#include <mutex>

#include "Allocator.h"
#include "os/bluestore/bluestore_types.h"
#include "fastbmap_allocator_impl.h"
#include "include/mempool.h"
#include "common/debug.h"

class BitmapAllocator : public Allocator,
  public AllocatorLevel02<AllocatorLevel01Loose> {
protected:
  int64_t allocate_raw(uint64_t want_size, 
    uint64_t alloc_unit,
    uint64_t max_alloc_size,
    int64_t hint,
    PExtentVector* extents) override;
  void release_raw(size_t count, const bluestore_pextent_t* to_release) {
    _free_l2(count, to_release);
  }
  uint64_t get_free_raw() const override {
    return get_available();
  }
public:
  BitmapAllocator(CephContext* _cct, int64_t capacity, int64_t alloc_unit,
                  bool with_cache, std::string_view name);
  ~BitmapAllocator() override
  {
  }

  const char* get_type() const override
  {
    return "bitmap";
  }

  void dump() override;
  void foreach(
    std::function<void(uint64_t offset, uint64_t length)> notify) override
  {
    foreach_internal(notify);
  }
  double get_fragmentation() override
  {
    return get_fragmentation_internal();
  }

  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;

  void shutdown() override;
};

#endif
