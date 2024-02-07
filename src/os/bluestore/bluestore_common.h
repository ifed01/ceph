// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_COMMON_H
#define CEPH_OSD_BLUESTORE_COMMON_H

#include "include/intarith.h"
#include "include/ceph_assert.h"
#include "kv/KeyValueDB.h"

template <class Bitset, class Func>
void apply_for_bitset_range(uint64_t off,
  uint64_t len,
  uint64_t granularity,
  Bitset &bitset,
  Func f) {
  auto end = round_up_to(off + len, granularity) / granularity;
  ceph_assert(end <= bitset.size());
  uint64_t pos = off / granularity;
  while (pos < end) {
    f(pos, bitset);
    pos++;
  }
}

// merge operators

struct Int64ArrayMergeOperator : public KeyValueDB::MergeOperator {
  void merge_nonexistent(
    const char *rdata, size_t rlen, std::string *new_value) override {
    *new_value = std::string(rdata, rlen);
  }
  void merge(
    const char *ldata, size_t llen,
    const char *rdata, size_t rlen,
    std::string *new_value) override {
    ceph_assert(llen == rlen);
    ceph_assert((rlen % 8) == 0);
    new_value->resize(rlen);
    const ceph_le64* lv = (const ceph_le64*)ldata;
    const ceph_le64* rv = (const ceph_le64*)rdata;
    ceph_le64* nv = &(ceph_le64&)new_value->at(0);
    for (size_t i = 0; i < rlen >> 3; ++i) {
      nv[i] = lv[i] + rv[i];
    }
  }
  // We use each operator name and each prefix to construct the
  // overall RocksDB operator name for consistency check at open time.
  const char *name() const override {
    return "int64_array";
  }
};

template <class Dev, class Collection>
bool try_discard(Dev* bdev,
                 const Collection& release_set,
                 bool async)
{
  ceph_assert(bdev);
  size_t i = 0;
  size_t size = release_set.size();
  bool b = bdev->try_discard(
    size,
    [&](std::pair<uint64_t, uint64_t>* ret) {
      ceph_assert(i < size);
      ceph_assert(ret);
      const auto& p = release_set[i++];
      ret->first = p.offset;
      ret->second = p.length;
    });
  return b;
}

// write a label in the first block.  always use this size.  note that
// bluefs makes a matching assumption about the location of its
// superblock (always the second block of the device).
#define BDEV_LABEL_BLOCK_SIZE  4096

// reserved for standalone DB volume:
// label (4k) + bluefs super (4k), which means we start at 8k.
#define DB_SUPER_RESERVED  (BDEV_LABEL_BLOCK_SIZE + 4096)


#endif
