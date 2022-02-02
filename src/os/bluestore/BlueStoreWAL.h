// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_WAL_H
#define CEPH_OSD_BLUESTORE_WAL_H

#include <atomic>
#include <vector>
#include <functional>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/denc.h"
#include "include/intarith.h"
#include "include/uuid.h"

#include "common/ceph_mutex.h"
#include "common/perf_counters.h"

#include "kv/KeyValueDB.h"

class BlockDevice;

struct bluewal_head_t {
  ///
  /// Each header embeds optional page part which is valid
  /// (and hence indicates the beginning of the next page)
  /// if page_count is greater than 0
  ///
  uint64_t page_seq = 0;
  uint32_t page_count = 0;

  uint64_t seq = 0;
  uint32_t len = 0;
  uuid_d uuid;
  uint32_t csum = 0;

  DENC(bluewal_head_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.page_seq, p);
    denc(v.page_count, p);
    denc(v.seq, p);
    denc(v.len, p);
    denc(v.uuid, p);
    denc(v.csum, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(bluewal_head_t)

class BluestoreWAL {
protected:
  struct Op {
    uint64_t transact_seqno = 0;
    uint64_t wiping_pages = 0;
    uint64_t prev_page_seqno = 0;
    BlueStore::TransContext* txc = nullptr;
    bool running = false;

    void run(uint64_t wp, uint64_t prev, BlueStore::TransContext* _txc) {
      wiping_pages = wp;
      prev_page_seqno = prev;
      txc = _txc;
      running = true;
    }
  };

  typedef std::function<void (BlueStore::TransContext*)> txc_completion_fn;

protected:

  CephContext* cct = nullptr;
  BlockDevice* bdev = nullptr;

  uuid_d uuid;
  uint64_t total = 0;
  size_t page_size = 0;
  size_t block_size = 0;

  size_t head_size = 0;

  std::vector<uint64_t> page_offsets;

  std::atomic<uint64_t> last_submitted_page_seqno = 0;// last page seq got DB submit confirmation
  std::atomic<uint64_t> num_pending_free = 0;         // amount of ops waiting for free pages
  std::atomic<uint64_t> num_knocking = 0;

  struct chest_t {
    size_t l1 = 0;
    size_t l2 = 0;

    bool full() const {
	return  l1 !=0 && l2 != 0;
    }
    void reset() {
      *this = chest_t();
    }
  };
  ceph::mutex gate_lock = ceph::make_mutex("BlueStoreWAL::lock_gateway");
  std::array<chest_t, 8> gate_chest;

  ceph::mutex lock = ceph::make_mutex("BlueStoreWAL::lock");
  ceph::condition_variable flush_cond;   ///< wait here for transactions in WAL to commit
  uint64_t avail = 0;
  uint64_t page_seqno = 0;
  uint64_t curpage_pos = 0;
  uint64_t transact_seqno = 0;
  uint64_t last_committed_page_seqno = 0;             // last page seq committed to DB
  uint64_t last_wiping_page_seqno = 0; 		      // last page seq wiping has been triggered for
  uint64_t last_wiped_page_seqno = 0;		      // last page seq which has been wiped

  std::vector<Op> ops;

  uint64_t min_pending_io_seqno = 1;

  PerfCounters* logger = nullptr;

protected:
  bool init_op(size_t need, uint64_t* _need_pages, Op**);

  inline size_t get_total_pages() const {
    return page_offsets.size();
  }
  inline size_t get_page_idx(uint64_t seqno) const {
    ceph_assert(seqno);
    return ((seqno - 1)  % get_total_pages());
  }
  size_t wipe_pages(IOContext* ioc);

protected:
  // following funcs made virtual to be able to make UT stubs for them
  virtual void aio_write(uint64_t off,
			 bufferlist& bl,
			 IOContext* ioc,
			 bool buffered);

  void _notify_txc(Op& op, txc_completion_fn on_finish);
  void _aio_finish(Op& op, txc_completion_fn on_finish);
  Op* _log(BlueStore::TransContext* txc);

public:
  static const size_t DEF_PAGE_SIZE = 1ull << 24; // 16MB
  static const size_t DEF_BLOCK_SIZE = 4096;

  BluestoreWAL(
    CephContext* _cct,
    BlockDevice* _bdev,
    const uuid_d& _uuid,
    uint64_t psize = DEF_PAGE_SIZE,
    uint64_t bsize = DEF_BLOCK_SIZE);

  virtual ~BluestoreWAL() {
    if (logger) {
      cct->get_perfcounters_collection()->remove(logger);
      delete logger;
    }
  }
  void init_add_pages(uint64_t offset, uint64_t len);

  void log(BlueStore::TransContext* txc);
  void submitted(uint64_t outdated_page_seqno, KeyValueDB& db);

  void aio_submit(IOContext* ioc) {
    bdev->aio_submit(ioc);
  }
  void aio_finish(BlueStore::TransContext* txc, txc_completion_fn on_finish);

  void shutdown();

  uint64_t get_total() const {
    return total;
  }
  uint64_t get_avail() const {
    return avail;
  }
  uint64_t get_page_size() const {
    return page_size;
  }
  uint64_t get_header_size() const {
    return head_size;
  }
};

#endif
