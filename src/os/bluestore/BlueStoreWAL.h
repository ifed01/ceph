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
//#include <mutex>
#include <vector>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/functional/hash.hpp>

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
class BlueStore;

struct bluewal_page_head_t {
  uint64_t seq = 0;
  uuid_d uuid;
  uint32_t following_pages = 0;

  DENC(bluewal_page_head_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.seq, p);
    denc(v.uuid, p);
    denc(v.following_pages, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(bluewal_page_head_t)

struct bluewal_transact_head_t {
  uint64_t seq = 0;
  uint32_t len = 0;
  uuid_d uuid;
  uint32_t csum = 0;
    
  DENC(bluewal_transact_head_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.seq, p);
    denc(v.len, p);
    denc(v.uuid, p);
    denc(v.csum, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(bluewal_transact_head_t)

class BluestoreWAL {
protected:
  typedef boost::intrusive::unordered_set_member_hook<> unordered_set_hook;
  struct Op : public BlueStore::AioContext {
    BluestoreWAL* wal = nullptr;
    IOContext ioc;
    uint64_t transact_seqno = 0;
    uint64_t wiping_pages = 0;
    uint64_t prev_page_seqno = 0;
    void* txc = nullptr;
    bool running = true;

    unordered_set_hook member_hook_; //member hook

    Op(BluestoreWAL* _wal,
      CephContext* cct,
      uint64_t _seq,
      uint64_t wp,
      void* _txc)
      : wal(_wal),
      ioc(cct, this),
      transact_seqno(_seq),
      wiping_pages(wp),
      txc(_txc) {
    }
    // dummy ctor for lookups
    Op(uint64_t _seq) : ioc(nullptr, nullptr), transact_seqno(_seq) {
    }
    ~Op() override{
    }
    friend std::size_t hash_value(const Op& op) {
      return std::size_t(op.transact_seqno);
    }
    friend bool operator==(const Op& left, const Op& right) {
      return left.transact_seqno == right.transact_seqno;
    }

  protected:
    void aio_finish(BlueStore* store) override;
  };

protected:
  static const size_t MAX_BUCKETS = 8192;

  CephContext* cct = nullptr;
  BlockDevice* bdev = nullptr;

  uuid_d uuid;
  uint64_t total = 0;
  size_t page_size = 0;
  size_t block_size = 0;

  size_t phead_size = 0;
  size_t thead_size = 0;
  
  std::vector<uint64_t> page_offsets;

  std::atomic<uint64_t> last_submitted_page_seqno = 0;// last page seq got DB submit confirmation
  std::atomic<uint64_t> num_pending_free = 0;         // amount of ops waiting for free pages

  ceph::mutex lock = ceph::make_mutex("BlueStoreWAL::lock");
  ceph::condition_variable flush_cond;   ///< wait here for transactions in WAL to commit
  uint64_t avail = 0;
  uint64_t page_seqno = 0;
  uint64_t curpage_pos = 0;
  uint64_t transact_seqno = 0;
  uint64_t last_committed_page_seqno = 0;             // last page seq committed to DB
  uint64_t last_wiping_page_seqno = 0; 		      // last page seq wiping has been triggered for
  uint64_t last_wiped_page_seqno = 0;		      // last page seq which has been wiped

  struct DeleteDisposer {
    void operator()(Op* o) { delete o; }
  };

  typedef 
    boost::intrusive::member_hook<Op, unordered_set_hook, &Op::member_hook_>
      MemberOption;
  using pending_transactions_t = boost::intrusive::unordered_set<Op, MemberOption>;
  pending_transactions_t::bucket_type pt_buckets[MAX_BUCKETS];
  pending_transactions_t pending_transactions;

  uint64_t min_pending_io_seqno = 1;
  uint64_t max_pending_io_seqno = 0;

  PerfCounters* logger = nullptr;

protected:
  void _pad_bl(bufferlist& bl, size_t pad_size);
  bool get_write_pos(size_t need, uint64_t* _need_pages);

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
  virtual void notify_store(BlueStore* store, uint64_t seqno, void* txc);
  virtual void aio_write(uint64_t off,
			 bufferlist& bl,
			 IOContext* ioc,
			 bool buffered);
  virtual void aio_submit(IOContext* ioc);

  void aio_finish(BlueStore* store, Op& op);

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
  
  int submit(void* txc, bufferlist& txc_payload);
  void submitted(uint64_t outdated_page_seqno, KeyValueDB& db);

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
  uint64_t get_page_header_size() const {
    return phead_size;
  }
  uint64_t get_transact_header_size() const {
    return thead_size;
  }
};

#endif
