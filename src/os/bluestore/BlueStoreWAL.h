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
  static const size_t MAX_TXCS_PER_OP = 16;
  struct Op {
    uint64_t op_seqno = 0;
    uint64_t txc_seqno = 0;
    uint64_t wiping_pages = 0;
    uint64_t prev_page_seqno = 0;
    bool running = false;
    size_t num_txcs = 0;
    BlueStore::TransContext* txc[MAX_TXCS_PER_OP] = {nullptr};

    Op() {}
    Op(const Op& from) : op_seqno(from.op_seqno),
      txc_seqno(from.txc_seqno),
      wiping_pages(from.wiping_pages),
      prev_page_seqno(from.prev_page_seqno),
      running(from.running),
      num_txcs(from.num_txcs) {
      for (size_t i = 0; i < num_txcs; i++) {
        txc[i] = from.txc[i];
      }
    }
    void run(uint64_t wp, uint64_t prev, BlueStore::TransContext* _txc) {
      ceph_assert(!running);
      ceph_assert(num_txcs == 0);
      wiping_pages = wp;
      prev_page_seqno = prev;
      txc[num_txcs++] = _txc;
      running = true;
    }
    void run_more(BlueStore::TransContext* _txc) {
      ceph_assert(running);
      ceph_assert(num_txcs != 0);
      ceph_assert (num_txcs < MAX_TXCS_PER_OP);
      txc[num_txcs++] = _txc;
    }
    void reset() {
      op_seqno = 0;
      txc_seqno = 0;
      wiping_pages = 0;
      prev_page_seqno = 0;
      running = false;
      num_txcs = 0;
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

  struct chest_entry_t {
    size_t payload_len = 0;
    size_t entry_count =0;
    std::array<BlueStore::TransContext*, MAX_TXCS_PER_OP> txcs;
    bool maybe_add(BlueStore::TransContext* txc, size_t len, size_t max_len) {
      // a single txc longer than max_size is allowed in a free slot
      bool ret = entry_count < txcs.size() &&
        (payload_len == 0 || (payload_len + len <= max_len));
      if (ret) {
        txcs[entry_count++] = txc;
        payload_len += len;
      }
      return ret;
    }
    BlueStore::TransContext* maybe_get(size_t pos) {
      return pos < entry_count ? txcs[pos]: nullptr;
    }
    void reset() {
      payload_len = 0;
      entry_count = 0;
    }
  };

  class chest_t {
    // total number of entries in the chest
    std::atomic<size_t> total_entry_count = 0;

    // amount of valid array elements in entries array
    size_t row_count = 0;

    // keep one txc less than Op can fit
    std::array<chest_entry_t, MAX_TXCS_PER_OP> entries;
  public:
    size_t get_payload_len(size_t alignment) const {
      size_t res = 0;
      for (size_t row = 0; row < row_count; row++) {
        res += p2roundup(entries[row].payload_len, alignment);
      }
      return res;
    }
    size_t get_entry_count() const {
      return total_entry_count;
    }
    bool add(BlueStore::TransContext* txc, size_t len,
      size_t max_len, bool permit_last) {
      bool ret = false;
      size_t max_entries = entries.size();
      if (!permit_last) {
        --max_entries;
      }
      if (total_entry_count < max_entries) {
        for(size_t i = 0; i < row_count; i++) {
          if (entries[i].maybe_add(txc, len, max_len)) {
            total_entry_count++;
            return true;
          }
        }
        ceph_assert(row_count < entries.size());
        entries[row_count].reset();
        ret = entries[row_count].maybe_add(txc, len, max_len);
        if (ret) {
          total_entry_count++;
          row_count++;
        }
      }
      return ret;
    }
    BlueStore::TransContext* get_next_if_any(size_t* ret_pos) {
      BlueStore::TransContext* ret = nullptr;

      size_t row = *ret_pos / entries.size();
      size_t pos = *ret_pos %  entries.size();
      while (row < row_count && !ret) {
        ret = entries[row].maybe_get(pos);
        if (ret) {
          ++pos;
        } else {
          ++row;
          pos = 0;
        }
      }

      *ret_pos = row * entries.size() + pos;
      return ret;
    }
    void claim(chest_t& from) {
      ceph_assert(row_count == 0);
      row_count = from.row_count;
      entries.swap(from.entries);

      from.row_count = 0;
      total_entry_count = from.total_entry_count.fetch_and(0);
    }
  } gate_chest;
  ceph::mutex gate_lock = ceph::make_mutex("BlueStoreWAL::lock_gateway");

  ceph::mutex lock = ceph::make_mutex("BlueStoreWAL::lock");
  ceph::condition_variable flush_cond;   ///< wait here for transactions in WAL to commit
  uint64_t avail = 0;
  uint64_t page_seqno = 0;
  uint64_t curpage_pos = 0;
  uint64_t cur_op_seqno = 0;
  uint64_t cur_txc_seqno = 0;
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

  void _notify_txc(uint64_t prev_page_seqno,
                   BlueStore::TransContext* txc,
                   txc_completion_fn on_finish);
  void _process_finished(txc_completion_fn on_finish);
  Op* _log(BlueStore::TransContext* txc);
  void _prepare_submit_txc(bluewal_head_t& header,
                           uint64_t txc_seqno,
                           BlueStore::TransContext* txc,
                           bufferlist::page_aligned_appender& appender,
                           bufferlist& bl);

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

  void shutdown(KeyValueDB& db);

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
