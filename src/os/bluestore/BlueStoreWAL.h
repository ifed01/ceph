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
class BlueWALContext;
class BlueWALContextSync;

struct bluewal_head_t {
  uuid_d uuid;
  ///
  /// Each header embeds optional page part which is valid
  /// if page_count is greater than 0
  /// (and hence indicates the beginning of the next page)
  ///
  uint64_t page_seq = 0;
  uint32_t page_count = 0;

  uint64_t seq = 0;
  uint32_t len = 0;
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

class BluestoreWAL;
class WALAsyncOpThread : public Thread {
  std::atomic<bool> do_stop = false;
  ceph::mutex lock = ceph::make_mutex("WALAsyncOpThread::lock");
  ceph::condition_variable cond;

  void *entry() override;
protected:
  BluestoreWAL& wal;
  virtual void do_op();
public:
  explicit WALAsyncOpThread(BluestoreWAL& _wal) : wal(_wal) {}

  void trigger_op() {
    std::unique_lock l(lock);
    cond.notify_all();
  }
  void stop() {
    {
      std::unique_lock l(lock);
      do_stop = true;
      cond.notify_all();
    }
    join();
    do_stop = false;
  }
};

class BluestoreWAL {
protected:
  static const size_t MAX_TXCS_PER_OP = 16;
  struct Op {
    uint64_t op_seqno = 0;
    uint64_t txc_seqno = 0;
    uint64_t page_seqno = 0;
    uint64_t wiping_pages = 0;
    bool running = false;
    size_t num_txcs = 0;
    mono_clock::time_point io_start;
    mono_clock::time_point birth_time;

    BlueWALContext* txc[MAX_TXCS_PER_OP] = {nullptr};

    void run(uint64_t wp, uint64_t pseq, BlueWALContext* _txc) {
      ceph_assert(!running);
      ceph_assert(num_txcs == 0);
      wiping_pages = wp;
      page_seqno = pseq;
      txc[num_txcs++] = _txc;
      running = true;
    }
    inline void maybe_update_page_seqno(uint64_t pseq) {
      page_seqno = pseq;
    }
    void run_more(BlueWALContext* _txc) {
      ceph_assert(running);
      ceph_assert(num_txcs != 0);
      ceph_assert (num_txcs < MAX_TXCS_PER_OP);
      txc[num_txcs++] = _txc;
    }
    void reset() {
      op_seqno = 0;
      txc_seqno = 0;
      wiping_pages = 0;
      page_seqno = 0;
      running = false;
      num_txcs = 0;
    }
    bool empty() const {
      return num_txcs == 0;
    }
  };

  typedef std::unique_lock<ceph::mutex> wal_unique_lock_t;

protected:

  CephContext* cct = nullptr;
  BlockDevice* bdev = nullptr;
  KeyValueDB* db = nullptr;
  WALAsyncOpThread flush_thread;

  uuid_d uuid;
  uint64_t total = 0;
  size_t flush_threshold = 0; // min amount of submitted pages to flush DB
  size_t page_size = 0;
  size_t block_size = 0;

  size_t head_size = 0;

  std::vector<uint64_t> page_offsets;

  // amount of ops waiting for avail pages to proceed
  std::atomic<uint64_t> num_queued = 0;
  std::atomic<uint64_t> num_knocking = 0;

  struct chest_entry_t {
    size_t payload_len = 0;
    size_t entry_count =0;
    std::array<BlueWALContext*, MAX_TXCS_PER_OP> txcs;
    std::array<size_t, MAX_TXCS_PER_OP> txc_sizes;
    bool maybe_add(BlueWALContext* txc,
                   size_t len,
                   size_t h_len,
                   size_t b_len,
                   size_t p_len) {
      // a single txc longer than max_size is allowed in a free slot
      bool ret = entry_count < txcs.size();
      if (ret) {
        if(payload_len + h_len + len <= b_len) {
          payload_len += h_len + len;
          txc_sizes[entry_count] = h_len + len;
          txcs[entry_count++] = txc;
        } else if (payload_len == 0) {
          if (h_len + len <= p_len) {
            payload_len = h_len + len;
            txc_sizes[entry_count] = payload_len;
            txcs[entry_count++] = txc;
          } else {
            auto tail = len % (p_len - h_len);
            auto p_count = len / (p_len - h_len);
            payload_len = p_count * p_len + (tail ? h_len + tail : 0);
            txc_sizes[entry_count] = payload_len;
            txcs[entry_count++] = txc;
          }
        } else {
          ret = false;
        }
      }
      return ret;
    }
    BlueWALContext* maybe_get(size_t pos) {
      return pos < entry_count ? txcs[pos]: nullptr;
    }
    size_t maybe_get_size(size_t pos) {
      return pos < entry_count ? txc_sizes[pos]: 0;
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
    void foreach_row(std::function<void (size_t)> fn) const {
      size_t row = 0;
      size_t res;
      while (row < row_count &&
              (res = entries[row].payload_len)) {
        fn(res);
        row++;
      }
    }
    size_t get_entry_count() const {
      return total_entry_count;
    }
    bool add(BlueWALContext* txc,
      size_t len,
      size_t h_len,
      size_t b_len,
      size_t p_len,
      bool permit_last) {
      bool ret = false;
      size_t max_entries = entries.size();
      if (!permit_last) {
        --max_entries;
      }
      if (total_entry_count < max_entries) {
        for(size_t i = 0; i < row_count; i++) {
          if (entries[i].maybe_add(txc, len, h_len, b_len, p_len)) {
            total_entry_count++;
            return true;
          }
        }
        ceph_assert(row_count < entries.size());
        entries[row_count].reset();
        ret = entries[row_count].maybe_add(txc, len, h_len, b_len, p_len);
        if (ret) {
          total_entry_count++;
          row_count++;
        }
      }
      return ret;
    }
    BlueWALContext* get_next_if_any(size_t* ret_pos, size_t* ret_size) {
      BlueWALContext* ret = nullptr;

      size_t row = *ret_pos / entries.size();
      size_t pos = *ret_pos %  entries.size();
      while (row < row_count && !ret) {
        ret = entries[row].maybe_get(pos);
        if (ret) {
          *ret_size = entries[row].maybe_get_size(pos);
          ceph_assert(*ret_size);
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
  uint64_t page_seqno = 0;                            // current page
  uint64_t curpage_pos = 0;                           // pos within current page
  uint64_t cur_txc_seqno = 0;
  std::atomic<uint64_t> last_submitted_page_seqno = 0;// last page seq got DB submit confirmation
  std::atomic<uint64_t> last_committed_page_seqno = 0;// last page seq committed to DB
  uint64_t last_wiping_page_seqno = 0; 		      // last page seq wiping has been triggered for
  uint64_t last_wiped_page_seqno = 0;		      // last page seq which has been wiped

  uint64_t cur_op_seqno = 0;
  std::vector<Op> ops;

  uint64_t min_pending_io_seqno = 1;

  std::atomic<size_t> future_ops = 0;// amount of future wal ops advertised

  PerfCounters* logger = nullptr;

  bool sync_flush = false;
protected:
  void assess_payload(const chest_t& chest, size_t* _need_pages);
  bool init_op(const chest_t& chest, size_t* _need_pages, Op**);

  inline size_t get_total_pages() const {
    return page_offsets.size();
  }
  inline size_t get_page_idx(uint64_t seqno) const {
    ceph_assert(seqno);
    return ((seqno - 1)  % get_total_pages());
  }
  // stages wiping for any submitted page
  size_t wipe_pages(IOContext* ioc);
  // stages wiping for any submitted page
  // and applies it
  void wipe_pages();

protected:
  // made virtual to be able to make UT stubs if needed
  virtual void aio_write(uint64_t off,
			 bufferlist& bl,
			 IOContext* ioc,
			 bool buffered);
  virtual int read(uint64_t off,
                   uint64_t len,
		   bufferlist* bl,
		   IOContext* ioc,
		   bool buffered);

  virtual void do_flush_db();

  void _finish_op(Op& op, bool deep);
  Op* _log(BlueWALContext* txc, bool force);
  void _prepare_txc_submit(bluewal_head_t& header,
                           BlueWALContext* txc,
                           bufferlist::page_aligned_appender& appender,
                           bufferlist& bl);
  void _submit_huge_txc(bluewal_head_t& header,
                            IOContext* anchor_ioc,
                            BlueWALContext* txc,
                            size_t txc_size0);

  void _maybe_write_unlock(IOContext* anchor_ioc,
                           uint64_t _offs,
                           bufferlist::page_aligned_appender& appender,
                           bufferlist& _bl,
                           wal_unique_lock_t* lck);
  int _read_page_header(uint64_t offset,
                        uint64_t expected_page_no,
                        uint64_t page_count,
                        bluewal_head_t* header);
public:
  static const size_t DEF_PAGE_SIZE = 1ull << 24; // 16MB
  static const size_t DEF_BLOCK_SIZE = 4096;

  BluestoreWAL(
    CephContext* _cct,
    BlockDevice* _bdev,
    KeyValueDB* _db,
    const uuid_d& _uuid,
    uint64_t fsize = DEF_PAGE_SIZE,
    uint64_t psize = DEF_PAGE_SIZE,
    uint64_t bsize = DEF_BLOCK_SIZE);

  virtual ~BluestoreWAL() {
    if (logger) {
      cct->get_perfcounters_collection()->remove(logger);
      delete logger;
    }
  }
  void init_add_pages(uint64_t offset, uint64_t len);

  void advertise_future_op(size_t num = 1) {
    future_ops += num;
  }
  int log(BlueWALContext* txc, bool force = true);
  int log_submit_sync(BlueWALContextSync* txc);

  int advertise_and_log(BlueWALContext* txc);

  void submitted(BlueWALContext* txc);

  // made virtual to be able to make UT stubs if needed
  virtual void aio_submit(IOContext* ioc) {
    bdev->aio_submit(ioc);
  }
  void aio_finish(BlueWALContext* txc);

  void shutdown();
  int replay(bool wipe_on_complete,
    std::function<int(const std::string&)> submit_db_fn);

  void flush_db();

  uint64_t get_total() const {
    return total;
  }
  uint64_t get_avail() const {
    return avail;
  }
  uint64_t get_page_size() const {
    return page_size;
  }
  uint64_t get_block_size() const {
    return block_size;
  }
  uint64_t get_header_size() const {
    return head_size;
  }

  //primarily for UT purposes
  void set_sync_flush(bool b) {
    sync_flush = b;
  }
};

#endif
