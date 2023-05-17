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

#include <utility>
#include <limits>

#include "common/errno.h"
#include "include/intarith.h"
#include "include/crc32c.h"
#include "blk/BlockDevice.h"
#include "BlueStore.h"

#include "BlueStoreWAL.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "BlueWAL: "

std::ostream& operator<<(std::ostream& out, const bluewal_head_t& h)
{
  out << "wal_header(" 
      << " pseq " << h.page_seq
      << " pcnt " << h.page_count
      << " tseq " << h.seq
      << " len " << h.len
      << " uuid " << h.uuid
      << " h.csum " << h.csum
      << ")";
  return out;
}

void *WALAsyncOpThread::entry()
{
  std::unique_lock l(lock);
  while (!do_stop) {
    cond.wait(l);
    if (!do_stop) {
      l.unlock();
      do_op();
      l.lock();
    }
  }
  return NULL;
}

void WALAsyncOpThread::do_op()
{
  wal.flush_db();
}

// =======================================================
enum {
  l_bluestore_wal_first = l_bluestore_last,
  l_bluestore_wal_input_avg,
  l_bluestore_wal_pad_bytes,
  l_bluestore_wal_output_avg,
  l_bluestore_wal_throttle_avg,
  l_bluestore_wal_merge_avg,
  l_bluestore_wal_chest_avg,
  l_bluestore_wal_matched_ops,
  l_bluestore_wal_not_matched_ops,
  l_bluestore_wal_match_miss_ops,
  l_bluestore_wal_wipe_bytes,
  l_bluestore_wal_knocking_avg,
  l_bluestore_wal_submit_lat,
  l_bluestore_wal_queued_lat,
  l_bluestore_wal_aio_lat,
  l_bluestore_wal_aio_done_lat,
  l_bluestore_wal_aio_finish_lat,
  l_bluestore_wal_aio_finish_misordered,
  l_bluestore_wal_flush_lat,
  l_bluestore_wal_last
};

BluestoreWAL::BluestoreWAL(CephContext* _cct,
  BlockDevice* _bdev,
  KeyValueDB* _db,
  const uuid_d& _uuid,
  uint64_t fsize,
  uint64_t psize,
  uint64_t bsize) :
    cct(_cct),
    bdev(_bdev),
    db(_db),
    flush_thread(*this),
    uuid(_uuid),
    flush_threshold(fsize / psize),
    page_size(psize),
    block_size(bsize),
    gate_chest(bsize)
{
  ceph_assert(psize >= bsize);
  if (!flush_threshold) {
    flush_threshold = 1;
  }

  curpage_pos = psize; //assign position which enforces new page selection
  page_seqno = 0;

  // estimate header size(s)
  bluewal_head_t header;
  header.uuid = uuid;
  bufferlist bl;
  encode(header, bl);
  head_size = bl.length();

  ops.resize(128 * 1024); //FIXME: make configurable/dependable of WAL size

  PerfCountersBuilder b(cct, "bluestoreWAL",
    l_bluestore_wal_first, l_bluestore_wal_last);
    b.add_u64_avg(l_bluestore_wal_input_avg, "input_avg",
      "Ops average submitted to WAL");
    b.add_u64_counter(l_bluestore_wal_pad_bytes, "pad_bytes",
      "Byte count added as padding when writing to WAL");
    b.add_u64_avg(l_bluestore_wal_output_avg, "output_avg",
      "Writes average submitted to disk");
    b.add_time_avg(l_bluestore_wal_throttle_avg, "throttle_avg",
      "Average throttled latency");
    b.add_time_avg(l_bluestore_wal_merge_avg, "merge_avg",
      "Average merging delay latency");
    b.add_u64_avg(l_bluestore_wal_chest_avg, "chest_avg",
      "Average ops submitted to WAL per batch (excluding single item batches)");
    b.add_u64_counter(l_bluestore_wal_matched_ops, "matched_ops",
      "Amount of ops merged into a single disk block");

    b.add_u64_counter(l_bluestore_wal_not_matched_ops, "not_matched_ops",
      "Amount of ops lacked merging due to full chest");
    b.add_u64_counter(l_bluestore_wal_match_miss_ops, "match_miss_ops",
      "Amount of ops lacked merging due to missing matching op");

    b.add_u64_counter(l_bluestore_wal_wipe_bytes, "wipe_bytes",
      "Byte count written by WAL to disk when wiping");
    b.add_time_avg(l_bluestore_wal_submit_lat, "submit_lat",
      "Average submit latency");
    b.add_time_avg(l_bluestore_wal_queued_lat, "queued_lat",
      "Average queued latency");
    b.add_time_avg(l_bluestore_wal_aio_lat, "aio_lat",
      "Average aio latency");
    b.add_time_avg(l_bluestore_wal_aio_done_lat, "aio_done_lat",
      "Average ordered aio latency");
    b.add_time_avg(l_bluestore_wal_aio_finish_lat, "aio_finish_lat",
      "Average aio_finish latency");
    b.add_u64_counter(l_bluestore_wal_aio_finish_misordered, "aio_finish_mis",
      "");
    b.add_u64_avg(l_bluestore_wal_knocking_avg, "knocking_avg",
      "Average amount of ops acquiring entrance lock");
    b.add_time_avg(l_bluestore_wal_flush_lat, "flush_lat",
      "Average DB flushing latency");

  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);

  flush_thread.create("bwal_kv_flush");
}

void BluestoreWAL::init_add_pages(uint64_t offset, uint64_t len)
{
  ceph_assert(len >= page_size);

  while (len >= page_size) {
    page_offsets.emplace_back(offset);
    offset += page_size;
    len -= page_size;
    total += page_size;
    avail += page_size;
  }
}

void BluestoreWAL::assess_payload(const chest_t& chest, uint64_t* need_pages)
{
  *need_pages = 0;
  auto page_pos = curpage_pos;
  chest.foreach_row(
    [&](size_t _need) {
      if (_need + page_pos <= page_size) {
        page_pos += _need;
        page_pos += p2nphase(page_pos, block_size);
      } else {
        if (page_pos + _need <= page_size) {
          (*need_pages)++;
          page_pos = p2roundup(_need, block_size);
        } else {
          auto tail = _need % page_size;
          *need_pages += _need / page_size + (tail ? 1 : 0);
          page_pos = 0;
        }
      }
    });
}

bool BluestoreWAL::init_op(const chest_t& chest, size_t* _need_pages, Op** op)
{
  auto &op_res = ops[(cur_op_seqno + 1) % ops.size()];
  if (op_res.running) {
    dout(7) << __func__
            << " no more ops, need waiting"
            << dendl;
    return false;
  }
  assess_payload(chest, _need_pages);
  if (*_need_pages == 0) {
    op_res.op_seqno = ++cur_op_seqno;
    *op = &op_res;
    return true;
  }
  ceph_assert(*_need_pages <= get_total_pages());

  uint64_t avail_pages = get_total_pages() - (page_seqno - last_wiped_page_seqno);
  uint64_t non_wiped_avail_pages = 0;

  // pending wiping pages can't be used and they prevent
  // following committed pages from being used as well.
  // hence measuring how many committed pages are available
  if (last_wiped_page_seqno == last_wiping_page_seqno) {
    non_wiped_avail_pages = last_committed_page_seqno - last_wiping_page_seqno;
  }
  if (avail_pages + non_wiped_avail_pages < *_need_pages) {
    dout(7) << __func__
            << " waiting on avail pages: "
            << avail_pages << " non-wiped avail pages: "
            << non_wiped_avail_pages << " need pages: "
            << *_need_pages
            << dendl;
    return false;
  }

  //FIXME: sanity!!!
  if (!(avail_pages + non_wiped_avail_pages  == avail / page_size) ||
      !(last_submitted_page_seqno <= page_seqno) ||
      !(last_committed_page_seqno <= last_submitted_page_seqno) ||
      !(last_wiping_page_seqno <= last_committed_page_seqno) ||
      !(last_wiped_page_seqno <= last_wiping_page_seqno)) {
    derr << __func__ << " before assertion:"
         << " need pages:" << *_need_pages
         << " avail pages:" << avail_pages
         << " avail:" << avail
         << " avail/page_size:" << avail / page_size
         << " pseq:" << page_seqno
         << " cpos: " << curpage_pos
         << " last submitted:" << last_submitted_page_seqno
         << " last committed:" << last_committed_page_seqno
         << " wiping: " << last_wiping_page_seqno
         << " wiped: " << last_wiped_page_seqno
         <<dendl;
  }
  ceph_assert(avail_pages + non_wiped_avail_pages == avail / page_size);
  ceph_assert(last_submitted_page_seqno <= page_seqno);
  ceph_assert(last_committed_page_seqno <= last_submitted_page_seqno);
  ceph_assert(last_wiping_page_seqno <= last_committed_page_seqno);
  ceph_assert(last_wiped_page_seqno <= last_wiping_page_seqno);
  ceph_assert(avail_pages + non_wiped_avail_pages == avail / page_size);

  // we can use non-wiped pages as we're planning to overwrite them
  // but we need to adjust relevant last sequences to avoid
  // confusion
  uint64_t non_wiped_advance = *_need_pages > avail_pages ? *_need_pages - avail_pages : 0;
  ceph_assert(non_wiped_advance <= non_wiped_avail_pages);
  last_wiping_page_seqno += non_wiped_advance;
  last_wiped_page_seqno += non_wiped_advance;

  op_res.op_seqno = ++cur_op_seqno;
  *op = &op_res;
  return true;
}

size_t BluestoreWAL::wipe_pages(IOContext* ioc)
{
  size_t wiping = 0;

  if (last_wiping_page_seqno < last_committed_page_seqno) {

    dout(7) << __func__ << " " << last_wiping_page_seqno << "->"
	    << last_committed_page_seqno
	    << dendl;
    wiping = last_committed_page_seqno - last_wiping_page_seqno;
    ceph_assert(wiping <= get_total_pages());
    ceph_assert(page_size * wiping <= avail);
    avail -= page_size * wiping;

    auto* buf = calloc(block_size, 0xff);

    do {
      // need to be done on each iteration since aio_write claims the buffer
      bufferptr bp = buffer::claim_char(block_size, (char*)buf);
      bufferlist bl;
      bl.append(bp);
      // piggy-back the write with wiping req for outdated pages
      ++last_wiping_page_seqno;
      logger->inc(l_bluestore_wal_wipe_bytes, bl.length());
      aio_write(page_offsets[get_page_idx(last_wiping_page_seqno)], bl, ioc, false);
    } while (last_wiping_page_seqno < last_committed_page_seqno);
    free(buf);
  }
  return wiping;
}

void BluestoreWAL::wipe_pages()
{
  IOContext ioctx(cct, NULL);
  auto wiping = wipe_pages(&ioctx);
  if (wiping) {
    ceph_assert(curpage_pos == 0);
    ceph_assert(page_seqno != 0);
    bluewal_head_t header;
    header.uuid = uuid;
    header.page_seq = page_seqno;
    header.page_count = 1;
    header.seq = ++cur_txc_seqno;
    header.len = 0;
    header.csum = 0;

    bufferlist bl;
    encode(header, bl);
    curpage_pos = bl.length();

    auto ap = bl.get_page_aligned_appender(
      p2roundup(block_size, size_t(CEPH_PAGE_SIZE)) / CEPH_PAGE_SIZE);

    IOContext ioctx0(cct, NULL);
    _maybe_write_unlock(
      &ioctx0,
      0,
      ap,
      bl,
      nullptr);

    aio_submit(&ioctx0);
    //write out dummy block txc first
    ioctx0.aio_wait();
    //then do the wiping
    aio_submit(&ioctx);
    ioctx.aio_wait();

    last_wiped_page_seqno = last_wiping_page_seqno;
    avail += page_size * wiping;
  }
}

void BluestoreWAL::aio_write(uint64_t off,
  bufferlist& bl,
  IOContext* ioc,
  bool buffered)
{
  bdev->aio_write(off, bl, ioc, false);
}

int BluestoreWAL::read(uint64_t off,
  uint64_t len,
  bufferlist* bl,
  IOContext* ioc,
  bool buffered)
{
  return bdev->read(off, len, bl, ioc, buffered);
}

void BluestoreWAL::do_flush_db()
{
  db->flush_all();
}


void BluestoreWAL::aio_finish(BlueWALContext* txc)
{
  mono_clock::time_point t0 = mono_clock::now();
  dout(7) << __func__ << " txc " << txc << dendl;
  ceph_assert(txc);
  ceph_assert(txc->get_wal_op_ctx());
  ceph_assert(txc->get_ioc()->num_running == 0);
  txc->get_ioc()->release_running_aios();
  Op& op = *static_cast<Op*>(txc->get_wal_op_ctx());
  logger->tinc(l_bluestore_wal_aio_lat, mono_clock::now() - op.io_start);
  txc->set_wal_op_ctx(nullptr);
  dout(7) << __func__ << " Op " << op.op_seqno
          << " min Op  " << min_pending_io_seqno << dendl;
  std::unique_lock l(lock);
  op.running = false;
  if (op.op_seqno == min_pending_io_seqno) {
    _finish_op(op, true);

    //awake pending submits if any
    if (num_queued) {
      flush_cond.notify_all();
      l.unlock();
    }
  } else {
    logger->inc(l_bluestore_wal_aio_finish_misordered);
  }
  logger->tinc(l_bluestore_wal_aio_finish_lat, mono_clock::now() - t0);
}

void BluestoreWAL::_finish_op(Op& op, bool deep)
{
  logger->tinc(l_bluestore_wal_aio_done_lat, mono_clock::now() - op.io_start);

  ceph_assert(!op.running);
  ceph_assert(op.op_seqno == min_pending_io_seqno);
  dout(7) << __func__ << " processing Op " << op.op_seqno
          << ", pseq " << op.page_seqno
          << ", num_txc " << op.num_txcs
          << dendl;
  for (size_t i = 0; i < op.num_txcs; i++) {
    ceph_assert(op.txc[i]);
    op.txc[i]->set_wal_seq(op.page_seqno);
    dout(7) << __func__ << " txc tseq " << op.txc[i]->get_wal_tseq() << dendl;
    op.txc[i]->wal_aio_finish();
  }
  if (op.wiping_pages) {
    dout(7) << __func__ << " wiped " << op.wiping_pages << " "
            << last_wiped_page_seqno << " <= "
	    << last_wiping_page_seqno
	    << dendl;
      avail += op.wiping_pages * page_size;
      last_wiped_page_seqno += op.wiping_pages;
      //FIXME: debug!!!!
      if (!(last_wiped_page_seqno <= last_wiping_page_seqno)) {
        dout(0) << __func__ << " wiped poorly " << op.wiping_pages << " "
	        << last_wiped_page_seqno << " <= "
	        << last_wiping_page_seqno
                << dendl;
    }
    ceph_assert(last_wiped_page_seqno <= last_wiping_page_seqno);
  }
  ++min_pending_io_seqno;
  dout(7) << __func__ << " processed Op " << op.op_seqno
          << dendl;
  op.reset();
  if (deep) {
    while (min_pending_io_seqno <= cur_op_seqno) {
      auto& op2 = ops[min_pending_io_seqno % ops.size()];
      dout(7) << __func__ << " may be processing Op " << op2.op_seqno
	      << ", page seqno " << op2.page_seqno
	      << " running " << op2.running
	      << " num_txcs " << op2.num_txcs
	      << dendl;
      if (op2.running) {
	break;
      }

      _finish_op(op2, false);
    }
  }
}

void BluestoreWAL::_prepare_txc_submit(bluewal_head_t& header,
                                       BlueWALContext* txc,
                                       bufferlist::page_aligned_appender& ap,
                                       bufferlist& bl)
{
  auto& t = txc->get_payload();
  txc->set_wal_tseq(++cur_txc_seqno);
  size_t t_len = t.length();
  auto csum = ceph_crc32c(cur_txc_seqno,
    (const unsigned char*)t.c_str(),
    t_len);

  header.seq = cur_txc_seqno;
  header.len = t_len;
  header.csum = csum;

  encode(header, bl);
  ap.append(t.c_str(), t_len);
  ceph_assert(bl.length() <= page_size);
}

void BluestoreWAL::_submit_huge_txc(bluewal_head_t& header,
                                        IOContext* anchor_ioc,
                                        BlueWALContext* txc,
                                        size_t pages)
{
  auto& t = txc->get_payload();
  txc->set_wal_tseq(++cur_txc_seqno);
  size_t t_len = t.size();
  const char* cptr = t.c_str();
  size_t max_t_len = page_size - head_size;
  auto csum = ceph_crc32c(cur_txc_seqno, (const unsigned char*)cptr, t_len);

  // first transaction's header keeps csum and len for the whole payload
  header.csum = csum;
  header.seq = cur_txc_seqno;

  // page_seq is to be already incremented outside,
  // let's decrement it for the sake of the following
  // loop simplicity
  ceph_assert(page_seqno);
  --page_seqno; // adjust for the loop below to use current page
  do {
    curpage_pos = 0;

    header.len = std::min(max_t_len, t_len);
    header.page_seq = ++page_seqno;

    bufferlist bl;
    bl.reserve(page_size);
    auto appender = bl.get_page_aligned_appender(
      p2roundup(page_size, size_t(CEPH_PAGE_SIZE)) / CEPH_PAGE_SIZE);
    encode(header, bl);
    appender.append(cptr, header.len);
    cptr += header.len;
    t_len -= header.len;
    ceph_assert(bl.length() <= page_size);
    curpage_pos = bl.length();

    _maybe_write_unlock(anchor_ioc,
                        0,
                        appender,
                        bl,
                        nullptr);
    --pages;
    header.csum = 0;
    header.seq = 0;
    header.page_count = 0; // folowup pages keep page_count = 0
  } while(pages);
  assert(curpage_pos <= page_size);
}

void BluestoreWAL::_maybe_write_unlock(IOContext* anchor_ioc,
                                       uint64_t _page_pos,
                                       bufferlist::page_aligned_appender& ap,
                                       bufferlist& _bl,
                                       wal_unique_lock_t* lck)
{
  size_t pad = 0;
  if (_bl.length()) {
    uint64_t write_offs = _page_pos + page_offsets[get_page_idx(page_seqno)];

    pad = p2nphase(size_t(_bl.length()), block_size);
    avail -= _bl.length() + pad;
    curpage_pos += pad;
    // unlock earlier if possible
    if (lck) {
      lck->unlock();
    }
    if (pad) {
      ap.append_zero(pad);
      logger->inc(l_bluestore_wal_pad_bytes, pad);
    }
    dout(7) << __func__
            << " tseq next: " << cur_txc_seqno
            << " write 0x" << std::hex
            << write_offs << "~" << _bl.length()
            << std::dec
            << " cpos " << curpage_pos
            << dendl;
    logger->inc(l_bluestore_wal_output_avg, _bl.length());
    aio_write(write_offs, _bl, anchor_ioc, false);
  }
}

int BluestoreWAL::advertise_and_log(BlueWALContext* txc)
{
  advertise_future_op();
  return log(txc);
}

int BluestoreWAL::log(BlueWALContext* txc)
{
  ceph_assert(txc);
  ceph_assert(!txc->get_wal_op_ctx());
  Op* op = _log(txc, false);
  if (op) {
    logger->tinc(l_bluestore_wal_submit_lat,
      mono_clock::now() - op->birth_time);
    txc->set_wal_op_ctx(op);
    op->io_start = mono_clock::now();
    aio_submit(txc->get_ioc());
    return 0;
  }
  return -EINPROGRESS;
}

int BluestoreWAL::log_submit_sync(BlueWALContextSync* txc)
{
  ceph_assert(txc);
  ceph_assert(!txc->get_wal_op_ctx());
  int r = -EIO;
  dout(7) << __func__ << " start " << dendl;
  advertise_future_op();
  Op* op = _log(txc, true);
  if (op) {
    txc->set_wal_op_ctx(op);
    op->io_start = mono_clock::now();
    auto* ioc = txc->get_ioc();
    bdev->aio_submit(ioc);
    ioc->aio_wait();
    aio_finish(txc);
    //we might need wait for completion explicitly as the above
    // aio_finish() call doesn't guarantee that relevant wal operaton
    // has actually finished - this might be postponed till we're done with
    // prior wal operations - to ensure their proper ordering
    txc->wait_completed();
    r = txc->wal_submitted();
  }
  if (r >= 0) {
    dout(7) << __func__ << " completed." << dendl;
  } else {
    derr << __func__ << " failed: " << cpp_strerror(r) << dendl;
  }
  return r;
}

void BluestoreWAL::_maybe_throttle()
{
  uint64_t wait_nsec = 0;
  uint64_t throttled_space = get_total() *
    (1 - cct->_conf->bluestore_wal_throttle_enable_threshold);
  if (throttled_space > get_avail()) {
    wait_nsec =
      cct->_conf->bluestore_wal_throttle_max_delay_nsec;
    if (get_avail()) {
      uint64_t base_thottle_nsec =
        cct->_conf->bluestore_wal_throttle_base_delay_nsec;
      double r_pct = (double)get_avail() / throttled_space;
      double k = 1 / (r_pct * r_pct);
      wait_nsec = std::min(
        wait_nsec,
        uint64_t(base_thottle_nsec * k));
    }
    /*FIXME: may be log long lasting throttling?
      if (wait_nsec > 10000000) {
      dout(0) << __func__ << " throttling " << txc << " "
              << wait_nsec << " nsec "
              << " avail = " << get_avail()
              << dendl;
    }*/
    logger->tinc(l_bluestore_wal_throttle_avg,
      utime_t(0, wait_nsec));
  }
  if (!future_ops && !wait_nsec) {
    uint64_t merge_delay =
      cct->_conf->bluestore_wal_txc_merge_delay_nsec;
    if (merge_delay && gate_chest.get_entry_count() == 0) {
      wait_nsec = merge_delay;
      logger->tinc(l_bluestore_wal_merge_avg,
        utime_t(0, wait_nsec));
    }
  }
  if (wait_nsec) {
    timespec ts(wait_nsec / 1000000000, wait_nsec % 1000000000);
    nanosleep(&ts, nullptr);
  }
}

BluestoreWAL::Op* BluestoreWAL::_log(BlueWALContext* txc, bool force)
{
  auto birth_time = mono_clock::now();
  auto& t = txc->get_payload();
  size_t t_len = t.length();
  // depending on payload len we might need just a single header
  // or multiple pages with a specific header each.
  size_t full_len = head_size + t_len;
  if (full_len > page_size) {
    auto tail = t_len % (page_size - head_size);
    auto p_count = t_len / (page_size - head_size);
    full_len = p_count * page_size + (tail ? head_size + tail : 0);
  }
  dout(7) << __func__ << " txc " << txc
          << " txc seqctx: " << txc->get_sequence_ctx()
          << " txc plen:" << t_len
          << " txc full_len:" << full_len
          << " force: " << force
          << dendl;
  logger->inc(l_bluestore_wal_input_avg, t_len);

  ceph_assert(future_ops);
  --future_ops;
  if(!force) {
    _maybe_throttle();
  }

  std::unique_lock glock(gate_lock);
  // We can temporarily store op in the chest if more ops to come.
  // Last op or absence of space in the chest will force pending ops to go.
  // The rationale is to try to merge multiple ops into a single
  // disk block to reduce amount of disk writes issued.
  if (!force &&
      future_ops) {
    int add_pos = gate_chest.add(txc, full_len, false);
    if (add_pos >= 0) {
      dout(7) << __func__ << " added to chest,"
              << " items:" << gate_chest.get_entry_count()
              << " insert pos:" << add_pos
              << dendl;
      return nullptr;
    }
  }
  chest_t my_chest(block_size);
  my_chest.claim(gate_chest);
  // last txc should be added in any case
  int r =  my_chest.add(txc, full_len, true);
  dout(7) << __func__ << " added to chest (last),"
    << " items:" << gate_chest.get_entry_count()
    << " insert pos:" << r
    << dendl;
  ceph_assert(r >= 0);

  if (my_chest.get_entry_count() > 1) {
    logger->inc(l_bluestore_wal_chest_avg, my_chest.get_entry_count());
  }

  // take primary WAL lock to submit the ops
  // and release gate lock to enable new ops accumulation
  //
  std::unique_lock l(lock);
  glock.unlock();

  size_t need_pages = 0;
  Op* op_ptr = nullptr;
  bool was_queued = false;
  auto t0 = mono_clock::now();
  while (!init_op(my_chest,
                  &need_pages,
                  &op_ptr)) {
    ++num_queued;
    was_queued = true;
    dout(8) << __func__ << " - no op, waiting: "
            << num_queued << dendl;
    flush_cond.wait(l);
    --num_queued;
    dout(8) << __func__ << " wait done" << dendl;
  }
  dout(10) << __func__ << " txc start: "
           << " tseq " << cur_txc_seqno + 1 // new txc seq
           << " need pages " << need_pages << " pseq: " << page_seqno
           << " cpos " << curpage_pos
           << " Op " << op_ptr->op_seqno
           << dendl;
  if (was_queued) {
    dout(10) << __func__ << " pending in queue for "
             << mono_clock::now() - t0
             << dendl;

    logger->tinc(l_bluestore_wal_queued_lat,
      mono_clock::now() - t0);
  }
  auto& op = *op_ptr;
  op.birth_time = birth_time;

  size_t chest_pos = 0;
  size_t txc_size = 0;

  //NB: we need to perform write within the last txc context,
  // as aio_submit is issued on it by the caller
  auto* anchor_ioc = txc->get_ioc();
  size_t wiping = wipe_pages(anchor_ioc);

  auto* cur_txc = my_chest.get_next_if_any(&chest_pos, &txc_size);

  bluewal_head_t header;
  header.uuid = uuid;

  bufferlist bl;
  auto appender = bl.get_page_aligned_appender(
    p2roundup(block_size, size_t(CEPH_PAGE_SIZE)) / CEPH_PAGE_SIZE);
  bl.reserve(block_size);

  // offset to perform writing to
  auto write_pos = curpage_pos;
  auto op_page_seqno = page_seqno; // page seq bound to Op
  do {
    auto pad = p2nphase(size_t(bl.length()), block_size);
    if (txc_size <= pad ||
        txc_size + pad + curpage_pos <= page_size) {
      header.page_seq = 0;
      header.page_count = 0;
      ceph_assert(page_seqno);

      if (txc_size > pad) {
        dout(20) << __func__ << " pad + txc: pad " << pad
                 << " tsize " << txc_size << " bl " << bl.length()
                 << " cpos " << curpage_pos
                 << dendl;
        if (pad) {
          appender.append_zero(pad);
          logger->inc(l_bluestore_wal_pad_bytes, pad);
          curpage_pos += pad;
        }
      }
      _prepare_txc_submit(header,
                          cur_txc,
                          appender,
                          bl);
      curpage_pos += txc_size;
      dout(10) << __func__ << " txc: "
             << " tseq " << cur_txc_seqno
               << " tsize " << txc_size << " bl " << bl.length()
               << " cpos " << curpage_pos
               << " " << cur_txc
               << dendl;
    } else {
      ceph_assert(need_pages);
      _maybe_write_unlock(anchor_ioc, write_pos, appender, bl, nullptr);
      bl.clear();

      // mark page's leftovers as used if new page is required
      if (curpage_pos) {
        avail -= page_size - curpage_pos;
        curpage_pos = 0;
        ++page_seqno;
        op_page_seqno = page_seqno;
      }
      write_pos = 0;
      auto txc_size_aligned = p2roundup(txc_size, block_size);
      auto tail = txc_size_aligned % page_size;
      auto pages = txc_size_aligned / page_size + (tail ? 1 : 0);
      header.page_seq = page_seqno;
      header.page_count = pages;

      if (pages == 1) {
        ceph_assert(txc_size <= page_size);
        _prepare_txc_submit(header,
                            cur_txc,
                            appender,
                            bl);
        curpage_pos += txc_size;
      } else {
        _submit_huge_txc(header,
                         anchor_ioc,
                         cur_txc,
                         pages);
        // this implements unconditional page change
        // for huge txc and enforces txc's "self-submission" indication,
        // i.e. the txc submission to DB marks all the involved pages
        // submitted
        ceph_assert(curpage_pos);
        if (curpage_pos <= page_size) {
          avail -= page_size - curpage_pos;
        }
        curpage_pos = page_size;
        op_page_seqno = page_seqno + 1;
      }
      dout(10) << __func__ << " txc/new_page: "
               << " tseq " << cur_txc_seqno
               << " tsize " << txc_size << " bl " << bl.length()
               << " cpos " << curpage_pos
               << " " << cur_txc
               << dendl;
    }
    if (op.empty()) {
      op.run(
        wiping,
        op_page_seqno,
        cur_txc);
    } else {
      // this is a no-op unless op_page_seqno is modified
      // to provided "self-submission" indication for huge txc
      //
      op.maybe_update_page_seqno(op_page_seqno);
    }
    cur_txc = my_chest.get_next_if_any(&chest_pos, &txc_size);
    if (cur_txc) {
      op.run_more(cur_txc);
    }
  } while(cur_txc);
  _maybe_write_unlock(anchor_ioc, write_pos, appender, bl, &l);
  return &op;
}

void BluestoreWAL::submitted(BlueWALContext* txc)
{
  // txc's wal seqno indicates all the preceeding pages have been submitted,
  // while the bound one is still 1being submitted (i.e. busy)
  // we might get multiple confirmations for the same current page,
  // just ignore repeated/outdated ones.

  auto pseqno = txc->get_wal_seq();
  ceph_assert(pseqno);
  --pseqno; // now indicates the last submitted page
  // make sure we update last_submitted in a strictly increasing way
  // without acquiring a lock
  auto expected = last_submitted_page_seqno.load();
  if ((pseqno > expected) &&
    last_submitted_page_seqno.compare_exchange_weak(expected, pseqno)) {
    bool maybe_flush = pseqno >= last_committed_page_seqno + flush_threshold;
    dout(8) << __func__ << " pseq " << pseqno
            << " last_submitted: " << last_submitted_page_seqno
            << " last_committed: " << last_committed_page_seqno
            << " flush thrs: " << flush_threshold
            << " avail: " << avail
            << " maybe flush: " << maybe_flush
            << dendl;
    if (maybe_flush) {
      if (sync_flush) {
        flush_db();
      } else {
        flush_thread.trigger_op();
      }
    }
  }
}

void BluestoreWAL::shutdown(bool restricted)
{
  if (!flush_thread.is_started()) {
    return;
  }

  dout(10) << __func__ << " WAL" << dendl;
  flush_thread.stop();
  std::unique_lock l(lock);
  ceph_assert(num_queued == 0);
  dout(15) << __func__ << " WAL thread stopped" << dendl;

  if (restricted) {
    return;
  }
  do_flush_db();
  last_committed_page_seqno = last_submitted_page_seqno.load();
  IOContext ioctx(cct, nullptr);
  auto wiping = wipe_pages(&ioctx);
  if (wiping) {
    dout(10) << __func__
            << " wiping " << wiping
            << dendl;

    aio_submit(&ioctx);
    ioctx.aio_wait();

    dout(10) << __func__
            << " wiped " << dendl;
  }
}

int BluestoreWAL::_read_page_header(uint64_t o,
                                    uint64_t pcount,
                                    bluewal_head_t* header)
{
  bufferlist bl;
  IOContext ioc(cct, NULL);
  dout(20) << __func__ << " reading page head at 0x"
           << std::hex << o << "~" << block_size << std::dec
           << dendl;
  int r = read(o, block_size, &bl, &ioc, false);
  if (r < 0) {
    derr << __func__ << " failed reading page head at 0x"
         << std::hex << o << "~" << block_size << std::dec
         << " : " << cpp_strerror(r)
         << dendl;
  }
  ceph_assert(r == 0);
  r = -1;    
  try {
    auto pp = bl.front().begin_deep();
    header->decode(pp);
    dout(15) << __func__ << " got page " << *header
             << " at 0x"
             << std::hex << o << std::dec
             << dendl;
      
    if (!header->page_count) {
      dout(15) << __func__ << " page information is inconsistent, ignoring"
               << dendl;
    } else if (header->uuid != uuid) {
      dout(15) << __func__ << " unexpected header uuid, ignoring"
               << dendl;
    } else {
      r = 0;
    }
  } catch (ceph::buffer::error& e) {
    dout(25) << __func__ << " unable to decode page header at offset " << o
             << ": " << e.what()
	     << dendl;
  }
  return r;
}

int BluestoreWAL::replay(bool restricted,
  std::function<int(const std::string&)> submit_db_fn)
{
  dout(10) << __func__
           << " page = " << page_size
           << " head = " << head_size
           << " block = " << block_size
           << " restricted = " << restricted
           << dendl;
  if (!flush_thread.is_started()) {
    flush_thread.create("bwal_kv_flush");
  }
  auto page_count = page_offsets.size();
  std::deque<bluewal_head_t> valid_page_headers;
  for (auto poffs : page_offsets) {
    bluewal_head_t header;
    int r = _read_page_header(poffs, page_count, &header);
    if (r == 0) {
      valid_page_headers.emplace_back(header);
      ceph_assert(header.page_count == 1); //FIXME add support for multi-page ops
    }
  }
  if (valid_page_headers.empty()) {
    dout(15) << __func__ << " wal is empty, completed." << dendl;
    //FIXME: setup all the sequence numbers properly!
    //FIXME: actually we need to proceed with increasing txc_seqno to avoid potential issues
    //with accessing legacy blocks
    return 0;
  } 
  std::sort(valid_page_headers.begin(), valid_page_headers.end(),
    [&](const bluewal_head_t& a, const bluewal_head_t& b) {
      return a.page_seq < b.page_seq;
    });
  //sanity check that page sequence numbering is monotonically increasing
  auto it0 = valid_page_headers.begin();  
  auto it = it0;
  ++it;
  while (it <  valid_page_headers.end()) {
    dout(15) << __func__ << " pseq " << it->page_seq << dendl;
    if (it->page_seq != it0->page_seq + 1) {
      derr << __func__ << " page sequencing is broken, found a gap: "
           << it0->page_seq << " <-> " << it->page_seq
	   << dendl;
      return -1;
    }
    it0 = it;
    ++it;
  }
  size_t db_submitted = 0;
  uint64_t next_txc_seqno = valid_page_headers.front().seq;
  for (auto& h0 : valid_page_headers) {
    uint64_t o = page_offsets[get_page_idx(h0.page_seq)];
    auto o0 = o;
    uint64_t last_offset = o + page_size;
    bluewal_head_t h;
    bool read_next;
    bufferlist bl;
    size_t pos = 0;
    do {
      IOContext ioc(cct, NULL);
      auto o00 = o0 + pos;
      int r;
      auto delta = bl.length() - pos;
      if (delta < head_size) {
        ceph_assert(o + block_size <= last_offset);
        dout(20) << __func__ << " reading block head at 0x"
                 << std::hex << o << "~" << block_size << std::dec
                 << dendl;
        r = read(o, block_size, &bl, &ioc, false);
        if (r < 0) {
          derr << __func__ << " failed reading block head at 0x"
               << std::hex << o << "~" << block_size << std::dec
               << " : " << cpp_strerror(r)
               << dendl;
        }
        ceph_assert(r == 0);
        o += block_size;
        pos += delta;
      }
      dout(20) << __func__ << " inspecting block head at 0x"
               << std::hex << o00 << "~" << bl.length() - pos << std::dec
               << dendl;
      read_next = false;
      try {
        auto p = bl.cbegin(pos);
        decode(h, p);
        dout(20) << __func__ << " got txc " << h << dendl;
        if (h.seq != next_txc_seqno) {
          dout(15) << __func__ << " misordered txc at 0x"
                   << std::hex << o00 << std::dec
                   << ", stopping"
                   << dendl;
        } else if (h.uuid != uuid) {
          dout(15) << __func__ << " unexpected txc header uuid"
                   << ", stopping"
                   << dendl;
        } else {

          int64_t delta = (int64_t)h.len - (int64_t)p.get_remaining();
          if (delta > 0) {
            auto to_read = p2roundup((uint64_t)delta, block_size);
            dout(20) << __func__ << " reading block tail at 0x"
                     << std::hex << o << "~" << to_read << std::dec
                     << dendl;
            ceph_assert(o + to_read <= last_offset);
            r = read(o, to_read, &bl, &ioc, false);
            if (r < 0) {
              derr << __func__ << " failed reading block tail at 0x"
                   << std::hex << o << "~" << to_read << std::dec
                   << " : " << cpp_strerror(r)
                   << dendl;
            }
            ceph_assert(r == 0);
            o += to_read;
          }
          ceph_assert(p.get_remaining() >= h.len);
          if (h.len > 0) {
            std::string content;
            p.copy(h.len, content);
            auto csum = ceph_crc32c(h.seq,
                                    (const unsigned char*)content.c_str(),
                                    content.size());
            if (csum != h.csum) {
              derr << __func__ << " txc checksum failed, txc " << h
                   << " at offset 0x"
                   << std::hex << o00 << std::dec
                   << " actual csum " << csum
                   << dendl;
              return r;
            }
            ++db_submitted;
            dout(7) << __func__ << " submit txc " << h << dendl;
            r = submit_db_fn(content);
            if (r != 0) {
              derr << __func__ << " txc submit failed, txc " << h
                   << " at offset 0x"
                   << std::hex << o00 << std::dec
                   << dendl;
              return r;
            }
          } else {
            dout(15) << __func__ << " dummy txc found."
                     << dendl;
          }
          ++next_txc_seqno;
          pos = p.get_off();
          read_next = (pos + head_size <= page_size);
        }
      } catch (ceph::buffer::error& e) {
        std::string next_step = ", trying next page";
        auto pos_bak = pos;
        read_next = ((pos % block_size) != 0) && (bl.length() + block_size <= page_size);
        if (read_next) {
          ceph_assert(bl.length() - pos < block_size);
          pos = bl.length();
          next_step = ", trying next block";
        }
        dout(15) << __func__ << " undecodable txc header at offset 0x"
                 << std::hex << o00 << std::dec
                 << next_step
                 << " bl " << bl.length()
                 << " pos " << pos_bak
                 << dendl;
      }
    } while(read_next);
  }
  if (!restricted && db_submitted) {
    do_flush_db();
  }
  last_submitted_page_seqno = valid_page_headers.back().page_seq;
  last_committed_page_seqno = valid_page_headers.back().page_seq;
  page_seqno = last_committed_page_seqno + 1;
  min_pending_io_seqno = 1;

  curpage_pos = 0;
  cur_txc_seqno = next_txc_seqno;
  last_wiping_page_seqno = valid_page_headers.front().page_seq - 1;
  last_wiped_page_seqno = last_wiping_page_seqno;
  avail = total; // - page_size * (last_committed_page_seqno - last_wiped_page_seqno);
  cur_op_seqno = 0;
  if (!restricted) {
    dout(5) << __func__ << " completed, wiping pending:"
            << " avail:" << avail
            << " avail/page_size:" << avail / page_size
            << " page seqno:" << page_seqno
            << " page pos: " << curpage_pos
            << " last submitted:" << last_submitted_page_seqno
            << " last committed:" << last_committed_page_seqno
            << " wiping: " << last_wiping_page_seqno
            << " wiped: " << last_wiped_page_seqno
            << dendl;
    wipe_pages();
  }
  dout(5) << __func__ << " completed:"
           << " avail:" << avail
           << " avail/page_size:" << avail / page_size
           << " last submitted:" << last_submitted_page_seqno
           << " last committed:" << last_committed_page_seqno
           << " wiping: " << last_wiping_page_seqno
           << " wiped: " << last_wiped_page_seqno
           << " next tseq: " << cur_txc_seqno
           << dendl; 
  return 0;
}

void BluestoreWAL::flush_db()
{
  uint64_t last_submitted_snap = last_submitted_page_seqno; // make a snapshot
  if (last_submitted_snap >= last_committed_page_seqno + flush_threshold) {
    dout(8) << __func__
      << " last_submitted_snap: " << last_submitted_snap
      << " last_committed: " << last_committed_page_seqno
      << " flush thrs: " << flush_threshold
      << " avail: " << avail
      << " num_queued: " << num_queued
      << dendl;

    auto t0 = mono_clock::now();
    dout(7) << __func__ << " flushing" << dendl;
    do_flush_db();
    dout(7) << __func__ << " flushed" << dendl;
    std::unique_lock l(lock);
    dout(7) << __func__ << " locked" << dendl;
    // recalculate under the lock
    if (last_submitted_snap > last_committed_page_seqno) {
      int64_t to_flush = page_size *
        (last_submitted_snap - last_committed_page_seqno);
      last_committed_page_seqno = last_submitted_snap;
      avail += to_flush;
    }
    dout(7) << __func__
      << " last_submitted_snap: " << last_submitted_snap
      << " last_submitted: " << last_submitted_page_seqno
      << " last_committed: " << last_committed_page_seqno
      << " avail: " << avail
      << " num_queued: " << num_queued
      << " flush completed in " << (mono_clock::now() - t0)
      << dendl;

    //awake pending submits if any
    if (num_queued) {
      flush_cond.notify_all();
      l.unlock();
    }
    logger->tinc(l_bluestore_wal_flush_lat, mono_clock::now() - t0);
  }
}

// =======================================================
