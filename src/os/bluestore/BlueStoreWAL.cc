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

// =======================================================
enum {
  l_bluestore_wal_first = l_bluestore_last,
  l_bluestore_wal_input_avg,
  l_bluestore_wal_pad_bytes,
  l_bluestore_wal_output_avg,
  l_bluestore_wal_chest_avg,
  l_bluestore_wal_matched_ops,
  l_bluestore_wal_not_matched_ops,
  l_bluestore_wal_match_miss_ops,
  l_bluestore_wal_wipe_bytes,
  l_bluestore_wal_knocking_avg,
  l_bluestore_wal_queued_lat,
  l_bluestore_wal_aio_finish_lat,
  l_bluestore_wal_flush_lat,
  l_bluestore_wal_last
};

BluestoreWAL::BluestoreWAL(CephContext* _cct,
  BlockDevice* _bdev,
  const uuid_d& _uuid,
  uint64_t fsize,
  uint64_t psize,
  uint64_t bsize) :
    cct(_cct),
    bdev(_bdev),
    uuid(_uuid),
    flush_threshold(fsize / psize),
    page_size(psize),
    block_size(bsize)
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
    b.add_time_avg(l_bluestore_wal_aio_finish_lat, "aio_finish_lat",
      "Average aio_finish latency");
    b.add_u64_avg(l_bluestore_wal_knocking_avg, "knocking_avg",
      "Average amount of ops acquiring entrance lock");
    b.add_time_avg(l_bluestore_wal_queued_lat, "queued_lat",
      "Average queued latency");
    b.add_time_avg(l_bluestore_wal_flush_lat, "flush_lat",
      "Average DB flushing latency");

  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
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
    ceph_assert(wiping < get_total_pages());
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

void BluestoreWAL::aio_finish(BlueWALContext* txc)
{
  mono_clock::time_point t0 = mono_clock::now();
  dout(7) << __func__ << " txc " << txc << dendl;
  ceph_assert(txc);
  ceph_assert(txc->get_wal_op_ctx());
  ceph_assert(txc->get_ioc()->num_running == 0);
  txc->get_ioc()->release_running_aios();
  Op& op = *static_cast<Op*>(txc->get_wal_op_ctx());
  txc->set_wal_op_ctx(nullptr);
  dout(7) << __func__ << " Op " << op.op_seqno
          << " min Op  " << min_pending_io_seqno << dendl;
  std::unique_lock l(lock);
  op.running = false;
  if (op.op_seqno == min_pending_io_seqno) {
    _finish_op(op, true);

    //awake pending submits if any
    if (num_queued) {
      l.unlock();
      flush_cond.notify_all();
    }
  }
  logger->tinc(l_bluestore_wal_aio_finish_lat, mono_clock::now() - t0);
}

void BluestoreWAL::_finish_op(Op& op, bool deep)
{
  ceph_assert(!op.running);
  ceph_assert(op.op_seqno == min_pending_io_seqno);
  dout(7) << __func__ << " processing Op " << op.op_seqno
          << ", pseq " << op.page_seqno
          << ", num_txc " << op.num_txcs
          << dendl;
  for (size_t i = 0; i < op.num_txcs; i++) {
    ceph_assert(op.txc[i]);
    op.txc[i]->set_wal_seq(op.page_seqno);
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
  size_t t_len = t.length();
  auto csum = ceph_crc32c(++cur_txc_seqno,
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
  size_t t_len = t.size();
  const char* cptr = t.c_str();
  size_t max_t_len = page_size - head_size;
  auto csum = ceph_crc32c(++cur_txc_seqno, (const unsigned char*)cptr, t_len);

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
  assert(curpage_pos < page_size);
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

int BluestoreWAL::log(BlueWALContext* txc, bool force)
{
  ceph_assert(txc);
  ceph_assert(!txc->get_wal_op_ctx());

  Op* op = _log(txc, false);
  if (op) {
    txc->set_wal_op_ctx(op);
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
    dout(7) << __func__ << " completed: " << dendl;
  } else {
    derr << __func__ << " failed: " << cpp_strerror(r) << dendl;
  }
  return r;
}

BluestoreWAL::Op* BluestoreWAL::_log(BlueWALContext* txc, bool force)
{
  auto& t = txc->get_payload();
  size_t t_len = t.length();
  dout(7) << __func__ << " txc " << txc
          << " txc plen:" << t_len
          << " force: " << force
          << dendl;

  logger->inc(l_bluestore_wal_input_avg, t_len);
  chest_t my_chest;
  {
    std::unique_lock l(gate_lock);
    ceph_assert(future_ops);
    --future_ops;
    // We can temporarily store op in the chest if more ops to come.
    // Last op or absence of space in the chest will force pending ops to go.
    // The rationale is to try to merge multiple ops into a single
    // disk block to reduce amount of disk writes issued.
    if (!force &&
        future_ops &&
        gate_chest.add(txc, t_len, head_size, block_size, page_size, false)) {
          dout(7) << __func__ << " added to chest, items:"
                  << gate_chest.get_entry_count() << dendl;
          return nullptr;
    }
    my_chest.claim(gate_chest);
  }
  // last txc should be added in any case
  bool b = my_chest.add(txc, t_len, head_size, block_size, page_size, true);
  ceph_assert(b);

  if (my_chest.get_entry_count() > 1) {
    logger->inc(l_bluestore_wal_chest_avg, my_chest.get_entry_count());
  }

  std::unique_lock l(lock);

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
    logger->tinc(l_bluestore_wal_queued_lat,
      mono_clock::now() - t0);
  }
  auto& op = *op_ptr;

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
        if (curpage_pos < page_size) {
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

void BluestoreWAL::submitted(BlueWALContext* txc,
			     std::function<void()> flush_db_fn)
{
  // txc's wal seqno indicates all the preceeding pages have been submitted,
  // while the bound one is still being submitted (i.e. busy)
  // we might get multiple confirmations for the same current page,
  // just ignore repeated/outdated ones.

  auto page_seqno = txc->get_wal_seq();
  ceph_assert(page_seqno);
  --page_seqno; // no indicates the last submitted page
  dout(7) << __func__ << " pseq " << page_seqno
          << " last_submitted: " << last_submitted_page_seqno
          << " last_committed: " << last_committed_page_seqno
          << " avail: " << avail
          << " num_queued: " << num_queued
          << dendl;
  auto expected = last_submitted_page_seqno.load();
  if ((page_seqno > expected) &&
      last_submitted_page_seqno.compare_exchange_weak(expected, page_seqno)) {
    if (page_seqno >=
         last_committed_page_seqno + flush_threshold) {
      auto t0 = mono_clock::now();
      flush_db_fn();
      std::unique_lock l(lock);
      dout(7) << __func__ << " pseq " << page_seqno
              << " last_submitted: " << last_submitted_page_seqno
              << " last_committed: " << last_committed_page_seqno
              << " avail: " << avail
              << " num_queued: " << num_queued
              << dendl;
      // recalculate under the lock
      if (page_seqno > last_committed_page_seqno) {
        int64_t to_flush = page_size *
          (page_seqno - last_committed_page_seqno);
        last_committed_page_seqno = page_seqno;
        avail += to_flush;
      }

      //awake pending submits if any
      if (num_queued) {
        l.unlock();
        flush_cond.notify_all();
      }
      logger->tinc(l_bluestore_wal_flush_lat, mono_clock::now() - t0);
    }
  }
}


void BluestoreWAL::shutdown(std::function<void()> flush_db_fn)
{
  dout(10) << __func__ << " WAL" << dendl;
  std::unique_lock l(lock);
  ceph_assert(num_queued == 0);
  flush_db_fn();
  last_committed_page_seqno = last_submitted_page_seqno.load();
  IOContext ioctx(cct, nullptr);
  auto wiping = wipe_pages(&ioctx);
  if (wiping) {
    dout(7) << __func__
            << "wiping " << wiping
            << dendl;

    aio_submit(&ioctx);
    ioctx.aio_wait();

    dout(7) << __func__
            << "wiped " << dendl;
  }
}

int BluestoreWAL::_read_page_header(uint64_t o,
                                    uint64_t expected_page_no,
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
      
    if (!header->page_count ||
        (expected_page_no && (header->page_seq % pcount) != expected_page_no)) {
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

int BluestoreWAL::replay(bool wipe_on_complete,
  std::function<int(const std::string&)> submit_db_fn,
  std::function<void()> flush_db_fn)
{
  dout(10) << __func__ << " WAL" << dendl;
  size_t page_no = 0; // any no is expected on start
  auto page_count = page_offsets.size();
  std::deque<bluewal_head_t> valid_page_headers;
  for (auto poffs : page_offsets) {
    bluewal_head_t header;
    int r = _read_page_header(poffs, page_no, page_count, &header);
    if (r == 0) {
      valid_page_headers.emplace_back(header);
      ceph_assert(header.page_count == 1); //FIXME add support for multi-page ops
      page_no = header.page_seq + 1;
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
          ++next_txc_seqno;

          int64_t delta = (int64_t)h.len - (int64_t)p.get_remaining();
          if (delta > 0) {
            auto to_read = p2roundup((uint64_t)delta, block_size);
            dout(20) << __func__ << " reading block tail at 0x"
                     << std::hex << o << "~" << to_read << std::dec
                     << dendl;
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
          read_next = o < last_offset;
          pos = p.get_off();
        }
      } catch (ceph::buffer::error& e) {
        std::string next_step;
        read_next = o < last_offset;
        auto pos_bak = pos;
        if (pos % block_size == 0 || !read_next) {
          next_step = ", trying next page";
          read_next = false;
        } else {
          ceph_assert(bl.length() - pos < block_size);
          pos = bl.length();
          next_step = ", trying next block";
        }
        dout(15) << __func__ << " undecodable txc header at offset 0x"
                 << std::hex << o00 << std::dec
                 << " next " << next_step
                 << " bl " << bl.length()
                 << " pos " << pos_bak
                 << dendl;
      }
    } while(read_next);
  }
  if (db_submitted) {
    flush_db_fn();
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
  if (wipe_on_complete) {
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
// =======================================================
