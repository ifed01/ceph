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

#include "include/intarith.h"
#include "include/crc32c.h"
#include "blk/BlockDevice.h"
#include "BlueStore.h"

#include "BlueStoreWAL.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "BlueWAL: "

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
  l_bluestore_submitted_lat,
  l_bluestore_wal_last
};

BluestoreWAL::BluestoreWAL(CephContext* _cct,
  BlockDevice* _bdev,
  const uuid_d& _uuid,
  uint64_t psize,
  uint64_t bsize) :
    cct(_cct),
    bdev(_bdev),
    uuid(_uuid),
    page_size(psize),
    block_size(bsize)
{
  ceph_assert(psize >= bsize);

  curpage_pos = psize; //assign position which enforces new page selection

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
    b.add_time_avg(l_bluestore_submitted_lat, "submitted_lat",
      "Average DB submission processing latency");

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

bool BluestoreWAL::init_op(size_t need, uint64_t* _need_pages, Op** op)
{
  auto &op_res = ops[(cur_op_seqno + 1) % ops.size()];
  if (op_res.running) {
    dout(7) << __func__
            << " no more ops, need waiting"
            << dendl;
    return false;
  }
  size_t need_pages = 0;
  if (need + head_size + curpage_pos > page_size) {
    auto psize = page_size - head_size;
    need_pages = round_up_to(need, psize) / psize;
    ceph_assert(need_pages <= get_total_pages());

    uint64_t avail_pages = get_total_pages() - (page_seqno - last_wiped_page_seqno);
    uint64_t non_wiped_avail_pages = 0;

    // pending wiping pages can't be used and prevent
    // following committed pages from usage as well
    // hence measuring how many committed pages are available
    if (last_wiped_page_seqno == last_wiping_page_seqno) {
      non_wiped_avail_pages = last_committed_page_seqno - last_wiping_page_seqno;
    }
    if (avail_pages + non_wiped_avail_pages < need_pages) {
      return false;
    }

    //FIXME: sanity!!!
    if (!(avail_pages + non_wiped_avail_pages  == avail / page_size) ||
        !(last_submitted_page_seqno <= page_seqno) ||
        !(last_committed_page_seqno <= last_submitted_page_seqno) ||
        !(last_wiping_page_seqno <= last_committed_page_seqno) ||
        !(last_wiped_page_seqno <= last_wiping_page_seqno)) {
      derr << __func__ << " before assertion:"
           << " need pages:" << need_pages
           << " avail pages:" << avail_pages
           << " avail:" << avail
           << " avail/page_size:" << avail / page_size
           << " page seqno:" << page_seqno
           << " page pos: " << curpage_pos
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
    uint64_t non_wiped_advance = need_pages > avail_pages ? need_pages - avail_pages : 0;
    ceph_assert(non_wiped_advance <= non_wiped_avail_pages);
    last_wiping_page_seqno += non_wiped_advance;
    last_wiped_page_seqno += non_wiped_advance;
  }

  op_res.op_seqno = ++cur_op_seqno;
  *_need_pages = need_pages;
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

void BluestoreWAL::aio_write(uint64_t off,
  bufferlist& bl,
  IOContext* ioc,
  bool buffered)
{
  bdev->aio_write(off, bl, ioc, false);
}

void BluestoreWAL::_notify_txc(uint64_t prev_page_seqno,
                               BlueStore::TransContext* txc,
                               txc_completion_fn on_finish)
{
  ceph_assert(txc);
  txc->wal_seq = prev_page_seqno;
  on_finish(txc);
}
void BluestoreWAL::aio_finish(BlueStore::TransContext* txc,
                              txc_completion_fn on_finish)
{
  mono_clock::time_point t0 = mono_clock::now();
  ceph_assert(txc);
  ceph_assert(txc->wal_op_ctx);
  ceph_assert(txc->ioc.num_running == 0);
  txc->ioc.release_running_aios();
  Op& op = *static_cast<Op*>(txc->wal_op_ctx);
  dout(7) << __func__ << " " << op.op_seqno
          << " min " << min_pending_io_seqno << dendl;
  std::unique_lock l(lock);
  op.running = false;
  if (op.op_seqno == min_pending_io_seqno) {
    _finish_op(op, on_finish, true);

    //awake pending submits if any
    if (num_pending_free) {
      l.unlock();
      flush_cond.notify_all();
    }
  }
  logger->tinc(l_bluestore_wal_aio_finish_lat, mono_clock::now() - t0);
}

void BluestoreWAL::_finish_op(Op& op, txc_completion_fn on_finish, bool deep)
{
  ceph_assert(!op.running);
  ceph_assert(op.op_seqno == min_pending_io_seqno);
  dout(7) << __func__ << " processing " << op.op_seqno
          << " prev seqno " << op.prev_page_seqno
          << dendl;
  for (size_t i = 0; i < op.num_txcs; i++) {
    _notify_txc(op.prev_page_seqno, op.txc[i], on_finish);
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
  dout(7) << __func__ << " processed " << op.op_seqno
          << dendl;
  op.reset();
  if (deep) {
    while (min_pending_io_seqno <= cur_op_seqno) {
      auto& op2 = ops[min_pending_io_seqno % ops.size()];
      dout(7) << __func__ << " may be processing " << op2.op_seqno
	      << " prev seqno " << op2.prev_page_seqno
	      << " running " << op2.running
	      << " num_txcs " << op2.num_txcs
	      << dendl;
      if (op2.running) {
	break;
      }

      _finish_op(op2, on_finish, false);
    }
  }
}

void BluestoreWAL::_prepare_submit_txc(
  bluewal_head_t& header,
  uint64_t txc_seqno,
  BlueStore::TransContext* txc,
  bufferlist::page_aligned_appender& appender,
  bufferlist& bl)
{
  auto& t = txc->t->get_as_bytes();
  size_t t_len = t.length();

  auto csum = ceph_crc32c(txc_seqno,
    (const unsigned char*)t.c_str(),
    t_len);

  header.seq = txc_seqno;
  // always put the whole length into the first transaction header
  header.len = t_len;
  // first transaction's header keeps csum for the whole payload
  header.csum = csum;

  size_t to_write = head_size + t_len;

  auto left = p2nphase(size_t(bl.length()), block_size);
  size_t pad = 0;
  if (left && left < to_write) {
    pad = left;
    to_write += pad;
  } else if (left) {
    logger->inc(l_bluestore_wal_matched_ops); //NB: this doesn't account the first
  }

  if (pad) {
    appender.append_zero(pad);
    logger->inc(l_bluestore_wal_pad_bytes, pad);
  }
  encode(header, bl);
  appender.append(t.c_str(), t_len);
}

void BluestoreWAL::log(BlueStore::TransContext* txc)
{
  ceph_assert(txc);
  ceph_assert(!txc->wal_op_ctx);

  Op* op = _log(txc);
  if (op) {
    txc->wal_op_ctx = op;
    aio_submit(&txc->ioc);
  }
}

BluestoreWAL::Op* BluestoreWAL::_log(BlueStore::TransContext* txc)
{
  bufferlist bl;
  bluewal_head_t header;

  auto& t = txc->t->get_as_bytes();
  size_t t_len = t.length();
  dout(7) << __func__ << " transact payload len:" << t_len
          << " more finish ind: " << txc->more_aio_finish
          << dendl;

  logger->inc(l_bluestore_wal_input_avg, t_len);

  size_t need_len = 0;
  chest_t my_chest;
  if (txc->more_aio_finish) {
    std::unique_lock l(gate_lock);
    if (gate_chest.add(txc, head_size + t_len, block_size, false)) {
      dout(7) << __func__ << " added to chest, items:" << gate_chest.get_entry_count()
              << dendl;
      return nullptr;
    }
    my_chest.claim(gate_chest);
  } else if (gate_chest.get_entry_count() != 0) {
    std::unique_lock l(gate_lock);
    my_chest.claim(gate_chest);
  }
  // last txc should be added in any case
  bool b = my_chest.add(txc, head_size + t_len, block_size, true);
  ceph_assert(b);

  need_len = my_chest.get_payload_len(block_size);

  header.uuid = uuid;

  if (my_chest.get_entry_count() > 1) {
    logger->inc(l_bluestore_wal_chest_avg, my_chest.get_entry_count());
  }

/*  logger->inc(l_bluestore_wal_knocking_avg, num_knocking ? 1 : 0);
  ++num_knocking;
  std::unique_lock l(lock, std::try_to_lock);
  if (!l.owns_lock()) {
    int chest_pos = -1;
    std::unique_lock l2(gate_lock);
    bool found_match = false;
    for(size_t i = 0; i < gate_chest.size(); i++) {
      if (gate_chest[i].l1 == 0) {
        if (chest_pos < 0) {
          chest_pos = i;
        }
      } else if (gate_chest[i].l1 + t.size() + 2 * head_size <= block_size) {
        found_match = true;
        gate_chest[i].l2 = t.size();
        chest_pos = -1;
        break;
      }
    }
    if (!found_match && chest_pos >= 0) {
      gate_chest[chest_pos].l1 = t.size();
    } else if (!found_match) {
      logger->inc(l_bluestore_wal_not_matched_ops);
    }
    l2.unlock();
    //if (found_match) {
    //  return;
    }
    l.lock();
    if (chest_pos >= 0) {
      l2.lock();
      if (gate_chest[chest_pos].full()) {
	logger->inc(l_bluestore_wal_matched_ops, 2);
      } else {
        logger->inc(l_bluestore_wal_match_miss_ops);
      }
      gate_chest[chest_pos].reset();
    }
  }
  --num_knocking;*/
  std::unique_lock l(lock);

  uint64_t need_pages = 0;
  Op* op_ptr = nullptr;
  bool was_queued = false;
  auto t0 = mono_clock::now();
  while (!init_op(need_len, &need_pages, &op_ptr)) {
    ++num_pending_free;
    was_queued = true;
    dout(8) << __func__ << " - no op, waiting: "
            << num_pending_free << dendl;
    flush_cond.wait(l);
    --num_pending_free;
    dout(8) << __func__ << " wait done" << dendl;
  }
  if (was_queued) {
    logger->tinc(l_bluestore_wal_queued_lat,
      mono_clock::now() - t0);
  }
  auto& op = *op_ptr;

  size_t chest_pos = 0;
  auto* first_txc = my_chest.get_next_if_any(&chest_pos);

  size_t wiping = wipe_pages(&first_txc->ioc);

  op.run(
    wiping,
    page_seqno ? page_seqno - 1 : 0, //NB: we would get delayed submit notification
                                     // for page seq 0 but that's fine
    first_txc);

  header.page_count = need_pages;
  header.page_seq = page_seqno;

  if (need_pages <= 1 ) {
    if (need_pages == 1) {
      // we need to use next page(s)
      op.prev_page_seqno = page_seqno++;

      header.page_seq = page_seqno;
      // "utilize" page's leftovers
      avail -= page_size - curpage_pos;
      curpage_pos = 0;
    }
    dout(7) << __func__ << " need pages:" << need_pages
            << " op seq:" << op.op_seqno
            << " op prev_page_seqno:" << op.prev_page_seqno
            << " wiping " << op.wiping_pages
            << dendl;

    auto* cur_txc = first_txc;
    uint64_t offs = curpage_pos + page_offsets[get_page_idx(page_seqno)];

    auto appender = bl.get_page_aligned_appender(
      p2roundup(need_len, size_t(CEPH_PAGE_SIZE)) / CEPH_PAGE_SIZE);
    bl.reserve(need_len);
    do {
       _prepare_submit_txc(header,
                           ++cur_txc_seqno,
                           cur_txc,
                           appender,
                           bl);
      cur_txc = my_chest.get_next_if_any(&chest_pos);
      if(!cur_txc) {
        break;
      }
      op.run_more(cur_txc);
    } while(true);
    auto pad = p2nphase(size_t(bl.length()), block_size);
    avail -= bl.length() + pad;
    curpage_pos += bl.length() + pad;
    l.unlock();

    if (pad) {
      appender.append_zero(pad);
      logger->inc(l_bluestore_wal_pad_bytes, pad);
    }
    dout(7) << __func__
            << " txc  submitted " << cur_txc_seqno
            << " write 0x" << std::hex << offs
            << "~" << bl.length() << std::dec
            << dendl;
    logger->inc(l_bluestore_wal_output_avg, bl.length());
    /*logger->inc(l_bluestore_wal_output_ops);
    logger->inc(l_bluestore_wal_output_bytes, bl.length());*/
    //NB: we need to perform write within the last txc context,
    // as aio_submit is issued on it by the caller
    aio_write(offs, bl, &txc->ioc, false);
    return &op;
  }
  ceph_assert(false);
#if 0
  // we need to use next page(s)
  op.prev_page_seqno = page_seqno++;

  header.page_seq = page_seqno;
  {
    size_t t_written = 0;

    // "utilize" page's leftovers
    avail -= page_size - curpage_pos;
    curpage_pos = 0;


    if (likely(need_pages == 1)) {
      size_t to_write = p2roundup(head_size + t_len, block_size);

      auto appender = bl.get_page_aligned_appender(to_write / block_size);
      bl.reserve(to_write);

      encode(header, bl);
      appender.append(t.c_str(), t_len);
      auto pad = p2nphase(size_t(bl.length()), block_size);
      if (pad) {
        appender.append_zero(pad);
        logger->inc(l_bluestore_wal_pad_bytes, pad);
      }
      avail -= bl.length();
      auto offs = curpage_pos + page_offsets[get_page_idx(page_seqno)];
      curpage_pos += bl.length();
      l.unlock();
      dout(7) << __func__
              << " op submitted " << op.op_seqno
	      << " prev_page_seqno " << op.prev_page_seqno
	      << " wiping " << op.wiping_pages
              << " write 0x" << std::hex << offs
	      << "~" << bl.length() << std::dec
	      << dendl;

      logger->inc(l_bluestore_wal_output_bytes, bl.length());
      aio_write(offs, bl, &txc->ioc, false);
      return &op;
    }

    size_t page = 0;
    do {
      bl.clear();

      size_t payload_len =
        std::min(t_len - t_written, page_size - head_size);
      size_t to_write =
        p2roundup(head_size + payload_len, block_size);
      auto appender = bl.get_page_aligned_appender(to_write / block_size);
      bl.reserve(to_write);

      ceph_assert( 0 == (curpage_pos % page_size));
      curpage_pos = 0;
      if (page != 0) {
        header.page_seq = ++page_seqno;
        // page_count for "follower" pages contain amount of left pages
        // in the bunch (including the current page)
        header.page_count = need_pages - page;

	header.len = payload_len;    // note just a portion which fits into the current page
	header.csum = 0;             // don't care about csum of the portion
      }
      encode(header, bl);
      appender.append(t.c_str() + t_written, payload_len);
      t_written += payload_len;
      auto pad = p2nphase(size_t(bl.length()), block_size);
      if (pad) {
        appender.append_zero(pad);
        logger->inc(l_bluestore_wal_pad_bytes, pad);
      }
      auto offs = curpage_pos + page_offsets[get_page_idx(page_seqno)];
      dout(7) << __func__ << " write 0x" << std::hex << offs
	      << "~" << bl.length() << std::dec << dendl;
      avail -= bl.length();
      //it's expected that the previous page has been completely consumed
      curpage_pos += bl.length();
      ++page;
      logger->inc(l_bluestore_wal_output_bytes, bl.length());
      aio_write(offs, bl, &txc->ioc, false);
    } while (page < need_pages);
  }

  //FIXME: do we really need that any more????
  // when huge op is issues (2+ pages) - mark ending page
  // as completely consumed
  // and immediately request completion indication for 
  // new pages - this is a sort of protection from
  // huge op deadlocking the WAL while being uncommitted
  // 
  if (need_pages > 2) {
    avail -= page_size - curpage_pos;
    curpage_pos = page_size;
    // this will cause the next log() call to request the second submit
    // indication for the same page seqno - submitted() to handle(i.e. ignore) that
    op.prev_page_seqno = page_seqno;
  }

  dout(7) << __func__ << " op submitted " << op.op_seqno
	  << " wiping " << op.wiping_pages
	  << " prev_page_seqno " << op.prev_page_seqno
	  << dendl;
#endif
  return &op;
}

void BluestoreWAL::submitted(uint64_t submitted_page_seqno,
			     KeyValueDB& db)
{
  // we might get multiple confirmations for the same page
  // just ignore repeated/outdated ones
  if (submitted_page_seqno > last_submitted_page_seqno) {

    dout(7) << __func__ << " seq: " << submitted_page_seqno
            << " last_submitted: " << last_submitted_page_seqno
            << " last_committed: " << last_committed_page_seqno
            << " avail: " << avail
            << " num_pending_free: " << num_pending_free
            << dendl;
    auto t0 = mono_clock::now();

    ceph_assert(submitted_page_seqno > last_submitted_page_seqno);
    last_submitted_page_seqno = submitted_page_seqno;

    ceph_assert(last_submitted_page_seqno >= last_committed_page_seqno);
    auto to_flush = page_size *
     (last_submitted_page_seqno - last_committed_page_seqno);

    //FIXME: to configure, 1/4 of total for now
    if (to_flush >= (total >> 2)) {
      db.flush_all();
      std::unique_lock l(lock);
      last_committed_page_seqno = last_submitted_page_seqno;
      avail += to_flush;

      //awake pending submits if any
      if (num_pending_free) {
        l.unlock();
        flush_cond.notify_all();
      }
    }
    logger->tinc(l_bluestore_submitted_lat, mono_clock::now() - t0);
  }
}


void BluestoreWAL::shutdown(KeyValueDB& db)
{
  std::unique_lock l(lock);
  ceph_assert(num_pending_free == 0);
  db.flush_all();
  last_committed_page_seqno = last_submitted_page_seqno;

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
// =======================================================
