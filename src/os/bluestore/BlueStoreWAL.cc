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

#include <utility>

#include "blk/BlockDevice.h"
#include "BlueStore.h"

#include "BlueStoreWAL.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "BlueWAL: "

void BluestoreWAL::Op::aio_finish(BlueStore* store)
{
  running = false;
  wal->aio_finish(store, *this);
}
// =======================================================
enum {
  l_bluestore_wal_first = l_bluestore_last,
  l_bluestore_wal_input_bytes,
  l_bluestore_wal_pad_bytes,
  l_bluestore_wal_output_bytes,
  l_bluestore_wal_wipe_bytes,
  l_bluestore_wal_aio_finish_lat,
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
    block_size(bsize),
    pending_transactions(
      pending_transactions_t::bucket_traits(pt_buckets, MAX_BUCKETS))
{
  ceph_assert(psize >= bsize);

  curpage_pos = psize; //assign position which enforces new page selection

  // estimate header size(s)
  bluewal_transact_head_t theader;
  theader.uuid = uuid;
  bufferlist bl;
  encode(theader, bl);
  thead_size = bl.length();

  bluewal_page_head_t pheader;
  pheader.uuid = uuid;
  bl.clear();
  encode(pheader, bl);
  phead_size = bl.length();

  PerfCountersBuilder b(cct, "bluestoreWAL",
    l_bluestore_wal_first, l_bluestore_wal_last);
    b.add_u64_counter(l_bluestore_wal_input_bytes, "input_bytes",
      "Byte count submitted to WAL");
    b.add_u64_counter(l_bluestore_wal_pad_bytes, "pad_bytes",
      "Byte count added as padding when writing to WAL");
    b.add_u64_counter(l_bluestore_wal_output_bytes, "output_bytes",
      "Byte count written by WAL to disk");
    b.add_u64_counter(l_bluestore_wal_wipe_bytes, "wipe_bytes",
      "Byte count written by WAL to disk when wiping");
   b.add_time_avg(l_bluestore_wal_aio_finish_lat, "aio_finish_lat",
    "Average aio_finish latency");

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

void BluestoreWAL::_pad_bl(bufferlist& bl, size_t pad_size)
{
  uint64_t partial = bl.length() % pad_size;
  if (partial) {
    bl.append_zero(pad_size - partial);
    logger->inc(l_bluestore_wal_pad_bytes, pad_size - partial);
  }
}

bool BluestoreWAL::get_write_pos(size_t need, uint64_t* _need_pages)
{
  if (need + thead_size + curpage_pos <= page_size) {
    *_need_pages = 0;
    return true;
  }
  auto psize = page_size - phead_size - thead_size;
  size_t need_pages = round_up_to(need, psize) / psize;
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

  *_need_pages = need_pages;
  return true;
}

size_t BluestoreWAL::wipe_pages(IOContext* ioc)
{
  size_t wiping = 0;
  dout(7) << __func__ << " " << last_wiping_page_seqno << "->"
	  << last_committed_page_seqno
	  << dendl;
  ceph_assert(last_wiping_page_seqno < last_committed_page_seqno);
  wiping = last_committed_page_seqno - last_wiping_page_seqno;
  ceph_assert(wiping < get_total_pages());
  ceph_assert(page_size * wiping <= avail);
  avail -= page_size * wiping;

  auto* buf = calloc(block_size, 1);

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
  return wiping;
}

void BluestoreWAL::notify_store(BlueStore* store,
				uint64_t seqno,
				void* txc)
{
  if (txc) {
    store->txc_wal_finish(seqno, txc);
  }
}

void BluestoreWAL::aio_write(uint64_t off,
  bufferlist& bl,
  IOContext* ioc,
  bool buffered)
{
  bdev->aio_write(off, bl, ioc, false);
}

void BluestoreWAL::aio_submit(IOContext* ioc)
{
  bdev->aio_submit(ioc);
}

void BluestoreWAL::aio_finish(BlueStore* store, Op& op)
{
  mono_clock::time_point t0 = mono_clock::now();
  uint64_t wiped = 0;
  std::unique_lock l(lock);
  op.running = false;
  dout(7) << __func__ << " " << op.transact_seqno << dendl;
  if (op.transact_seqno == min_pending_io_seqno) {

    dout(7) << __func__ << " processing " << op.transact_seqno
	    << " prev seqno " << op.prev_page_seqno
	    << dendl;
    notify_store(store, op.prev_page_seqno, op.txc);
    dout(7) << __func__ << " processed " << op.transact_seqno
            << dendl;

    wiped += op.wiping_pages;
    pending_transactions.erase_and_dispose(
      op,
      DeleteDisposer());
    ++min_pending_io_seqno;
    while (min_pending_io_seqno <= max_pending_io_seqno) {
      auto it = pending_transactions.find(Op(min_pending_io_seqno));
      ceph_assert(it != pending_transactions.end());
      dout(7) << __func__ << " may be processing " << it->transact_seqno
	      << " prev seqno " << it->prev_page_seqno
	      << " running " << it->running
	      << dendl;
      if (it->running) {
	break;
      }

      notify_store(store, it->prev_page_seqno, it->txc);
      dout(7) << __func__ << " processed " << it->transact_seqno
	<< dendl;

      wiped += it->wiping_pages;
      pending_transactions.erase_and_dispose(it,
	DeleteDisposer());
      ++min_pending_io_seqno;
    }
    if (wiped) {
      dout(7) << __func__ << " wiped " << wiped << " "
	      << last_wiped_page_seqno << " <= "
	      << last_wiping_page_seqno
	      << dendl;
      avail += wiped * page_size;
      last_wiped_page_seqno += wiped;
      //FIXME: debug!!!!
      if (!(last_wiped_page_seqno <= last_wiping_page_seqno)) {
        dout(0) << __func__ << " wiped poorly " << wiped << " "
	      << last_wiped_page_seqno << " <= "
	      << last_wiping_page_seqno
	      << dendl;
      }
      ceph_assert(last_wiped_page_seqno <= last_wiping_page_seqno);
      flush_cond.notify_all();
    }
  }
  logger->tinc(l_bluestore_wal_aio_finish_lat, mono_clock::now() - t0);

}
int BluestoreWAL::submit(void* txc, bufferlist& t)
{
  bufferlist bl;
  bluewal_transact_head_t header;

  ceph_assert(txc);

  size_t t_len = t.length();
  dout(7) << __func__ << " transact payload len:" << t_len << dendl;

  header.uuid = uuid;

  logger->inc(l_bluestore_wal_input_bytes, t_len);

  std::unique_lock l(lock);
  // note current page to mark it as exhausted 
  // if page_seqno updates within this func
  auto prev_page_seqno = page_seqno;

  uint64_t need_pages = 0;
  while (!get_write_pos(t_len, &need_pages)) {
    ++num_pending_free;
    dout(7) << __func__ << " - no write pos, waiting: "
            << num_pending_free << dendl;
    flush_cond.wait(l);
    --num_pending_free;
    dout(7) << __func__ << " wait done" << dendl;
  }
  dout(7) << __func__ << " need pages:" << need_pages
	  << " transact len:" << t_len << dendl;
  ++transact_seqno;
  auto* op = new Op(
    this,
    cct,
    transact_seqno,
    0, //no wiping
    txc);
  auto* ioc = &(op->ioc);

  if (likely(!need_pages)) {
    uint64_t offs = curpage_pos + page_offsets[get_page_idx(page_seqno)];

    header.seq = transact_seqno;
    header.len = t_len;

    encode(header, bl);
    bl.claim_append(t);
    _pad_bl(bl, block_size);

    dout(7) << __func__ << " write 0x" << std::hex << offs
      << "~" << bl.length() << std::dec << dendl;

    curpage_pos += bl.length();
    avail -= bl.length();
    logger->inc(l_bluestore_wal_output_bytes, bl.length());
    aio_write(offs, bl, ioc, false);
  } else {
    bluewal_page_head_t pheader;
    size_t t_written = 0;

    // "utilize" page's leftovers
    avail -= page_size - curpage_pos;
    curpage_pos = 0;

    pheader.uuid = uuid;
    pheader.seq = ++page_seqno;
    pheader.following_pages = need_pages;
    encode(pheader, bl);

    header.seq = transact_seqno;
    // put the whole length into the first transaction header
    header.len = t_len;
    // first transaction's header keeps csum for the whole payload
    header.csum = t.crc32c(transact_seqno);
    encode(header, bl);

    size_t page = 0;
    do {
      if (likely(need_pages == 1)) {
        bl.claim_append(t);
      } else {
	size_t l = std::min(t_len - t_written, page_size - phead_size - thead_size);
	if (page != 0) {
	  bl.clear();
	  //it's expected that the previous page has been completely consumed
	  ceph_assert(curpage_pos == page_size);
	  curpage_pos = 0;

	  pheader.seq = ++page_seqno;
	  pheader.following_pages = 0; // this is a marker for "follower" pages,
			               // i.e. ones that do contain non-first part of
				       // a splitted op at the beginning
	  encode(pheader, bl);

	  header.len = l; // note just a portion which fits into the current page
	  header.csum = 0; // don't care about csum of the portion
	  encode(header, bl);
	}
	bufferlist subbuf;
	subbuf.substr_of(t, t_written, l); // FIXME: to avoid copy!!!
	t_written += l;
	bl.claim_append(subbuf);
      }
      _pad_bl(bl, block_size);
      auto offs = curpage_pos + page_offsets[get_page_idx(page_seqno)];
      dout(7) << __func__ << " write 0x" << std::hex << offs
	      << "~" << bl.length() << std::dec << dendl;
      avail -= bl.length();
      curpage_pos += bl.length();
      ++page;
      logger->inc(l_bluestore_wal_output_bytes, bl.length());
      aio_write(offs, bl, ioc, false);
    } while (page < need_pages);
  } // (!need_pages)

  size_t wiping = 0;
  if (last_wiping_page_seqno < last_committed_page_seqno) {
    wiping = wipe_pages(ioc);
  }

  aio_submit(ioc);

  // when huge op is issues (2+ pages) - mark ending page
  // as completely consumed
  // and immediately request completion indication for 
  // new pages - this is a sort of protection from
  // huge op deadlocking the WAL while being uncommitted
  // 
  if (need_pages > 2) {
    avail -= page_size - curpage_pos;
    curpage_pos = page_size;
    // this will cause the next submit call to request the second commit
    // indication for the same page seqno - committed() to handle(ignore) that
    op->prev_page_seqno = page_seqno;

  } else if (prev_page_seqno != page_seqno) {
    op->prev_page_seqno = prev_page_seqno;
  }
  op->wiping_pages = wiping;

  pending_transactions.insert(*op);

  max_pending_io_seqno = transact_seqno;

  dout(7) << __func__ << " op submitted " << op->transact_seqno
    << " wiping " << op->wiping_pages
    << " prev_page_seqno " << op->prev_page_seqno
    << dendl;

  return 0;
}

void BluestoreWAL::submitted(uint64_t submitted_page_seqno,
			     KeyValueDB& db)
{
  if (submitted_page_seqno) {

    dout(7) << __func__ << " seq: " << submitted_page_seqno
            << " last_submitted: " << last_submitted_page_seqno
            << " last_committed: " << last_committed_page_seqno
            << " avail: " << avail
            << " num_pending_free: " << num_pending_free
            << dendl;

    // When submitting multi-page ops we might get second commit 
    // indications for the last page seqno.
    // Just ignore non-first one.
    if (submitted_page_seqno == last_submitted_page_seqno) {
      return;
    }

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

      //awake pending submits only if there is no
      // page wiping in progress as it prevents page from the reuse
      // until the completion.
      if (num_pending_free &&
          last_wiping_page_seqno == last_wiped_page_seqno) {
        flush_cond.notify_all();
      }
    }
  }
}

void BluestoreWAL::shutdown()
{
  //FIXME
}
// =======================================================
