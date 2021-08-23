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

#include <errno.h>
#include <list>

#include "TransparentPG.h"
#include "OSD.h"
#include "messages/MOSDOpReply.h"
#include "common/perf_counters.h"

//#include <charconv>
#include <sstream>
#include <utility>
/*
#include <boost/intrusive_ptr.hpp>
#include <boost/tuple/tuple.hpp>

#include "PG.h"
#include "pg_scrubber.h"
#include "PrimaryLogPG.h"
#include "PrimaryLogScrub.h"
#include "OpRequest.h"
#include "ScrubStore.h"
#include "Session.h"
#include "objclass/objclass.h"
#include "osd/ClassHandler.h"

#include "cls/cas/cls_cas_ops.h"
#include "common/ceph_crypto.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/scrub_types.h"
#include "common/CDC.h"
#include "common/EventTrace.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDBackoff.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGBackfillRemove.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGUpdateLogMissing.h"
#include "messages/MOSDPGUpdateLogMissingReply.h"
#include "messages/MCommandReply.h"
#include "messages/MOSDScrubReserve.h"

#include "include/compat.h"
#include "mon/MonClient.h"
#include "osdc/Objecter.h"
#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_reader.h"
#include "include/ceph_assert.h"  // json_spirit clobbers it
#include "include/rados/rados_types.hpp"
*/
/*#ifdef WITH_LTTNG
#include "tracing/osd.h"
#else
#define tracepoint(...)
#endif*/

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this, osd->whoami, get_osdmap()
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::less;
using std::list;
using std::ostream;
using std::pair;
using std::make_pair;
using std::make_unique;
using std::map;
using std::ostringstream;
using std::set;
using std::string;
using std::string_view;
using std::stringstream;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::Formatter;
using ceph::decode;
using ceph::decode_noclear;
using ceph::encode;
using ceph::encode_destructively;

template <typename T>
static ostream& _prefix(std::ostream* _dout, T* pg) {
  return pg->gen_prefix(*_dout);
}

/*
 * Capture all object state associated with an in-progress read or write.
 */
struct TransparentPG::OpContext {
  OpRequestRef op;
  osd_reqid_t reqid;
  std::vector<OSDOp>* ops;

  /*const ObjectState* obs; // Old objectstate
  const SnapSet* snapset; // Old snapset

  ObjectState new_obs;  // resulting ObjectState
  SnapSet new_snapset;  // resulting SnapSet (in case of a write)
  //pg_stat_t new_stats;  // resulting Stats
  object_stat_sum_t delta_stats;

  bool modify;          // (force) modification (even if op_t is empty)
  bool user_modify;     // user-visible modification
  bool undirty;         // user explicitly un-dirtying this object
  bool cache_operation;     ///< true if this is a cache eviction
  bool ignore_cache;    ///< true if IGNORE_CACHE flag is std::set
  bool ignore_log_op_stats;  // don't log op stats
  bool update_log_only; ///< this is a write that returned an error - just record in pg log for dup detection
  ObjectCleanRegions clean_regions;

  // side effects
  std::list<std::pair<watch_info_t, bool> > watch_connects; ///< new watch + will_ping flag
  std::list<watch_disconnect_t> watch_disconnects; ///< old watch + send_discon
  std::list<notify_info_t> notifies;
  struct NotifyAck {
    std::optional<uint64_t> watch_cookie;
    uint64_t notify_id;
    ceph::buffer::list reply_bl;
    explicit NotifyAck(uint64_t notify_id) : notify_id(notify_id) {}
    NotifyAck(uint64_t notify_id, uint64_t cookie, ceph::buffer::list& rbl)
      : watch_cookie(cookie), notify_id(notify_id) {
      reply_bl = std::move(rbl);
    }
  };
  std::list<NotifyAck> notify_acks;

  uint64_t bytes_written, bytes_read;

  utime_t mtime;
  SnapContext snapc;           // writer snap context
  eversion_t at_version;       // pg's current version pointer
  version_t user_at_version;   // pg's current user version pointer

  /// index of the current subop - only valid inside of do_osd_ops()
  int current_osd_subop_num;
  /// total number of subops processed in this context for cls_cxx_subop_version()
  int processed_subop_count = 0;

  PGTransactionUPtr op_t;
  std::vector<pg_log_entry_t> log;
  std::optional<pg_hit_set_history_t> updated_hset_history;

  interval_set<uint64_t> modified_ranges;
  ObjectContextRef obc;
  ObjectContextRef clone_obc;    // if we created a clone
  ObjectContextRef head_obc;     // if we also update snapset (see trim_object)

  // FIXME: we may want to kill this msgr hint off at some point!
  std::optional<int> data_off = std::nullopt;*/

  TransparentPG* pg = nullptr;

  MOSDOpReply* reply = nullptr;

  /*int num_read;    ///< count read ops
  int num_write;   ///< count update ops

  mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > extra_reqids;
  mempool::osd_pglog::map<uint32_t, int> extra_reqid_return_codes;

  hobject_t new_temp_oid, discard_temp_oid;  ///< temp objects we should start/stop tracking*/

  /*std::list<std::function<void()>> on_applied;
  std::list<std::function<void()>> on_committed;
  std::list<std::function<void()>> on_finish;
  std::list<std::function<void()>> on_success;
  template <typename F>
  void register_on_finish(F&& f) {
    on_finish.emplace_back(std::forward<F>(f));
  }
  template <typename F>
  void register_on_success(F&& f) {
    on_success.emplace_back(std::forward<F>(f));
  }
  template <typename F>
  void register_on_applied(F&& f) {
    on_applied.emplace_back(std::forward<F>(f));
  }
  template <typename F>
  void register_on_commit(F&& f) {
    on_committed.emplace_back(std::forward<F>(f));
  }*/

  bool sent_reply = false;

  // pending async reads <off, len, op_flags> -> <outbl, outr>
/*  std::list<std::pair<boost::tuple<uint64_t, uint64_t, unsigned>,
    std::pair<ceph::buffer::list*, Context*> > > pending_async_reads;
  int inflightreads;
  friend struct OnReadComplete;
  void start_async_reads(PrimaryLogPG* pg);
  void finish_read(PrimaryLogPG* pg);
  bool async_reads_complete() {
    return inflightreads == 0;
  }

  RWState::State lock_type;
  ObcLockManager lock_manager;

  std::map<int, std::unique_ptr<OpFinisher>> op_finishers;*/

  OpContext(const OpContext& other);
  const OpContext& operator=(const OpContext& other);

  OpContext(OpRequestRef _op, osd_reqid_t _reqid, std::vector<OSDOp>* _ops,
    TransparentPG* _pg) :
    op(_op), reqid(_reqid), ops(_ops),
    pg(_pg)
    /*obs(&obc->obs),
    snapset(0),
    new_obs(obs->oi, obs->exists),
    modify(false), user_modify(false), undirty(false), cache_operation(false),
    ignore_cache(false), ignore_log_op_stats(false), update_log_only(false),
    bytes_written(0), bytes_read(0), user_at_version(0),
    current_osd_subop_num(0),
    obc(obc),*/
    /*num_read(0),
    num_write(0),
    sent_reply(false),
    inflightreads(0),
    lock_type(RWState::RWNONE)*/ {
    /*if (obc->ssc) {
      new_snapset = obc->ssc->snapset;
      snapset = &obc->ssc->snapset;
    }*/
  }
  /*OpContext(OpRequestRef _op, osd_reqid_t _reqid,
    std::vector<OSDOp>* _ops, PrimaryLogPG* _pg) :
    op(_op), reqid(_reqid), ops(_ops), obs(NULL), snapset(0),
    modify(false), user_modify(false), undirty(false), cache_operation(false),
    ignore_cache(false), ignore_log_op_stats(false), update_log_only(false),
    bytes_written(0), bytes_read(0), user_at_version(0),
    current_osd_subop_num(0),
    reply(NULL), pg(_pg),
    num_read(0),
    num_write(0),
    inflightreads(0),
    lock_type(RWState::RWNONE) {}
  void reset_obs(ObjectContextRef obc) {
    new_obs = ObjectState(obc->obs.oi, obc->obs.exists);
    if (obc->ssc) {
      new_snapset = obc->ssc->snapset;
      snapset = &obc->ssc->snapset;
    }
  }*/
  ~OpContext() {
    //ceph_assert(!op_t);
    if (reply)
      reply->put();
    /*for (std::list<std::pair<boost::tuple<uint64_t, uint64_t, unsigned>,
      std::pair<ceph::buffer::list*, Context*> > >::iterator i =
      pending_async_reads.begin();
      i != pending_async_reads.end();
      pending_async_reads.erase(i++)) {
      delete i->second.second;
    }*/
  }
  uint64_t get_features() {
    if (op && op->get_req()) {
      return op->get_req()->get_connection()->get_features();
    }
    return -1ull;
  }
};

void TransparentPG::do_op(OpRequestRef& op)
{
  MOSDOp* m = static_cast<MOSDOp*>(op->get_nonconst_req());
  ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
  if (m->finish_decode()) {
    op->reset_desc();   // for TrackedOp
    m->clear_payload();
  }
  dout(0) << __func__ << ": op " << *m << dendl;

  if (m->has_flag(CEPH_OSD_FLAG_PARALLELEXEC)) {
    // not implemented.
    dout(20) << __func__ << ": PARALLELEXEC not implemented " << *m << dendl;
    osd->reply_op_error(op, -EINVAL);
    return;
  }

  {
    int r = op->maybe_init_op_info(*get_osdmap());
    if (r) {
      osd->reply_op_error(op, r);
      return;
    }
  }

  OpContext* ctx = new OpContext(op, m->get_reqid(), &m->ops, this);

  op->mark_started();

  execute_ctx(ctx);
  utime_t prepare_latency = ceph_clock_now();
  prepare_latency -= op->get_dequeued_time();
  osd->logger->tinc(l_osd_op_prepare_lat, prepare_latency);
  if (op->may_read() && op->may_write()) {
    osd->logger->tinc(l_osd_op_rw_prepare_lat, prepare_latency);
  } else if (op->may_read()) {
    osd->logger->tinc(l_osd_op_r_prepare_lat, prepare_latency);
  } else if (op->may_write() || op->may_cache()) {
    osd->logger->tinc(l_osd_op_w_prepare_lat, prepare_latency);
  }
}

void TransparentPG::execute_ctx(OpContext* ctx)
{
//  FUNCTRACE(cct);
  dout(0) << __func__ << " " << ctx << dendl;
/*  ctx->reset_obs(ctx->obc);
  ctx->update_log_only = false; // reset in case finish_copyfrom() is re-running execute_ctx
  OpRequestRef op = ctx->op;
  auto m = op->get_req<MOSDOp>();
  ObjectContextRef obc = ctx->obc;
  const hobject_t& soid = obc->obs.oi.soid;

  // this method must be idempotent since we may call it several times
  // before we finally apply the resulting transaction.
  ctx->op_t.reset(new PGTransaction);

  if (op->may_write() || op->may_cache()) {
    // snap
    if (!(m->has_flag(CEPH_OSD_FLAG_ENFORCE_SNAPC)) &&
      pool.info.is_pool_snaps_mode()) {
      // use pool's snapc
      ctx->snapc = pool.snapc;
    }
    else {
      // client specified snapc
      ctx->snapc.seq = m->get_snap_seq();
      ctx->snapc.snaps = m->get_snaps();
      filter_snapc(ctx->snapc.snaps);
    }
    if ((m->has_flag(CEPH_OSD_FLAG_ORDERSNAP)) &&
      ctx->snapc.seq < obc->ssc->snapset.seq) {
      dout(10) << " ORDERSNAP flag set and snapc seq " << ctx->snapc.seq
	<< " < snapset seq " << obc->ssc->snapset.seq
	<< " on " << obc->obs.oi.soid << dendl;
      reply_ctx(ctx, -EOLDSNAPC);
      return;
    }

    // version
    ctx->at_version = get_next_version();
    ctx->mtime = m->get_mtime();

    dout(10) << __func__ << " " << soid << " " << *ctx->ops
      << " ov " << obc->obs.oi.version << " av " << ctx->at_version
      << " snapc " << ctx->snapc
      << " snapset " << obc->ssc->snapset
      << dendl;
  }
  else {
    dout(10) << __func__ << " " << soid << " " << *ctx->ops
      << " ov " << obc->obs.oi.version
      << dendl;
  }

  if (!ctx->user_at_version)
    ctx->user_at_version = obc->obs.oi.user_version;
  dout(30) << __func__ << " user_at_version " << ctx->user_at_version << dendl;

  {
#ifdef WITH_LTTNG
    osd_reqid_t reqid = ctx->op->get_reqid();
#endif
    tracepoint(osd, prepare_tx_enter, reqid.name._type,
      reqid.name._num, reqid.tid, reqid.inc);
  }
#ifdef HAVE_JAEGER
  if (ctx->op->osd_parent_span) {
    auto execute_span = jaeger_tracing::child_span(__func__, ctx->op->osd_parent_span);
  }
#endif

  int result = prepare_transaction(ctx);

  {
#ifdef WITH_LTTNG
    osd_reqid_t reqid = ctx->op->get_reqid();
#endif
    tracepoint(osd, prepare_tx_exit, reqid.name._type,
      reqid.name._num, reqid.tid, reqid.inc);
  }

  bool pending_async_reads = !ctx->pending_async_reads.empty();
  if (result == -EINPROGRESS || pending_async_reads) {
    // come back later.
    if (pending_async_reads) {
      ceph_assert(pool.info.is_erasure());
      in_progress_async_reads.push_back(make_pair(op, ctx));
      ctx->start_async_reads(this);
    }
    return;
  }

  if (result == -EAGAIN) {
    // clean up after the ctx
    close_op_ctx(ctx);
    return;
  }

  bool ignore_out_data = false;
  if (!ctx->op_t->empty() &&
    op->may_write() &&
    result >= 0) {
    // successful update
    if (ctx->op->allows_returnvec()) {
      // enforce reasonable bound on the return buffer sizes
      for (auto& i : *ctx->ops) {
	if (i.outdata.length() > cct->_conf->osd_max_write_op_reply_len) {
	  dout(10) << __func__ << " op " << i << " outdata overflow" << dendl;
	  result = -EOVERFLOW;  // overall result is overflow
	  i.rval = -EOVERFLOW;
	  i.outdata.clear();
	}
      }
    }
    else {
      // legacy behavior -- zero result and return data etc.
      ignore_out_data = true;
      result = 0;
    }
  }

  // prepare the reply
  ctx->reply = new MOSDOpReply(m, result, get_osdmap_epoch(), 0,
    ignore_out_data);
  dout(20) << __func__ << " alloc reply " << ctx->reply
    << " result " << result << dendl;

  // read or error?
  if ((ctx->op_t->empty() || result < 0) && !ctx->update_log_only) {
    // finish side-effects
    if (result >= 0)
      do_osd_op_effects(ctx, m->get_connection());

    complete_read_ctx(result, ctx);
    return;
  }

  ctx->reply->set_reply_versions(ctx->at_version, ctx->user_at_version);

  ceph_assert(op->may_write() || op->may_cache());

  // trim log?
  recovery_state.update_trim_to();

  // verify that we are doing this in order?
  if (cct->_conf->osd_debug_op_order && m->get_source().is_client() &&
    !pool.info.is_tier() && !pool.info.has_tiers()) {
    map<client_t, ceph_tid_t>& cm = debug_op_order[obc->obs.oi.soid];
    ceph_tid_t t = m->get_tid();
    client_t n = m->get_source().num();
    map<client_t, ceph_tid_t>::iterator p = cm.find(n);
    if (p == cm.end()) {
      dout(20) << " op order client." << n << " tid " << t << " (first)" << dendl;
      cm[n] = t;
    }
    else {
      dout(20) << " op order client." << n << " tid " << t << " last was " << p->second << dendl;
      if (p->second > t) {
	derr << "bad op order, already applied " << p->second << " > this " << t << dendl;
	ceph_abort_msg("out of order op");
      }
      p->second = t;
    }
  }

  if (ctx->update_log_only) {
    if (result >= 0)
      do_osd_op_effects(ctx, m->get_connection());

    dout(20) << __func__ << " update_log_only -- result=" << result << dendl;
    // save just what we need from ctx
    MOSDOpReply* reply = ctx->reply;
    ctx->reply = nullptr;
    reply->get_header().data_off = (ctx->data_off ? *ctx->data_off : 0);

    if (result == -ENOENT) {
      reply->set_enoent_reply_versions(info.last_update,
	info.last_user_version);
    }
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    // append to pg log for dup detection - don't save buffers for now
    record_write_error(op, soid, reply, result,
      ctx->op->allows_returnvec() ? ctx : nullptr);
    close_op_ctx(ctx);
    return;
  }

  // no need to capture PG ref, repop cancel will handle that
  // Can capture the ctx by pointer, it's owned by the repop
  ctx->register_on_commit(
    [m, ctx, this]() {
      if (ctx->op)
	log_op_stats(*ctx->op, ctx->bytes_written, ctx->bytes_read);

      if (m && !ctx->sent_reply) {
	MOSDOpReply* reply = ctx->reply;
	ctx->reply = nullptr;
	reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
	dout(10) << " sending reply on " << *m << " " << reply << dendl;
	osd->send_message_osd_client(reply, m->get_connection());
	ctx->sent_reply = true;
	ctx->op->mark_commit_sent();
      }
    });
  ctx->register_on_success(
    [ctx, this]() {
      do_osd_op_effects(
	ctx,
	ctx->op ? ctx->op->get_req()->get_connection() :
	ConnectionRef());
    });
  ctx->register_on_finish(
    [ctx]() {
      delete ctx;
    });

  // issue replica writes
  ceph_tid_t rep_tid = osd->get_tid();

  RepGather* repop = new_repop(ctx, rep_tid);

  issue_repop(repop, ctx);
  eval_repop(repop);
  repop->put();*/
}
