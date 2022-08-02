// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-a
// vim: ts=8 sw=2 smarttabG
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
#include "Session.h"*/
#include "objclass/objclass.h"
#include "osd/ClassHandler.h"

/*#include "cls/cas/cls_cas_ops.h"
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

static int check_offset_and_length(uint64_t offset, uint64_t length,
  uint64_t max, DoutPrefixProvider* dpp)
{
  if (offset >= max ||
    length > max ||
    offset + length > max) {
    ldpp_dout(dpp, 10) << __func__ << " "
      << "osd_max_object_size: " << max
      << "; Hard limit of object size is 4GB." << dendl;
    return -EFBIG;
  }

  return 0;
}
/*
 * Capture all object state associated with an in-progress read or write.
 */
struct TransparentPG::OpContext : public OpContextBase, public Context {
  OpRequestRef op;
  osd_reqid_t reqid;
  std::vector<OSDOp>* ops;

  //const ObjectState* obs; // Old objectstate
  //const SnapSet* snapset; // Old snapset

  /*ObjectState new_obs;  // resulting ObjectState
  SnapSet new_snapset;  // resulting SnapSet (in case of a write)
  //pg_stat_t new_stats;  // resulting Stats
  */
  object_stat_sum_t delta_stats;

  //bool modify;          // (force) modification (even if op_t is empty)
  bool user_modify;     // user-visible modification
/*  bool undirty;         // user explicitly un-dirtying this object
  bool cache_operation;     ///< true if this is a cache eviction
  bool ignore_cache;    ///< true if IGNORE_CACHE flag is std::set
  bool ignore_log_op_stats;  // don't log op stats
  bool update_log_only; ///< this is a write that returned an error - just record in pg log for dup detection
  ObjectCleanRegions clean_regions;
*/
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

  uint64_t bytes_written = 0;
  uint64_t bytes_read = 0;

  utime_t mtime;
  //SnapContext snapc;             // writer snap context
  eversion_t at_version;           // pg's current version pointer
  version_t user_at_version = 0;   // pg's current user version pointer

  /// index of the current subop - only valid inside of do_osd_ops()
  int current_osd_subop_num = 0;
  /// total number of subops processed in this context for cls_cxx_subop_version()
  int processed_subop_count = 0;

  PGTransactionUPtr op_t;
  /*std::vector<pg_log_entry_t> log;
  std::optional<pg_hit_set_history_t> updated_hset_history;

  interval_set<uint64_t> modified_ranges;*/
  /*ObjectContextRef obc;
  ObjectContextRef clone_obc;    // if we created a clone
  ObjectContextRef head_obc;     // if we also update snapset (see trim_object)
*/
  // FIXME: we may want to kill this msgr hint off at some point!
  std::optional<int> data_off = std::nullopt;

  TransparentPG* pg = nullptr;

  MOSDOpReply* reply = nullptr;

  int num_read;    ///< count read ops
  int num_write;   ///< count update ops

  /*mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > extra_reqids;
  mempool::osd_pglog::map<uint32_t, int> extra_reqid_return_codes;

  hobject_t new_temp_oid, discard_temp_oid;  ///< temp objects we should start/stop tracking

  std::list<std::function<void()>> on_applied;*/
  std::list<std::function<void()>> on_committed;
  /*std::list<std::function<void()>> on_finish;
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
  }*/
  template <typename F>
  void register_on_commit(F&& f) {
    on_committed.emplace_back(std::forward<F>(f));
  }

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

  std::map<int, std::unique_ptr<OpFinisher>> op_finishers;

  OpContext(const OpContext& other);
  const OpContext& operator=(const OpContext& other);*/

  OpContext(OpRequestRef _op, osd_reqid_t _reqid, std::vector<OSDOp>* _ops,
    ObjectContextRef& obc,
    TransparentPG* _pg) :
    OpContextBase(obc),
    op(_op), reqid(_reqid), ops(_ops),
    /*obs(&obc->obs),
    snapset(0),
    new_obs(obs->oi, obs->exists),
    modify(false), user_modify(false), undirty(false), cache_operation(false),
    ignore_cache(false), ignore_log_op_stats(false), update_log_only(false),
    bytes_written(0), bytes_read(0), user_at_version(0),
    current_osd_subop_num(0),
    obc(obc),*/
    pg(_pg)
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
    lock_type(RWState::RWNONE) {}*/
  void reset_obs(ObjectContextRef obc) {
    new_obs = ObjectState(obc->obs.oi, obc->obs.exists);
    /*if (obc->ssc) {
      new_snapset = obc->ssc->snapset;
      snapset = &obc->ssc->snapset;
    }*/
  }
  ~OpContext() override {
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
  const hobject_t& get_hobj() const {
    const MOSDOp* m = static_cast<const MOSDOp*>(op->get_req());
    ceph_assert(m->get_snapid() != CEPH_SNAPDIR); //SNAPDIR handling not implemented, see PrimaryLogPG
    return m->get_hobj();
  }
  PG* get_pg() override {
    return pg;
  }
  OpRequestRef get_op() override {
    return op;
  }
  int get_processed_subop_count() const override {
    return 0;
  }
protected:
  void finish(int) override {
    for (auto& p : on_committed) {
      p();
    }
  }
};

//FIXME: in use?
struct OpFinisher {
  virtual ~OpFinisher() {
  }

  virtual int execute() = 0;
};

void TransparentPG::maybe_create_new_object(
  OpContext *ctx,
  bool ignore_transaction)
{
  ObjectState& obs = ctx->new_obs;
  if (!obs.exists) {
    ctx->delta_stats.num_objects++;
    obs.exists = true;
    ceph_assert(!obs.oi.is_whiteout());
    obs.oi.new_object();
    if (!ignore_transaction)
      ctx->op_t->create(obs.oi.soid);
  } else if (obs.oi.is_whiteout()) {
    dout(10) << __func__ << " clearing whiteout on " << obs.oi.soid << dendl;
    ctx->new_obs.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    --ctx->delta_stats.num_whiteouts;
  }
}

void TransparentPG::do_op(OpRequestRef& op)
{
  MOSDOp* m = static_cast<MOSDOp*>(op->get_nonconst_req());
  ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
  if (m->finish_decode()) {
    op->reset_desc();   // for TrackedOp
    m->clear_payload();
  }
  dout(10) << __func__ << ": op " << *m << dendl;

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
  if (op->includes_pg_op()) {
    return do_pg_op(op);
  }

  ObjectContextRef obc;
  bool can_create = op->may_write();
  hobject_t missing_oid;

  const hobject_t& oid = m->get_hobj();

  int r = find_object_context(
    oid, &obc, can_create,
    m->has_flag(CEPH_OSD_FLAG_MAP_SNAP_CLONE),
    &missing_oid);

/*  if (r == -EAGAIN) {
    // If we're not the primary of this OSD, we just return -EAGAIN. Otherwise,
    // we have to wait for the object.
    if (is_primary()) {
      // missing the specific snap we need; requeue and wait.
      ceph_assert(!op->may_write()); // only happens on a read/cache
      wait_for_unreadable_object(missing_oid, op);
      return;
    }
  } else if (r == 0) {
    if (is_unreadable_object(obc->obs.oi.soid)) {
      dout(10) << __func__ << ": clone " << obc->obs.oi.soid
               << " is unreadable, waiting" << dendl;
      wait_for_unreadable_object(obc->obs.oi.soid, op);
      return;
    }

    // degraded object?  (the check above was for head; this could be a clone)
    if (write_ordered &&
        obc->obs.oi.soid.snap != CEPH_NOSNAP &&
        is_degraded_or_backfilling_object(obc->obs.oi.soid)) {
      dout(10) << __func__ << ": clone " << obc->obs.oi.soid
               << " is degraded, waiting" << dendl;
      wait_for_degraded_object(obc->obs.oi.soid, op);
      return;
    }
  }*/
  if (r && (r != -ENOENT || !obc)) {
    // copy the reqids for copy get on ENOENT
    /*if (r == -ENOENT &&
        (m->ops[0].op.op == CEPH_OSD_OP_COPY_GET)) {
      fill_in_copy_get_noent(op, oid, m->ops[0]);
      return;
    }*/
    dout(20) << __func__ << ": find_object_context got error " << r << dendl;
    /*if (op->may_write() &&
        get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      record_write_error(op, oid, nullptr, r);
    } else */
    {
      osd->reply_op_error(op, r);
    }
    return;
  }

  dout(25) << __func__ << " oi " << obc->obs.oi << dendl;

  OpContext* ctx = new OpContext(op, m->get_reqid(), &m->ops, obc, this);

  if ((op->may_read()) && (obc->obs.oi.is_lost())) {
    // This object is lost. Reading from it returns an error.
    dout(20) << __func__ << ": object " << obc->obs.oi.soid
         << " is lost" << dendl;
    reply_ctx(ctx, -ENFILE);
    return;
  }
  if (!op->may_write() &&
      !op->may_cache() &&
      !obc->obs.exists) {
    reply_ctx(ctx, -ENOENT);
    return;
  }

  op->mark_started();

  if (!op->may_write() &&
      !op->may_cache() &&
      !obc->obs.exists) {
    reply_ctx(ctx, -ENOENT);
    return;
  }

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
  dout(10) << __func__ << " " << ctx << dendl;
  ctx->reset_obs(ctx->obc);
  //ctx->update_log_only = false; // reset in case finish_copyfrom() is re-running execute_ctx
  OpRequestRef op = ctx->op;
  auto m = op->get_req<MOSDOp>();
  ObjectContextRef obc = ctx->obc;
  const hobject_t& soid = obc->obs.oi.soid;

  // this method must be idempotent since we may call it several times
  // before we finally apply the resulting transaction.
  ctx->op_t.reset(new PGTransaction);

  if (op->may_write() || op->may_cache()) {
    /*// snap
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
    }*/

    // version
    ctx->at_version = get_next_version();
    ctx->mtime = m->get_mtime();

    dout(10) << __func__ << " " << soid << " " << *ctx->ops
      << " ov " << obc->obs.oi.version
      << " av " << ctx->at_version
      //<< " snapc " << ctx->snapc
      //<< " snapset " << obc->ssc->snapset
      << dendl;
  } else {
    dout(10) << __func__ << " " << soid << " " << *ctx->ops
      << " ov " << obc->obs.oi.version
      << dendl;
  }

  if (!ctx->user_at_version)
    ctx->user_at_version = obc->obs.oi.user_version;
  dout(30) << __func__ << " user_at_version " << ctx->user_at_version << dendl;

  /*{
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
#endif*/

  int result = prepare_transaction(ctx);

  /*{
#ifdef WITH_LTTNG
    osd_reqid_t reqid = ctx->op->get_reqid();
#endif
    tracepoint(osd, prepare_tx_exit, reqid.name._type,
      reqid.name._num, reqid.tid, reqid.inc);
  }*/

  /*bool pending_async_reads = !ctx->pending_async_reads.empty();
  if (result == -EINPROGRESS || pending_async_reads) {
    // come back later.
    if (pending_async_reads) {
      ceph_assert(pool.info.is_erasure());
      in_progress_async_reads.push_back(make_pair(op, ctx));
      ctx->start_async_reads(this);
    }
    return;
  }*/

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
    } else {
      // legacy behavior -- zero result and return data etc.
      ignore_out_data = true;
      result = 0;
    }
  }

  // prepare the reply
  ctx->reply = new MOSDOpReply(m, result, get_osdmap_epoch(), 0,
    ignore_out_data);
  dout(20) << __func__ << " alloc reply " << ctx->reply
    << " result " << result
    << " op_t->empty(): " << ctx->op_t->empty()
    << dendl;

  // read or error?
  if ((ctx->op_t->empty() || result < 0) /*&& !ctx->update_log_only*/) {
    // finish side-effects
    if (result >= 0)
      do_osd_op_effects(ctx, m->get_connection());

    complete_read_ctx(result, ctx);
    return;
  }

  ctx->reply->set_reply_versions(ctx->at_version, ctx->user_at_version);

  ceph_assert(op->may_write() || op->may_cache());

  /*// trim log?
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
  }*/

  /*if (ctx->update_log_only) {
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
  }*/

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
      do_osd_op_effects(
	ctx,
	ctx->op ? ctx->op->get_req()->get_connection() :
	ConnectionRef());
    });
  /*ctx->register_on_success(
    [ctx, this]() {
      do_osd_op_effects(
	ctx,
	ctx->op ? ctx->op->get_req()->get_connection() :
	ConnectionRef());
    });
  ctx->register_on_finish(
    [ctx]() {
      delete ctx;
    });*/

  // issue replica writes
  ceph_tid_t rep_tid = osd->get_tid();

  /*RepGather* repop = new_repop(ctx, rep_tid);

  issue_repop(repop, ctx);
  eval_repop(repop);
  repop->put();*/

  eversion_t dummy_eversion;
  std::vector<pg_log_entry_t> dummy_log;
  std::optional<pg_hit_set_history_t> dummy_updated_bset_history;
  pgbackend->submit_transaction(
    soid,
    ctx->delta_stats,
    ctx->at_version,
    std::move(ctx->op_t),
    dummy_eversion, //recovery_state.get_pg_trim_to(),
    dummy_eversion, //recovery_state.get_min_last_complete_ondisk(),
    std::move(dummy_log), //ctx->log
    dummy_updated_bset_history,  //ctx->updated_hset_history,
    ctx,
    rep_tid,
    ctx->reqid,
    ctx->op);

  publish_stats_to_osd();
}

void TransparentPG::finish_ctx(OpContext* ctx, int log_op_type, int result)
{
  const hobject_t& soid = ctx->get_hobj();
  dout(20) << __func__ << " " << soid << " " << ctx
    << " op " << pg_log_entry_t::get_op_name(log_op_type)
    << dendl;
  utime_t now = ceph_clock_now();

/*#ifdef HAVE_JAEGER
  if (ctx->op->osd_parent_span) {
    auto finish_ctx_span = jaeger_tracing::child_span(__func__, ctx->op->osd_parent_span);
  }
#endif
  // Drop the reference if deduped chunk is modified
  if (ctx->new_obs.oi.is_dirty() &&
    (ctx->obs->oi.has_manifest() && ctx->obs->oi.manifest.is_chunked()) &&
    !ctx->cache_operation &&
    log_op_type != pg_log_entry_t::PROMOTE) {
    update_chunk_map_by_dirty(ctx);
    // If a clone is creating, ignore dropping the reference for manifest object
    if (!ctx->delta_stats.num_object_clones) {
      dec_refcount_by_dirty(ctx);
    }
  }*/

  //FIXME: we set user_modify when handling OP_CALL, need to handle here as well?
  /*// finish and log the op.
  if (ctx->user_modify) {
    // update the user_version for any modify ops, except for the watch op
    ctx->user_at_version = std::max(info.last_user_version, ctx->new_obs.oi.user_version) + 1;*/
    /* In order for new clients and old clients to interoperate properly
     * when exchanging versions, we need to lower bound the user_version
     * (which our new clients pay proper attention to)
     * by the at_version (which is all the old clients can ever see). */
/*    if (ctx->at_version.version > ctx->user_at_version)
      ctx->user_at_version = ctx->at_version.version;
    ctx->new_obs.oi.user_version = ctx->user_at_version;
  }*/
  ctx->bytes_written = ctx->op_t->get_bytes_written();

  if (ctx->new_obs.exists) {
    ctx->new_obs.oi.version = ctx->at_version;
    ctx->new_obs.oi.prior_version = ctx->obs->oi.version;
    ctx->new_obs.oi.last_reqid = ctx->reqid;
    if (ctx->mtime != utime_t()) {
      ctx->new_obs.oi.mtime = ctx->mtime;
      dout(10) << " set mtime to " << ctx->new_obs.oi.mtime << dendl;
      ctx->new_obs.oi.local_mtime = now;
    } else {
      dout(10) << " mtime unchanged at " << ctx->new_obs.oi.mtime << dendl;
    }

    // object_info_t
    map <string, bufferlist, less<>> attrs;
    bufferlist bv(sizeof(ctx->new_obs.oi));
    encode(ctx->new_obs.oi, bv,
      get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    attrs[OI_ATTR] = std::move(bv);

    /*// snapset
    if (soid.snap == CEPH_NOSNAP) {
      dout(10) << " final snapset " << ctx->new_snapset
	<< " in " << soid << dendl;
      bufferlist bss;
      encode(ctx->new_snapset, bss);
      attrs[SS_ATTR] = std::move(bss);
    } else {
      dout(10) << " no snapset (this is a clone)" << dendl;
    }*/
    ctx->op_t->setattrs(soid, attrs);
  } else {
    // reset cached oi
    ctx->new_obs.oi = object_info_t(ctx->obc->obs.oi.soid);
  }

  /*// append to log
  ctx->log.push_back(
    pg_log_entry_t(log_op_type, soid, ctx->at_version,
      ctx->obs->oi.version,
      ctx->user_at_version, ctx->reqid,
      ctx->mtime,
      (ctx->op && ctx->op->allows_returnvec()) ? result : 0));
  if (ctx->op && ctx->op->allows_returnvec()) {
    // also the per-op values
    ctx->log.back().set_op_returns(*ctx->ops);
    dout(20) << __func__ << " op_returns " << ctx->log.back().op_returns
      << dendl;
  }

  ctx->log.back().clean_regions = ctx->clean_regions;
  dout(20) << __func__ << " object " << soid << " marks clean_regions " << ctx->log.back().clean_regions << dendl;

  if (soid.snap < CEPH_NOSNAP) {
    switch (log_op_type) {
    case pg_log_entry_t::MODIFY:
    case pg_log_entry_t::PROMOTE:
    case pg_log_entry_t::CLEAN:
      dout(20) << __func__ << " encoding snaps from " << ctx->new_snapset
	<< dendl;
      encode(ctx->new_snapset.clone_snaps[soid.snap], ctx->log.back().snaps);
      break;
    default:
      break;
    }
  }

  if (!ctx->extra_reqids.empty()) {
    dout(20) << __func__ << "  extra_reqids " << ctx->extra_reqids << " "
      << ctx->extra_reqid_return_codes << dendl;
    ctx->log.back().extra_reqids.swap(ctx->extra_reqids);
    ctx->log.back().extra_reqid_return_codes.swap(ctx->extra_reqid_return_codes);
  }
*/
  // apply new object state.
  ctx->obc->obs = ctx->new_obs;

/*  if (soid.is_head() && !ctx->obc->obs.exists) {
    ctx->obc->ssc->exists = false;
    ctx->obc->ssc->snapset = SnapSet();
  } else {
    ctx->obc->ssc->exists = true;
    ctx->obc->ssc->snapset = ctx->new_snapset;
  }*/
}

int TransparentPG::prepare_transaction(OpContext* ctx)
{
  ceph_assert(!ctx->ops->empty());

  /*// valid snap context?
  if (!ctx->snapc.is_valid()) {
    dout(10) << " invalid snapc " << ctx->snapc << dendl;
    return -EINVAL;
  }*/

  // prepare the actual mutation
  int result = do_osd_ops(ctx, *ctx->ops);
  if (result < 0) {
    /*if (ctx->op->may_write() &&
      get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      // need to save the error code in the pg log, to detect dup ops,
      // but do nothing else
      ctx->update_log_only = true;
    }*/
    return result;
  }

  // read-op?  write-op noop? done?
  if (ctx->op_t->empty() /*&& !ctx->modify*/) {
    /*if (ctx->pending_async_reads.empty())
      unstable_stats.add(ctx->delta_stats);
    if (ctx->op->may_write() &&
      get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      ctx->update_log_only = true;
    }*/
    return result;
  }
  /*if (ctx->op_t->empty() && !ctx->modify) {
    if (ctx->pending_async_reads.empty())
      unstable_stats.add(ctx->delta_stats);
    if (ctx->op->may_write() &&
      get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      ctx->update_log_only = true;
    }
    return result;
  }*/
/*FIXME???
  // check for full
  if ((ctx->delta_stats.num_bytes > 0 ||
    ctx->delta_stats.num_objects > 0) &&  // FIXME: keys?
    pool.info.has_flag(pg_pool_t::FLAG_FULL)) {
    auto m = ctx->op->get_req<MOSDOp>();
    if (ctx->reqid.name.is_mds() ||   // FIXME: ignore MDS for now
      m->has_flag(CEPH_OSD_FLAG_FULL_FORCE)) {
      dout(20) << __func__ << " full, but proceeding due to FULL_FORCE or MDS"
	<< dendl;
    } else if (m->has_flag(CEPH_OSD_FLAG_FULL_TRY)) {
      // they tried, they failed.
      dout(20) << __func__ << " full, replying to FULL_TRY op" << dendl;
      return pool.info.has_flag(pg_pool_t::FLAG_FULL_QUOTA) ? -EDQUOT : -ENOSPC;
    } else {
      // drop request
      dout(20) << __func__ << " full, dropping request (bad client)" << dendl;
      return -EAGAIN;
    }
  }*/

  /*const hobject_t& soid = ctx->obs->oi.soid;
  // clone, if necessary
  if (soid.snap == CEPH_NOSNAP)
    make_writeable(ctx);*/

  finish_ctx(ctx,
    /*ctx->new_obs.exists ? pg_log_entry_t::MODIFY :
      pg_log_entry_t::DELETE,*/
    0,
    result);

  return result;
}

void TransparentPG::complete_read_ctx(int result, OpContext* ctx)
{
  auto m = ctx->op->get_req<MOSDOp>();
  //ceph_assert(ctx->async_reads_complete());

  for (auto p = ctx->ops->begin();
    p != ctx->ops->end() && result >= 0; ++p) {
    if (p->rval < 0 && !(p->op.flags & CEPH_OSD_OP_FLAG_FAILOK)) {
      result = p->rval;
      break;
    }
    ctx->bytes_read += p->outdata.length();
  }
  ctx->reply->get_header().data_off = (ctx->data_off ? *ctx->data_off : 0);

  MOSDOpReply* reply = ctx->reply;
  ctx->reply = nullptr;

  if (result >= 0) {
    /*if (!ctx->ignore_log_op_stats)*/ {
      log_op_stats(*ctx->op, ctx->bytes_written, ctx->bytes_read);

      publish_stats_to_osd();
    }

    /* FIXME may be need to sort out version handling here
    // on read, return the current object version
    if (ctx->obs) {
      reply->set_reply_versions(eversion_t(), ctx->obs->oi.user_version);
    } else*/ {
      reply->set_reply_versions(eversion_t(), ctx->user_at_version);
    }
  } else if (result == -ENOENT) {
    // on ENOENT, set a floor for what the next user version will be.
    reply->set_enoent_reply_versions(info.last_update, info.last_user_version);
  }

  reply->set_result(result);
  reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  osd->send_message_osd_client(reply, m->get_connection());
  close_op_ctx(ctx);
}

void TransparentPG::close_op_ctx(OpContext* ctx) {
  //release_object_locks(ctx->lock_manager);
  dout(10) << __func__ << " " << ctx << dendl;
  ctx->op_t.reset();
/*  for (auto p = ctx->on_finish.begin(); p != ctx->on_finish.end();
    ctx->on_finish.erase(p++)) {
    (*p)();
  }*/
  delete ctx;
}

void TransparentPG::reply_ctx(OpContext *ctx, int r)
{
  if (ctx->op)
    osd->reply_op_error(ctx->op, r);
  close_op_ctx(ctx);
}

void TransparentPG::log_op_stats(const OpRequest& op,
  const uint64_t inb,
  const uint64_t outb)
{
  auto m = op.get_req<MOSDOp>();
  const utime_t now = ceph_clock_now();

  const utime_t latency = now - m->get_recv_stamp();
  const utime_t process_latency = now - op.get_dequeued_time();

  osd->logger->inc(l_osd_op);

  osd->logger->inc(l_osd_op_outb, outb);
  osd->logger->inc(l_osd_op_inb, inb);
  osd->logger->tinc(l_osd_op_lat, latency);
  osd->logger->tinc(l_osd_op_process_lat, process_latency);

  if (op.may_read() && op.may_write()) {
    osd->logger->inc(l_osd_op_rw);
    osd->logger->inc(l_osd_op_rw_inb, inb);
    osd->logger->inc(l_osd_op_rw_outb, outb);
    osd->logger->tinc(l_osd_op_rw_lat, latency);
    osd->logger->hinc(l_osd_op_rw_lat_inb_hist, latency.to_nsec(), inb);
    osd->logger->hinc(l_osd_op_rw_lat_outb_hist, latency.to_nsec(), outb);
    osd->logger->tinc(l_osd_op_rw_process_lat, process_latency);
  } else if (op.may_read()) {
    osd->logger->inc(l_osd_op_r);
    osd->logger->inc(l_osd_op_r_outb, outb);
    osd->logger->tinc(l_osd_op_r_lat, latency);
    osd->logger->hinc(l_osd_op_r_lat_outb_hist, latency.to_nsec(), outb);
    osd->logger->tinc(l_osd_op_r_process_lat, process_latency);
  } else if (op.may_write() || op.may_cache()) {
    osd->logger->inc(l_osd_op_w);
    osd->logger->inc(l_osd_op_w_inb, inb);
    osd->logger->tinc(l_osd_op_w_lat, latency);
    osd->logger->hinc(l_osd_op_w_lat_inb_hist, latency.to_nsec(), inb);
    osd->logger->tinc(l_osd_op_w_process_lat, process_latency);
  } else {
    ceph_abort();
  }

  dout(15) << "transparent log_op_stats " << *m
    << " inb " << inb
    << " outb " << outb
    << " lat " << latency << dendl;

  /*if (m_dynamic_perf_stats.is_enabled()) {
    m_dynamic_perf_stats.add(osd, info, op, inb, outb, latency);
  }*/
}

ObjectContextRef TransparentPG::create_object_context(const object_info_t& oi)
{
  ObjectContextRef obc(object_contexts.lookup_or_create(oi.soid));
  ceph_assert(obc->destructor_callback == NULL);
  //FIXME ??? obc->destructor_callback = new C_PG_ObjectContext(this, obc.get());
  obc->obs.oi = oi;
  obc->obs.exists = false;
  obc->ssc = nullptr;
  dout(10) << "create_object_context " << (void*)obc.get() << " " << oi.soid << " " << dendl;
  if (is_active())
    populate_obc_watchers(obc);
  return obc;
}

// -------------------------------------------------------

void TransparentPG::get_watchers(list<obj_watch_item_t> *ls)
{
  std::scoped_lock l{*this};
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    ObjectContextRef obc(i.second);
    get_obc_watchers(obc, *ls);
  }
}

void TransparentPG::get_obc_watchers(ObjectContextRef obc, list<obj_watch_item_t> &pg_watchers)
{
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
         obc->watchers.begin();
        j != obc->watchers.end();
        ++j) {
    obj_watch_item_t owi;

    owi.obj = obc->obs.oi.soid;
    owi.wi.addr = j->second->get_peer_addr();
    owi.wi.name = j->second->get_entity();
    owi.wi.cookie = j->second->get_cookie();
    owi.wi.timeout_seconds = j->second->get_timeout();

    dout(30) << "watch: Found oid=" << owi.obj << " addr=" << owi.wi.addr
      << " name=" << owi.wi.name << " cookie=" << owi.wi.cookie << dendl;

    pg_watchers.push_back(owi);
  }
}

void TransparentPG::check_blocklisted_watchers()
{
  dout(20) << "TransparentPG::check_blocklisted_watchers for pg " << get_pgid() << dendl;
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i))
    check_blocklisted_obc_watchers(i.second);
}

void TransparentPG::check_blocklisted_obc_watchers(ObjectContextRef obc)
{
  dout(20) << "TransparentPG::check_blocklisted_obc_watchers for obc " << obc->obs.oi.soid << dendl;
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator k =
         obc->watchers.begin();
        k != obc->watchers.end();
        ) {
    //Advance iterator now so handle_watch_timeout() can erase element
    map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j = k++;
    dout(30) << "watch: Found " << j->second->get_entity() << " cookie " << j->second->get_cookie() << dendl;
    entity_addr_t ea = j->second->get_peer_addr();
    dout(30) << "watch: Check entity_addr_t " << ea << dendl;
    if (get_osdmap()->is_blocklisted(ea)) {
      dout(10) << "watch: Found blocklisted watcher for " << ea << dendl;
      ceph_assert(j->second->get_pg() == this);
      j->second->unregister_cb();
      handle_watch_timeout(j->second);
    }
  }
}

void TransparentPG::populate_obc_watchers(ObjectContextRef obc)
{
/*  ceph_assert(is_primary() && is_active());
  auto it_objects = recovery_state.get_pg_log().get_log().objects.find(obc->obs.oi.soid);
  ceph_assert((recovering.count(obc->obs.oi.soid) ||
          !is_missing_object(obc->obs.oi.soid)) ||
         (it_objects != recovery_state.get_pg_log().get_log().objects.end() && // or this is a revert... see recover_primary()
          it_objects->second->op ==
            pg_log_entry_t::LOST_REVERT &&
          it_objects->second->reverting_to ==
            obc->obs.oi.version));*/

  dout(10) << "populate_obc_watchers " << obc->obs.oi.soid << dendl;
  ceph_assert(obc->watchers.empty());
  // populate unconnected_watchers
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator p =
        obc->obs.oi.watchers.begin();
       p != obc->obs.oi.watchers.end();
       ++p) {
    utime_t expire = info.stats.last_became_active;
    expire += p->second.timeout_seconds;
    dout(10) << "  unconnected watcher " << p->first << " will expire " << expire << dendl;
    WatchRef watch(
      Watch::makeWatchRef(
        this, osd, obc, p->second.timeout_seconds, p->first.first,
        p->first.second, p->second.addr));
    watch->disconnect();
    obc->watchers.insert(
      make_pair(
        make_pair(p->first.first, p->first.second),
        watch));
  }
  // Look for watchers from blocklisted clients and drop
  check_blocklisted_obc_watchers(obc);
}

void TransparentPG::handle_watch_timeout(WatchRef watch)
{
  ObjectContextRef obc = watch->get_obc(); // handle_watch_timeout owns this ref
  dout(10) << "handle_watch_timeout obc " << obc << dendl;

  if (!is_active()) {
    dout(10) << "handle_watch_timeout not active, no-op" << dendl;
    return;
  }
  if (!obc->obs.exists) {
    dout(10) << __func__ << " object " << obc->obs.oi.soid << " dne" << dendl;
    return;
  }
/*  if (is_degraded_or_backfilling_object(obc->obs.oi.soid)) {
    callbacks_for_degraded_object[obc->obs.oi.soid].push_back(
      watch->get_delayed_cb()
      );
    dout(10) << "handle_watch_timeout waiting for degraded on obj "
             << obc->obs.oi.soid
             << dendl;
    return;
  }

  if (m_scrubber->write_blocked_by_scrub(obc->obs.oi.soid)) {
    dout(10) << "handle_watch_timeout waiting for scrub on obj "
             << obc->obs.oi.soid
             << dendl;
    m_scrubber->add_callback(
      watch->get_delayed_cb() // This callback!
      );
    return;
  }*/

  list<watch_disconnect_t> watch_disconnects = {
    watch_disconnect_t(watch->get_cookie(), watch->get_entity(), true)
  };
  complete_disconnect_watches(obc, watch_disconnects);

/*  OpContextUPtr ctx = simple_opc_create(obc);
  ctx->at_version = get_next_version();

  object_info_t& oi = ctx->new_obs.oi;
  oi.watchers.erase(make_pair(watch->get_cookie(),
                              watch->get_entity()));

  list<watch_disconnect_t> watch_disconnects = {
    watch_disconnect_t(watch->get_cookie(), watch->get_entity(), true)
  };
  ctx->register_on_success(
    [this, obc, watch_disconnects]() {
      complete_disconnect_watches(obc, watch_disconnects);
    });


  PGTransaction *t = ctx->op_t.get();
  ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, obc->obs.oi.soid,
                                    ctx->at_version,
                                    oi.version,
                                    0,
                                    osd_reqid_t(), ctx->mtime, 0));

  oi.prior_version = obc->obs.oi.version;
  oi.version = ctx->at_version;
  bufferlist bl;
  encode(oi, bl, get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
  t->setattr(obc->obs.oi.soid, OI_ATTR, bl);

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;

  // no ctx->delta_stats
  simple_opc_submit(std::move(ctx));*/
}

ObjectContextRef TransparentPG::get_object_context(
  const hobject_t& soid,
  bool can_create,
  const map<string, bufferlist, less<>> *attrs)
{
/*  auto it_objects = recovery_state.get_pg_log().get_log().objects.find(soid);
  ceph_assert(
    attrs || !recovery_state.get_pg_log().get_missing().is_missing(soid) ||
    // or this is a revert... see recover_primary()
    (it_objects != recovery_state.get_pg_log().get_log().objects.end() &&
      it_objects->second->op ==
      pg_log_entry_t::LOST_REVERT));*/
  ObjectContextRef obc = object_contexts.lookup(soid);
  osd->logger->inc(l_osd_object_ctx_cache_total);
  if (obc) {
    osd->logger->inc(l_osd_object_ctx_cache_hit);
    dout(10) << __func__ << ": found obc in cache: " << obc
             << dendl;
  } else {
    dout(10) << __func__ << ": obc NOT found in cache: " << soid << dendl;
    // check disk
    bufferlist bv;
    if (attrs) {
      auto it_oi = attrs->find(OI_ATTR);
      ceph_assert(it_oi != attrs->end());
      bv = it_oi->second;
    } else {
      int r = pgbackend->objects_get_attr(soid, OI_ATTR, &bv);
      if (r < 0) {
        if (!can_create) {
          dout(10) << __func__ << ": no obc for soid "
                   << soid << " and !can_create"
                   << dendl;
          return ObjectContextRef();   // -ENOENT!
        }

        dout(10) << __func__ << ": no obc for soid "
                 << soid << " but can_create"
                 << dendl;
        // new object.
        object_info_t oi(soid);
        /*SnapSetContext *ssc = get_snapset_context(
          soid, true, 0, false);
        ceph_assert(ssc);*/
        obc = create_object_context(oi);
        dout(10) << __func__ << ": " << obc << " " << soid
                 << " " << obc->rwstate
                 << " oi: " << obc->obs.oi
                 << dendl;
        return obc;
      }
    }
    object_info_t oi;
    try {
      bufferlist::const_iterator bliter = bv.begin();
      decode(oi, bliter);
    } catch (...) {
      dout(0) << __func__ << ": obc corrupt: " << soid << dendl;
      return ObjectContextRef();   // -ENOENT!
    }

    ceph_assert(oi.soid.pool == (int64_t)info.pgid.pool());

    obc = object_contexts.lookup_or_create(oi.soid);
    //FIXME? obc->destructor_callback = new C_PG_ObjectContext(this, obc.get());
    obc->obs.oi = oi;
    obc->obs.exists = true;

    if (is_primary() && is_active())
      populate_obc_watchers(obc);

    dout(10) << __func__ << ": creating obc from disk: " << obc
             << dendl;
  }

  dout(10) << __func__ << ": " << obc << " " << soid
           << " " << obc->rwstate
           << " oi: " << obc->obs.oi
           << " exists: " << (int)obc->obs.exists
           << dendl;
  return obc;
}

int TransparentPG::find_object_context(const hobject_t& oid,
                                       ObjectContextRef *pobc,
                                       bool can_create,
                                       bool map_snapid_to_clone,
                                       hobject_t *pmissing)
{
  FUNCTRACE(cct);
  ceph_assert(oid.pool == static_cast<int64_t>(info.pgid.pool()));
  // want the head?
  ceph_assert(oid.snap == CEPH_NOSNAP);
  ObjectContextRef obc = get_object_context(oid, can_create);
  if (!obc) {
    if (pmissing)
      *pmissing = oid;
    return -ENOENT;
  }
  dout(10) << __func__ << " " << oid
    << " @" << oid.snap
    << " oi=" << obc->obs.oi
    << dendl;
  *pobc = obc;
  return 0;
}

int TransparentPG::getattrs_maybe_cache(
  //ObjectContextRef obc,
  const hobject_t& soid,
  map<string, bufferlist, less<>>* out)
{
  int r = 0;
  ceph_assert(out);
  r = pgbackend->objects_get_attrs(soid, out);
  map<string, bufferlist, less<>> tmp;
  for (auto& [key, val] : *out) {
    if (key.size() > 1 && key[0] == '_') {
      tmp[key.substr(1, key.size())] = std::move(val);
    }
  }
  tmp.swap(*out);
  return r;
}

int TransparentPG::do_writesame(OpContext* ctx, OSDOp& osd_op)
{
  ceph_osd_op& op = osd_op.op;
  vector<OSDOp> write_ops(1);
  OSDOp& write_op = write_ops[0];
  uint64_t write_length = op.writesame.length;
  int result = 0;

  if (!write_length)
    return 0;

  if (!op.writesame.data_length || write_length % op.writesame.data_length)
    return -EINVAL;

  if (op.writesame.data_length != osd_op.indata.length()) {
    derr << "invalid length ws data length " << op.writesame.data_length << " actual len " << osd_op.indata.length() << dendl;
    return -EINVAL;
  }

  while (write_length) {
    write_op.indata.append(osd_op.indata);
    write_length -= op.writesame.data_length;
  }

  write_op.op.op = CEPH_OSD_OP_WRITE;
  write_op.op.extent.offset = op.writesame.offset;
  write_op.op.extent.length = op.writesame.length;
  result = do_osd_ops(ctx, write_ops);
  if (result < 0)
    derr << "do_writesame do_osd_ops failed " << result << dendl;

  return result;
}

int TransparentPG::do_read(OpContext* ctx, OSDOp& osd_op)
{
  dout(20) << __func__ << dendl;
  auto& op = osd_op.op;
  //auto& oi = ctx->new_obs.oi;
  auto& soid = ctx->get_hobj();
  //__u32 seq = oi.truncate_seq;
  //uint64_t size = oi.size;
  //bool trimmed_read = false;

  //dout(30) << __func__ << " oi.size: " << oi.size << dendl;
  //dout(30) << __func__ << " oi.truncate_seq: " << oi.truncate_seq << dendl;
  dout(30) << __func__ << " op.extent.truncate_seq: " << op.extent.truncate_seq << dendl;
  dout(30) << __func__ << " op.extent.truncate_size: " << op.extent.truncate_size << dendl;

  // are we beyond truncate_size?
  /*if ((seq < op.extent.truncate_seq) &&
    (op.extent.offset + op.extent.length > op.extent.truncate_size) &&
    (size > op.extent.truncate_size))
    size = op.extent.truncate_size;

  if (op.extent.length == 0) //length is zero mean read the whole object
    op.extent.length = size;

  if (op.extent.offset >= size) {
    op.extent.length = 0;
    trimmed_read = true;
  } else if (op.extent.offset + op.extent.length > size) {
    op.extent.length = size - op.extent.offset;
    trimmed_read = true;
  }*/

  dout(30) << __func__ << "op.extent.length is now " << op.extent.length << dendl;

  // read into a buffer
  int result = 0;
  /*if (trimmed_read && op.extent.length == 0) {
    // read size was trimmed to zero and it is expected to do nothing
    // a read operation of 0 bytes does *not* do nothing, this is why
    // the trimmed_read boolean is needed
  } else if (pool.info.is_erasure()) {
    // The initialisation below is required to silence a false positive
    // -Wmaybe-uninitialized warning
    std::optional<uint32_t> maybe_crc;
    // If there is a data digest and it is possible we are reading
    // entire object, pass the digest.  FillInVerifyExtent will
    // will check the oi.size again.
    if (oi.is_data_digest() && op.extent.offset == 0 &&
      op.extent.length >= oi.size)
      maybe_crc = oi.data_digest;
    ctx->pending_async_reads.push_back(
      make_pair(
	boost::make_tuple(op.extent.offset, op.extent.length, op.flags),
	make_pair(&osd_op.outdata,
	  new FillInVerifyExtent(&op.extent.length, &osd_op.rval,
	    &osd_op.outdata, maybe_crc, oi.size,
	    osd, soid, op.flags))));
    dout(10) << " async_read noted for " << soid << dendl;

    ctx->op_finishers[ctx->current_osd_subop_num].reset(
      new ReadFinisher(osd_op));
  } else*/ {
    int r = pgbackend->objects_read_sync(
      soid, op.extent.offset, op.extent.length, op.flags, &osd_op.outdata);
    // whole object?  can we verify the checksum?
    /*if (r >= 0 && op.extent.offset == 0 &&
      (uint64_t)r == oi.size && oi.is_data_digest()) {
      uint32_t crc = osd_op.outdata.crc32c(-1);
      if (oi.data_digest != crc) {
	osd->clog->error() << info.pgid << std::hex
	  << " full-object read crc 0x" << crc
	  << " != expected 0x" << oi.data_digest
	  << std::dec << " on " << soid;
	r = -EIO; // try repair later
      }
    }
    if (r == -EIO) {
      r = rep_repair_primary_object(soid, ctx);
    }*/
    if (r >= 0)
      op.extent.length = r;
    else if (r == -EAGAIN) {
      result = -EAGAIN;
    } else {
      result = r;
      op.extent.length = 0;
    }
    dout(10) << " read got " << r << " / " << op.extent.length
      << " bytes from obj " << soid << dendl;
  }
  if (result >= 0) {
    ctx->delta_stats.num_rd_kb += shift_round_up(op.extent.length, 10);
    ctx->delta_stats.num_rd++;
  }
  return result;
}

int TransparentPG::do_sparse_read(OpContext* ctx, OSDOp& osd_op) {
  dout(20) << __func__ << dendl;
  auto& op = osd_op.op;
  //auto& oi = ctx->new_obs.oi;
  auto& soid = ctx->get_hobj();

/*  if (op.extent.truncate_seq) {
    dout(0) << "sparse_read does not support truncation sequence " << dendl;
    return -EINVAL;
  }*/

  ++ctx->num_read;
  /*if (pool.info.is_erasure()) {
    // translate sparse read to a normal one if not supported
    uint64_t offset = op.extent.offset;
    uint64_t length = op.extent.length;
    if (offset > oi.size) {
      length = 0;
    }
    else if (offset + length > oi.size) {
      length = oi.size - offset;
    }

    if (length > 0) {
      ctx->pending_async_reads.push_back(
	make_pair(
	  boost::make_tuple(offset, length, op.flags),
	  make_pair(
	    &osd_op.outdata,
	    new ToSparseReadResult(&osd_op.rval, &osd_op.outdata, offset,
	      &op.extent.length))));
      dout(10) << " async_read (was sparse_read) noted for " << soid << dendl;

      ctx->op_finishers[ctx->current_osd_subop_num].reset(
	new ReadFinisher(osd_op));
    }
    else {
      dout(10) << " sparse read ended up empty for " << soid << dendl;
      map<uint64_t, uint64_t> extents;
      encode(extents, osd_op.outdata);
    }
  } else */ {
    // read into a buffer
    map<uint64_t, uint64_t> m;
    int r = osd->store->fiemap(ch, ghobject_t(soid, ghobject_t::NO_GEN,
      info.pgid.shard),
      op.extent.offset, op.extent.length, m);
    if (r < 0) {
      return r;
    }

    bufferlist data_bl;
    r = pgbackend->objects_readv_sync(soid, std::move(m), op.flags, &data_bl);
    /*if (r == -EIO) {
      r = rep_repair_primary_object(soid, ctx);
    }*/
    if (r < 0) {
      return r;
    }

    // Why SPARSE_READ need checksum? In fact, librbd always use sparse-read.
    // Maybe at first, there is no much whole objects. With continued use, more
    // and more whole object exist. So from this point, for spare-read add
    // checksum make sense.
    /*if ((uint64_t)r == oi.size && oi.is_data_digest()) {
      uint32_t crc = data_bl.crc32c(-1);
      if (oi.data_digest != crc) {
	osd->clog->error() << info.pgid << std::hex
	  << " full-object read crc 0x" << crc
	  << " != expected 0x" << oi.data_digest
	  << std::dec << " on " << soid;
	r = rep_repair_primary_object(soid, ctx);
	if (r < 0) {
	  return r;
	}
      }
    }*/

    op.extent.length = r;

    encode(m, osd_op.outdata); // re-encode since it might be modified
    ::encode_destructively(data_bl, osd_op.outdata);

    dout(10) << " sparse_read got " << r << " bytes from object "
      << soid << dendl;
  }

  ctx->delta_stats.num_rd_kb += shift_round_up(op.extent.length, 10);
  ctx->delta_stats.num_rd++;
  return 0;
}

int TransparentPG::do_extent_cmp(OpContext* ctx, OSDOp& osd_op)
{
  dout(20) << __func__ << dendl;
  ceph_osd_op& op = osd_op.op;

  /*auto& oi = ctx->new_obs.oi;
  uint64_t size = oi.size;
  if ((oi.truncate_seq < op.extent.truncate_seq) &&
    (op.extent.offset + op.extent.length > op.extent.truncate_size)) {
    size = op.extent.truncate_size;
  }

  if (op.extent.offset >= size) {
    op.extent.length = 0;
  } else if (op.extent.offset + op.extent.length > size) {
    op.extent.length = size - op.extent.offset;
  }

  if (op.extent.length == 0) {
    dout(20) << __func__ << " zero length extent" << dendl;
    return finish_extent_cmp(osd_op, bufferlist{});
  } else if (!ctx->obs->exists || ctx->obs->oi.is_whiteout()) {
    dout(20) << __func__ << " object DNE" << dendl;
    return finish_extent_cmp(osd_op, {});
  } else if (pool.info.is_erasure()) {
    // If there is a data digest and it is possible we are reading
    // entire object, pass the digest.
    std::optional<uint32_t> maybe_crc;
    if (oi.is_data_digest() && op.checksum.offset == 0 &&
      op.checksum.length >= oi.size) {
      maybe_crc = oi.data_digest;
    }

    // async read
    auto& soid = oi.soid;
    auto extent_cmp_ctx = new C_ExtentCmpRead(this, osd_op, maybe_crc, oi.size,
      osd, soid, op.flags);
    ctx->pending_async_reads.push_back({
      {op.extent.offset, op.extent.length, op.flags},
      {&extent_cmp_ctx->read_bl, extent_cmp_ctx} });

    dout(10) << __func__ << ": async_read noted for " << soid << dendl;

    ctx->op_finishers[ctx->current_osd_subop_num].reset(
      new ReadFinisher(osd_op));
    return -EINPROGRESS;
  }*/

  // sync read
  vector<OSDOp> read_ops(1);
  OSDOp& read_op = read_ops[0];

  read_op.op.op = CEPH_OSD_OP_SYNC_READ;
  read_op.op.extent.offset = op.extent.offset;
  read_op.op.extent.length = op.extent.length;
  read_op.op.extent.truncate_seq = op.extent.truncate_seq;
  read_op.op.extent.truncate_size = op.extent.truncate_size;

  int result = do_osd_ops(ctx, read_ops);
  if (result < 0) {
    derr << __func__ << " failed " << result << dendl;
    return result;
  }
  return finish_extent_cmp(osd_op, read_op.outdata);
}

int TransparentPG::finish_extent_cmp(OSDOp& osd_op, const bufferlist& read_bl)
{
  for (uint64_t idx = 0; idx < osd_op.indata.length(); ++idx) {
    char read_byte = (idx < read_bl.length() ? read_bl[idx] : 0);
    if (osd_op.indata[idx] != read_byte) {
      return (-MAX_ERRNO - idx);
    }
  }

  return 0;
}

namespace {

  template<typename U, typename V>
  int do_cmp_xattr(int op, const U& lhs, const V& rhs)
  {
    switch (op) {
    case CEPH_OSD_CMPXATTR_OP_EQ:
      return lhs == rhs;
    case CEPH_OSD_CMPXATTR_OP_NE:
      return lhs != rhs;
    case CEPH_OSD_CMPXATTR_OP_GT:
      return lhs > rhs;
    case CEPH_OSD_CMPXATTR_OP_GTE:
      return lhs >= rhs;
    case CEPH_OSD_CMPXATTR_OP_LT:
      return lhs < rhs;
    case CEPH_OSD_CMPXATTR_OP_LTE:
      return lhs <= rhs;
    default:
      return -EINVAL;
    }
  }

} // anonymous namespace

int TransparentPG::do_xattr_cmp_u64(int op, uint64_t v1, bufferlist& xattr)
{
  uint64_t v2;

  if (xattr.length()) {
    const char* first = xattr.c_str();
    if (auto [p, ec] = std::from_chars(first, first + xattr.length(), v2);
      ec != std::errc()) {
      return -EINVAL;
    }
  }
  else {
    v2 = 0;
  }
  dout(20) << "do_xattr_cmp_u64 '" << v1 << "' vs '" << v2 << "' op " << op << dendl;
  return do_cmp_xattr(op, v1, v2);
}

int TransparentPG::do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr)
{
  string_view v2s(xattr.c_str(), xattr.length());
  dout(20) << "do_xattr_cmp_str '" << v1s << "' vs '" << v2s << "' op " << op << dendl;
  return do_cmp_xattr(op, v1s, v2s);
}

void TransparentPG::do_pg_op(OpRequestRef op)
{
  const MOSDOp* m = static_cast<const MOSDOp*>(op->get_req());
  ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
  dout(10) << __func__ << " " << *m << dendl;

  op->mark_started();

  int result = 0;
  string cname, mname;

  snapid_t snapid = m->get_snapid();

  vector<OSDOp> ops = m->ops;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p) {
    std::unique_ptr<const PGLSFilter> filter;
    OSDOp& osd_op = *p;
    auto bp = p->indata.cbegin();
    switch (p->op.op) {
    case CEPH_OSD_OP_PGNLS_FILTER:
      /*try {
	decode(cname, bp);
	decode(mname, bp);
      }
      catch (const ceph::buffer::error& e) {
	dout(0) << "unable to decode PGLS_FILTER description in " << *m << dendl;
	result = -EINVAL;
	break;
      }
      std::tie(result, filter) = get_pgls_filter(bp);
      if (result < 0)
	break;

      ceph_assert(filter);

      // fall through
      */
      result = -ENOTSUP;
      break;

    case CEPH_OSD_OP_PGNLS:
      if (snapid != CEPH_NOSNAP) {
	result = -EINVAL;
	break;
      }
      if (get_osdmap()->raw_pg_to_pg(m->get_pg()) != info.pgid.pgid) {
	dout(10) << " pgnls pg=" << m->get_pg()
	  << " " << get_osdmap()->raw_pg_to_pg(m->get_pg())
	  << " != " << info.pgid << dendl;
	result = 0; // hmm?
      } else {
	unsigned list_size = std::min<uint64_t>(cct->_conf->osd_max_pgls,
	  p->op.pgls.count);

	dout(10) << " pgnls pg=" << m->get_pg() << " count " << list_size
	  << dendl;
	// read into a buffer
	vector<hobject_t> sentries;
	pg_nls_response_t response;
	try {
	  decode(response.handle, bp);
	}
	catch (const ceph::buffer::error& e) {
	  dout(0) << "unable to decode PGNLS handle in " << *m << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t next;
	hobject_t lower_bound = response.handle;
	hobject_t pg_start = info.pgid.pgid.get_hobj_start();
	hobject_t pg_end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
	dout(10) << " pgnls lower_bound " << lower_bound
	  << " pg_end " << pg_end << dendl;
	if (((!lower_bound.is_max() && lower_bound >= pg_end) ||
	  (lower_bound != hobject_t() && lower_bound < pg_start))) {
	  // this should only happen with a buggy client.
	  dout(10) << "outside of PG bounds " << pg_start << " .. "
	    << pg_end << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t current = lower_bound;
	int r = pgbackend->objects_list_partial(
	  current,
	  list_size,
	  list_size,
	  &sentries,
	  &next);
	if (r != 0) {
	  result = -EINVAL;
	  break;
	}

	map<hobject_t, pg_missing_item>::const_iterator missing_iter =
	  recovery_state.get_pg_log().get_missing().get_items().lower_bound(current);
	vector<hobject_t>::iterator ls_iter = sentries.begin();
	hobject_t _max = hobject_t::get_max();
	while (1) {
	  const hobject_t& mcand =
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() ?
	    _max :
	    missing_iter->first;
	  const hobject_t& lcand =
	    ls_iter == sentries.end() ?
	    _max :
	    *ls_iter;

	  hobject_t candidate;
	  if (mcand == lcand) {
	    candidate = mcand;
	    if (!mcand.is_max()) {
	      ++ls_iter;
	      ++missing_iter;
	    }
	  } else if (mcand < lcand) {
	    candidate = mcand;
	    ceph_assert(!mcand.is_max());
	    ++missing_iter;
	  } else {
	    candidate = lcand;
	    ceph_assert(!lcand.is_max());
	    ++ls_iter;
	  }

	  dout(10) << " pgnls candidate 0x" << std::hex << candidate.get_hash()
	    << " vs lower bound 0x" << lower_bound.get_hash()
	    << std::dec << dendl;

	  if (candidate >= next) {
	    break;
	  }

	  if (response.entries.size() == list_size) {
	    next = candidate;
	    break;
	  }

	  if (candidate.snap != CEPH_NOSNAP)
	    continue;

	  // skip internal namespace
	  if (candidate.get_namespace() == cct->_conf->osd_hit_set_namespace)
	    continue;

	  if (recovery_state.get_missing_loc().is_deleted(candidate))
	    continue;

	  // skip wrong namespace
	  if (m->get_hobj().nspace != librados::all_nspaces &&
	    candidate.get_namespace() != m->get_hobj().nspace)
	    continue;

	  /*if (filter && !pgls_filter(*filter, candidate))
	    continue;*/

	  dout(20) << "pgnls item 0x" << std::hex
	    << candidate.get_hash()
	    << ", rev 0x" << hobject_t::_reverse_bits(candidate.get_hash())
	    << std::dec << " "
	    << candidate.oid.name << dendl;

	  librados::ListObjectImpl item;
	  item.nspace = candidate.get_namespace();
	  item.oid = candidate.oid.name;
	  item.locator = candidate.get_key();
	  response.entries.push_back(item);
	}

	if (next.is_max() &&
	  missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() &&
	  ls_iter == sentries.end()) {
	  result = 1;

	  // Set response.handle to the start of the next PG according
	  // to the object sort order.
	  response.handle = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
	}
	else {
	  response.handle = next;
	}
	dout(10) << "pgnls handle=" << response.handle << dendl;
	encode(response, osd_op.outdata);
	dout(10) << " pgnls result=" << result << " outdata.length()="
	  << osd_op.outdata.length() << dendl;
      }
      break;

    case CEPH_OSD_OP_PGLS_FILTER:
      /*try {
	decode(cname, bp);
	decode(mname, bp);
      }
      catch (const ceph::buffer::error& e) {
	dout(0) << "unable to decode PGLS_FILTER description in " << *m << dendl;
	result = -EINVAL;
	break;
      }
      std::tie(result, filter) = get_pgls_filter(bp);
      if (result < 0)
	break;

      ceph_assert(filter);

      // fall through*/
      result = -ENOTSUP;
      break;

    case CEPH_OSD_OP_PGLS:
      if (snapid != CEPH_NOSNAP) {
	result = -EINVAL;
	break;
      }
      if (get_osdmap()->raw_pg_to_pg(m->get_pg()) != info.pgid.pgid) {
	dout(10) << " pgls pg=" << m->get_pg()
	  << " " << get_osdmap()->raw_pg_to_pg(m->get_pg())
	  << " != " << info.pgid << dendl;
	result = 0; // hmm?
      } else {
	unsigned list_size = std::min<uint64_t>(cct->_conf->osd_max_pgls,
	  p->op.pgls.count);

	dout(10) << " pgls pg=" << m->get_pg() << " count " << list_size << dendl;
	// read into a buffer
	vector<hobject_t> sentries;
	pg_ls_response_t response;
	try {
	  decode(response.handle, bp);
	}
	catch (const ceph::buffer::error& e) {
	  dout(0) << "unable to decode PGLS handle in " << *m << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t next;
	hobject_t current = response.handle;
	int r = pgbackend->objects_list_partial(
	  current,
	  list_size,
	  list_size,
	  &sentries,
	  &next);
	if (r != 0) {
	  result = -EINVAL;
	  break;
	}

	ceph_assert(snapid == CEPH_NOSNAP || recovery_state.get_pg_log().get_missing().get_items().empty());

	map<hobject_t, pg_missing_item>::const_iterator missing_iter =
	  recovery_state.get_pg_log().get_missing().get_items().lower_bound(current);
	vector<hobject_t>::iterator ls_iter = sentries.begin();
	hobject_t _max = hobject_t::get_max();
	while (1) {
	  const hobject_t& mcand =
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() ?
	    _max :
	    missing_iter->first;
	  const hobject_t& lcand =
	    ls_iter == sentries.end() ?
	    _max :
	    *ls_iter;

	  hobject_t candidate;
	  if (mcand == lcand) {
	    candidate = mcand;
	    if (!mcand.is_max()) {
	      ++ls_iter;
	      ++missing_iter;
	    }
	  } else if (mcand < lcand) {
	    candidate = mcand;
	    ceph_assert(!mcand.is_max());
	    ++missing_iter;
	  } else {
	    candidate = lcand;
	    ceph_assert(!lcand.is_max());
	    ++ls_iter;
	  }

	  if (candidate >= next) {
	    break;
	  }

	  if (response.entries.size() == list_size) {
	    next = candidate;
	    break;
	  }

	  if (candidate.snap != CEPH_NOSNAP)
	    continue;

	  // skip wrong namespace
	  if (candidate.get_namespace() != m->get_hobj().nspace)
	    continue;

	  if (recovery_state.get_missing_loc().is_deleted(candidate))
	    continue;

	  /*if (filter && !pgls_filter(*filter, candidate))
	    continue;*/

	  response.entries.push_back(make_pair(candidate.oid,
	    candidate.get_key()));
	}
	if (next.is_max() &&
	  missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() &&
	  ls_iter == sentries.end()) {
	  result = 1;
	}
	response.handle = next;
	encode(response, osd_op.outdata);
	dout(10) << " pgls result=" << result << " outdata.length()="
	  << osd_op.outdata.length() << dendl;
      }
      break;

    case CEPH_OSD_OP_PG_HITSET_LS:
      result = -ENOTSUP;
    /*{
      list< pair<utime_t, utime_t> > ls;
      for (list<pg_hit_set_info_t>::const_iterator p = info.hit_set.history.begin();
	p != info.hit_set.history.end();
	++p)
	ls.push_back(make_pair(p->begin, p->end));
      if (hit_set)
	ls.push_back(make_pair(hit_set_start_stamp, utime_t()));
      encode(ls, osd_op.outdata);
    }*/
    break;

    case CEPH_OSD_OP_PG_HITSET_GET:
      result = -ENOTSUP;
    /*{
      utime_t stamp(osd_op.op.hit_set_get.stamp);
      if (hit_set_start_stamp && stamp >= hit_set_start_stamp) {
	// read the current in-memory HitSet, not the version we've
	// checkpointed.
	if (!hit_set) {
	  result = -ENOENT;
	  break;
	}
	encode(*hit_set, osd_op.outdata);
	result = osd_op.outdata.length();
      } else {
	// read an archived HitSet.
	hobject_t oid;
	for (list<pg_hit_set_info_t>::const_iterator p = info.hit_set.history.begin();
	  p != info.hit_set.history.end();
	  ++p) {
	  if (stamp >= p->begin && stamp <= p->end) {
	    oid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);
	    break;
	  }
	}
	if (oid == hobject_t()) {
	  result = -ENOENT;
	  break;
	}
	if (!pool.info.is_replicated()) {
	  // FIXME: EC not supported yet
	  result = -EOPNOTSUPP;
	  break;
	}
	if (is_unreadable_object(oid)) {
	  wait_for_unreadable_object(oid, op);
	  return;
	}
	result = osd->store->read(ch, ghobject_t(oid), 0, 0, osd_op.outdata);
      }
    }*/
    break;

    case CEPH_OSD_OP_SCRUBLS:
      result = -ENOTSUP; //do_scrub_ls(m, &osd_op);
      break;

    default:
      result = -EINVAL;
      break;
    }

    if (result < 0)
      break;
  }

  // reply
  MOSDOpReply* reply = new MOSDOpReply(m, 0, get_osdmap_epoch(),
    CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
    false);
  reply->claim_op_out_data(ops);
  reply->set_result(result);
  reply->set_reply_versions(info.last_update, info.last_user_version);
  osd->send_message_osd_client(reply, m->get_connection());
}

int TransparentPG::do_osd_ops(OpContextBase* ctx0, vector<OSDOp>& ops)
{
  int result = 0;
  TransparentPG::OpContext* ctx =
    reinterpret_cast<TransparentPG::OpContext*>(ctx0);
  //SnapSetContext* ssc = ctx->obc->ssc;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
/*  const hobject_t& soid = oi.soid;
  const bool skip_data_digest = osd->store->has_builtin_csum() &&
    osd->osd_skip_data_digest;*/

  const hobject_t& soid = ctx->get_hobj();
  PGTransaction* t = ctx->op_t.get();

  dout(10) << __func__ << " " << soid << " " << ops << dendl;
  /*#ifdef HAVE_JAEGER
  if (ctx->op->osd_parent_span) {
    auto do_osd_op_span = jaeger_tracing::child_span(__func__, ctx->op->osd_parent_span);
  }
#endif*/

  ctx->current_osd_subop_num = 0; // FIXME: is it used?
  for (auto p = ops.begin(); p != ops.end(); ++p, ctx->current_osd_subop_num++, ctx->processed_subop_count++) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op;

    OpFinisher* op_finisher = nullptr;
    /*{
      auto op_finisher_it = ctx->op_finishers.find(ctx->current_osd_subop_num);
      if (op_finisher_it != ctx->op_finishers.end()) {
	op_finisher = op_finisher_it->second.get();
      }
    }*/

/*    // TODO: check endianness (ceph_le32 vs uint32_t, etc.)
    // The fields in ceph_osd_op are little-endian (according to the definition in rados.h),
    // but the code in this function seems to treat them as native-endian.  What should the
    // tracepoints do?
    tracepoint(osd, do_osd_op_pre, soid.oid.name.c_str(), soid.snap.val, op.op, ceph_osd_op_name(op.op), op.flags);
*/
    dout(10) << __func__ << " " << osd_op << " " << op.op << dendl;

    auto bp = osd_op.indata.cbegin();

    // user-visible modifcation?
    switch (op.op) {
      // non user-visible modifications
    case CEPH_OSD_OP_WATCH:
    case CEPH_OSD_OP_CACHE_EVICT:
    case CEPH_OSD_OP_CACHE_FLUSH:
    case CEPH_OSD_OP_CACHE_TRY_FLUSH:
    case CEPH_OSD_OP_UNDIRTY:
    case CEPH_OSD_OP_COPY_FROM:  // we handle user_version update explicitly
    case CEPH_OSD_OP_COPY_FROM2:
    case CEPH_OSD_OP_CACHE_PIN:
    case CEPH_OSD_OP_CACHE_UNPIN:
    case CEPH_OSD_OP_SET_REDIRECT:
    case CEPH_OSD_OP_SET_CHUNK:
    case CEPH_OSD_OP_TIER_PROMOTE:
    case CEPH_OSD_OP_TIER_FLUSH:
    case CEPH_OSD_OP_TIER_EVICT:
      break;
    default:
      /*if (op.op & CEPH_OSD_OP_MODE_WR)
	ctx->user_modify = true;*/
      break;
    }

    // munge -1 truncate to 0 truncate
    if (ceph_osd_op_uses_extent(op.op) &&
      op.extent.truncate_seq == 1 &&
      op.extent.truncate_size == (-1ULL)) {
      op.extent.truncate_size = 0;
      op.extent.truncate_seq = 0;
    }

    // munge ZERO -> TRUNCATE?  (don't munge to DELETE or we risk hosing attributes)
    /*if (op.op == CEPH_OSD_OP_ZERO &&
      obs.exists &&
      op.extent.offset < static_cast<Option::size_t>(osd->osd_max_object_size) &&
      op.extent.length >= 1 &&
      op.extent.length <= static_cast<Option::size_t>(osd->osd_max_object_size) &&
      op.extent.offset + op.extent.length >= oi.size) {
      if (op.extent.offset >= oi.size) {
	// no-op
	goto fail;
      }
      dout(10) << " munging ZERO " << op.extent.offset << "~" << op.extent.length
	<< " -> TRUNCATE " << op.extent.offset << " (old size is " << oi.size << ")" << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
    }*/

    switch (op.op) {

      // --- READS ---

    case CEPH_OSD_OP_CMPEXT:
      ++ctx->num_read;
      /*tracepoint(osd, do_osd_op_pre_extent_cmp, soid.oid.name.c_str(),
	soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset,
	op.extent.length, op.extent.truncate_size,
	op.extent.truncate_seq);*/

      if (op_finisher == nullptr) {
	result = do_extent_cmp(ctx, osd_op);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_SYNC_READ:
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      // fall through
    case CEPH_OSD_OP_READ:
      ++ctx->num_read;
      /*tracepoint(osd, do_osd_op_pre_read, soid.oid.name.c_str(),
	soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset,
	op.extent.length, op.extent.truncate_size,
	op.extent.truncate_seq);*/
      if (op_finisher == nullptr) {
	if (!ctx->data_off) {
	  ctx->data_off = op.extent.offset;
	}
	result = do_read(ctx, osd_op);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_CHECKSUM:
      ++ctx->num_read;
      result = -ENOTSUP;
      /*{
	tracepoint(osd, do_osd_op_pre_checksum, soid.oid.name.c_str(),
	  soid.snap.val, oi.size, oi.truncate_seq, op.checksum.type,
	  op.checksum.offset, op.checksum.length,
	  op.checksum.chunk_size);

	if (op_finisher == nullptr) {
	  result = do_checksum(ctx, osd_op, &bp);
	} else {
	  result = op_finisher->execute();
	}
      }*/
      break;

      /* map extents */
    case CEPH_OSD_OP_MAPEXT:
      //tracepoint(osd, do_osd_op_pre_mapext, soid.oid.name.c_str(), soid.snap.val, op.extent.offset, op.extent.length);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_read;
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->fiemap(ch, ghobject_t(soid, ghobject_t::NO_GEN,
	  info.pgid.shard),
	  op.extent.offset, op.extent.length, bl);
	osd_op.outdata = std::move(bl);
	if (r < 0)
	  result = r;
	else
	  ctx->delta_stats.num_rd_kb += shift_round_up(bl.length(), 10);
	ctx->delta_stats.num_rd++;
	dout(10) << " map_extents done on object " << soid << dendl;
      }
      break;

      /* map extents */
    case CEPH_OSD_OP_SPARSE_READ:
      /*tracepoint(osd, do_osd_op_pre_sparse_read, soid.oid.name.c_str(),
	soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset,
	op.extent.length, op.extent.truncate_size,
	op.extent.truncate_seq);*/
      if (op_finisher == nullptr) {
	result = do_sparse_read(ctx, osd_op);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_CALL:
    {
      string cname, mname;
      bufferlist indata;
      try {
	bp.copy(op.cls.class_len, cname);
	bp.copy(op.cls.method_len, mname);
	bp.copy(op.cls.indata_len, indata);
      }
      catch (ceph::buffer::error& e) {
	dout(10) << "call unable to decode class + method + indata" << dendl;
	dout(30) << "in dump: ";
	osd_op.indata.hexdump(*_dout);
	*_dout << dendl;
	result = -EINVAL;
	//tracepoint(osd, do_osd_op_pre_call, soid.oid.name.c_str(), soid.snap.val, "???", "???");
	break;
      }
      //tracepoint(osd, do_osd_op_pre_call, soid.oid.name.c_str(), soid.snap.val, cname.c_str(), mname.c_str());

      ClassHandler::ClassData* cls;
      result = ClassHandler::get_instance().open_class(cname, &cls);
      ceph_assert(result == 0);   // init_op_flags() already verified this works.

      ClassHandler::ClassMethod* method = cls->get_method(mname);
      if (!method) {
	dout(10) << "call method " << cname << "." << mname << " does not exist" << dendl;
	result = -EOPNOTSUPP;
	break;
      }

      int flags = method->get_flags();
      if (flags & CLS_METHOD_WR)
	ctx->user_modify = true;

      bufferlist outdata;
      dout(10)<< __func__ << " call method " << cname << "." << mname << dendl;
      int prev_rd = ctx->num_read;
      int prev_wr = ctx->num_write;
      result = method->exec((cls_method_context_t)&ctx, indata, outdata);

      if (ctx->num_read > prev_rd && !(flags & CLS_METHOD_RD)) {
	derr << __func__ << " method " << cname << "." << mname << " tried to read object but is not marked RD" << dendl;
	result = -EIO;
	break;
      }
      if (ctx->num_write > prev_wr && !(flags & CLS_METHOD_WR)) {
	derr << __func__ << " method " << cname << "." << mname << " tried to update object but is not marked WR" << dendl;
	result = -EIO;
	break;
      }

      dout(10) << __func__ << " method called response length=" << outdata.length() << dendl;
      op.extent.length = outdata.length();
      osd_op.outdata.claim_append(outdata);
      dout(30) << "out dump: ";
      osd_op.outdata.hexdump(*_dout);
      *_dout << dendl;
    }
    break;

    case CEPH_OSD_OP_STAT:
      // note: stat does not require RD
    {
      //tracepoint(osd, do_osd_op_pre_stat, soid.oid.name.c_str(), soid.snap.val);

      if (obs.exists && !oi.is_whiteout()) {
	encode(oi.size, osd_op.outdata);
	encode(oi.mtime, osd_op.outdata);
	dout(10) << "stat oi has " << oi.size << " " << oi.mtime << dendl;
      } else {
	result = -ENOENT;
	dout(10) << "stat oi object does not exist" << dendl;
      }
      /*struct stat st;
      result = osd->store->stat(
        ch,
        ghobject_t(soid, ghobject_t::NO_GEN, pg_whoami.shard),
        &st);*/

      ctx->delta_stats.num_rd++;
    }
    break;

    case CEPH_OSD_OP_ISDIRTY:
      ++ctx->num_read;
      /*{
	tracepoint(osd, do_osd_op_pre_isdirty, soid.oid.name.c_str(), soid.snap.val);
	bool is_dirty = obs.oi.is_dirty();
	encode(is_dirty, osd_op.outdata);
	ctx->delta_stats.num_rd++;
	result = 0;
      }*/
      result = -ENOTSUP; // FIXME
      break;

    case CEPH_OSD_OP_UNDIRTY:
      ++ctx->num_write;
      result = -ENOTSUP; // FIXME
    /*result = 0;
      {
	//tracepoint(osd, do_osd_op_pre_undirty, soid.oid.name.c_str(), soid.snap.val);
	if (oi.is_dirty()) {
	  ctx->undirty = true;  // see make_writeable()
	  ctx->modify = true;
	  ctx->delta_stats.num_wr++;
	}
      }*/
      break;

    case CEPH_OSD_OP_CACHE_TRY_FLUSH:
      ++ctx->num_write;
      result = -ENOTSUP;
      /*result = 0;
      {
	//tracepoint(osd, do_osd_op_pre_try_flush, soid.oid.name.c_str(), soid.snap.val);
	if (ctx->lock_type != RWState::RWNONE) {
	  dout(10) << "cache-try-flush without SKIPRWLOCKS flag set" << dendl;
	  result = -EINVAL;
	  break;
	}
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE || obs.oi.has_manifest()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_cache_pinned()) {
	  dout(10) << "cache-try-flush on a pinned object, consider unpin this object first" << dendl;
	  result = -EPERM;
	  break;
	}
	if (oi.is_dirty()) {
	  result = start_flush(ctx->op, ctx->obc, false, NULL, std::nullopt);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	}
	else {
	  result = 0;
	}
      }*/
      break;

    case CEPH_OSD_OP_CACHE_FLUSH:
      ++ctx->num_write;
      result = -ENOTSUP; 
      /*result = 0;
      {
	//tracepoint(osd, do_osd_op_pre_cache_flush, soid.oid.name.c_str(), soid.snap.val);
	if (ctx->lock_type == RWState::RWNONE) {
	  dout(10) << "cache-flush with SKIPRWLOCKS flag set" << dendl;
	  result = -EINVAL;
	  break;
	}
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE || obs.oi.has_manifest()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_cache_pinned()) {
	  dout(10) << "cache-flush on a pinned object, consider unpin this object first" << dendl;
	  result = -EPERM;
	  break;
	}
	hobject_t missing;
	if (oi.is_dirty()) {
	  result = start_flush(ctx->op, ctx->obc, true, &missing, std::nullopt);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	}
	else {
	  result = 0;
	}
	// Check special return value which has set missing_return
	if (result == -ENOENT) {
	  dout(10) << __func__ << " CEPH_OSD_OP_CACHE_FLUSH got ENOENT" << dendl;
	  ceph_assert(!missing.is_min());
	  wait_for_unreadable_object(missing, ctx->op);
	  // Error code which is used elsewhere when wait_for_unreadable_object() is used
	  result = -EAGAIN;
	}
      }*/
      break;

    case CEPH_OSD_OP_CACHE_EVICT:
      ++ctx->num_write;
      result = -ENOTSUP;
      /*result = 0;
      {
	//tracepoint(osd, do_osd_op_pre_cache_evict, soid.oid.name.c_str(), soid.snap.val);
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE || obs.oi.has_manifest()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_cache_pinned()) {
	  dout(10) << "cache-evict on a pinned object, consider unpin this object first" << dendl;
	  result = -EPERM;
	  break;
	}
	if (oi.is_dirty()) {
	  result = -EBUSY;
	  break;
	}
	if (!oi.watchers.empty()) {
	  result = -EBUSY;
	  break;
	}
	if (soid.snap == CEPH_NOSNAP) {
	  result = _verify_no_head_clones(soid, ssc->snapset);
	  if (result < 0)
	    break;
	}
	result = _delete_oid(ctx, true, false);
	if (result >= 0) {
	  // mark that this is a cache eviction to avoid triggering normal
	  // make_writeable() clone creation in finish_ctx()
	  ctx->cache_operation = true;
	}
	osd->logger->inc(l_osd_tier_evict);
      }*/
      break;

    case CEPH_OSD_OP_GETXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	//tracepoint(osd, do_osd_op_pre_getxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	string name = "_" + aname;
	/*int r = getattr_maybe_cache(
	  ctx->obc,
	  name,
	  &(osd_op.outdata));*/
	int r = pgbackend->objects_get_attr(soid, name, &(osd_op.outdata));

	if (r >= 0) {
	  op.xattr.value_len = osd_op.outdata.length();
	  result = 0;
	  ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	} else
	  result = r;

	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_GETXATTRS:
      ++ctx->num_read;
      {
	//tracepoint(osd, do_osd_op_pre_getxattrs, soid.oid.name.c_str(), soid.snap.val);
	map<string, bufferlist, less<>> out;
	result = getattrs_maybe_cache(
	  ctx->get_hobj(),
	  &out);

	bufferlist bl;
	encode(out, bl);
	ctx->delta_stats.num_rd_kb += shift_round_up(bl.length(), 10);
	ctx->delta_stats.num_rd++;
	osd_op.outdata.claim_append(bl);
      }
      break;

    case CEPH_OSD_OP_CMPXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	//tracepoint(osd, do_osd_op_pre_cmpxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	string name = "_" + aname;
	name[op.xattr.name_len + 1] = 0;

	bufferlist xattr;
	/*result = getattr_maybe_cache(
	  ctx->obc,
	  name,
	  &xattr);*/
	result = pgbackend->objects_get_attr(soid, name, &xattr);

	if (result < 0 && result != -EEXIST && result != -ENODATA)
	  break;

	ctx->delta_stats.num_rd++;
	ctx->delta_stats.num_rd_kb += shift_round_up(xattr.length(), 10);

	switch (op.xattr.cmp_mode) {
	case CEPH_OSD_CMPXATTR_MODE_STRING:
	{
	  string val;
	  bp.copy(op.xattr.value_len, val);
	  val[op.xattr.value_len] = 0;
	  dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << val
	    << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	  result = do_xattr_cmp_str(op.xattr.cmp_op, val, xattr);
	}
	break;

	case CEPH_OSD_CMPXATTR_MODE_U64:
	{
	  uint64_t u64val;
	  try {
	    decode(u64val, bp);
	  }
	  catch (ceph::buffer::error& e) {
	    result = -EINVAL;
	    goto fail;
	  }
	  dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << u64val
	    << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	  result = do_xattr_cmp_u64(op.xattr.cmp_op, u64val, xattr);
	}
	break;

	default:
	  dout(10) << "bad cmp mode " << (int)op.xattr.cmp_mode << dendl;
	  result = -EINVAL;
	}

	if (!result) {
	  dout(10) << "comparison returned false" << dendl;
	  result = -ECANCELED;
	  break;
	}
	if (result < 0) {
	  dout(10) << "comparison returned " << result << " " << cpp_strerror(-result) << dendl;
	  break;
	}

	dout(10) << "comparison returned true" << dendl;
      }
      break;

    case CEPH_OSD_OP_ASSERT_VER:
      ++ctx->num_read;
      /*{
	uint64_t ver = op.assert_ver.ver;
	//tracepoint(osd, do_osd_op_pre_assert_ver, soid.oid.name.c_str(), soid.snap.val, ver);
	if (!ver)
	  result = -EINVAL;
	else if (ver < oi.user_version)
	  result = -ERANGE;
	else if (ver > oi.user_version)
	  result = -EOVERFLOW;
      }*/
      result = -ENOTSUP; // FIXME
      break;

    case CEPH_OSD_OP_LIST_WATCHERS:
      ++ctx->num_read;
      {
	//tracepoint(osd, do_osd_op_pre_list_watchers, soid.oid.name.c_str(), soid.snap.val);
	obj_list_watch_response_t resp;

	map<pair<uint64_t, entity_name_t>, watch_info_t>::const_iterator oi_iter;
	for (oi_iter = oi.watchers.begin(); oi_iter != oi.watchers.end();
	  ++oi_iter) {
	  dout(20) << "key cookie=" << oi_iter->first.first
	    << " entity=" << oi_iter->first.second << " "
	    << oi_iter->second << dendl;
	  ceph_assert(oi_iter->first.first == oi_iter->second.cookie);
	  ceph_assert(oi_iter->first.second.is_client());

	  watch_item_t wi(oi_iter->first.second, oi_iter->second.cookie,
	    oi_iter->second.timeout_seconds, oi_iter->second.addr);
	  resp.entries.push_back(wi);
	}

	resp.encode(osd_op.outdata, ctx->get_features());
	result = 0;

	ctx->delta_stats.num_rd++;
	break;
      }

/*    case CEPH_OSD_OP_LIST_SNAPS:
      ++ctx->num_read;
      {
	//tracepoint(osd, do_osd_op_pre_list_snaps, soid.oid.name.c_str(), soid.snap.val);
	obj_list_snap_response_t resp;

	if (!ssc) {
	  ssc = ctx->obc->ssc = get_snapset_context(soid, false);
	}
	ceph_assert(ssc);
	dout(20) << " snapset " << ssc->snapset << dendl;

	int clonecount = ssc->snapset.clones.size();
	clonecount++;  // for head
	resp.clones.reserve(clonecount);
	for (auto clone_iter = ssc->snapset.clones.begin();
	  clone_iter != ssc->snapset.clones.end(); ++clone_iter) {
	  clone_info ci;
	  ci.cloneid = *clone_iter;

	  hobject_t clone_oid = soid;
	  clone_oid.snap = *clone_iter;

	  auto p = ssc->snapset.clone_snaps.find(*clone_iter);
	  if (p == ssc->snapset.clone_snaps.end()) {
	    osd->clog->error() << "osd." << osd->whoami
	      << ": inconsistent clone_snaps found for oid "
	      << soid << " clone " << *clone_iter
	      << " snapset " << ssc->snapset;
	    result = -EINVAL;
	    break;
	  }
	  for (auto q = p->second.rbegin(); q != p->second.rend(); ++q) {
	    ci.snaps.push_back(*q);
	  }

	  dout(20) << " clone " << *clone_iter << " snaps " << ci.snaps << dendl;

	  map<snapid_t, interval_set<uint64_t> >::const_iterator coi;
	  coi = ssc->snapset.clone_overlap.find(ci.cloneid);
	  if (coi == ssc->snapset.clone_overlap.end()) {
	    osd->clog->error() << "osd." << osd->whoami
	      << ": inconsistent clone_overlap found for oid "
	      << soid << " clone " << *clone_iter;
	    result = -EINVAL;
	    break;
	  }
	  const interval_set<uint64_t>& o = coi->second;
	  ci.overlap.reserve(o.num_intervals());
	  for (interval_set<uint64_t>::const_iterator r = o.begin();
	    r != o.end(); ++r) {
	    ci.overlap.push_back(pair<uint64_t, uint64_t>(r.get_start(),
	      r.get_len()));
	  }

	  map<snapid_t, uint64_t>::const_iterator si;
	  si = ssc->snapset.clone_size.find(ci.cloneid);
	  if (si == ssc->snapset.clone_size.end()) {
	    osd->clog->error() << "osd." << osd->whoami
	      << ": inconsistent clone_size found for oid "
	      << soid << " clone " << *clone_iter;
	    result = -EINVAL;
	    break;
	  }
	  ci.size = si->second;

	  resp.clones.push_back(ci);
	}
	if (result < 0) {
	  break;
	}
	if (!ctx->obc->obs.oi.is_whiteout()) {
	  ceph_assert(obs.exists);
	  clone_info ci;
	  ci.cloneid = CEPH_NOSNAP;

	  //Size for HEAD is oi.size
	  ci.size = oi.size;

	  resp.clones.push_back(ci);
	}
	resp.seq = ssc->snapset.seq;

	resp.encode(osd_op.outdata);
	result = 0;

	ctx->delta_stats.num_rd++;
      }
      break;*/

    case CEPH_OSD_OP_NOTIFY:
      ++ctx->num_read;
      {
	uint32_t timeout;
	bufferlist bl;

	try {
	  uint32_t ver; // obsolete
	  decode(ver, bp);
	  decode(timeout, bp);
	  decode(bl, bp);
	}
	catch (const ceph::buffer::error& e) {
	  timeout = 0;
	}
	//tracepoint(osd, do_osd_op_pre_notify, soid.oid.name.c_str(), soid.snap.val, timeout);
	if (!timeout)
	  timeout = cct->_conf->osd_default_notify_timeout;

	notify_info_t n;
	n.timeout = timeout;
	n.notify_id = osd->get_next_id(get_osdmap_epoch());
	n.cookie = op.notify.cookie;
	n.bl = bl;
	ctx->notifies.push_back(n);

	// return our unique notify id to the client
	encode(n.notify_id, osd_op.outdata);
      }
      break;

    case CEPH_OSD_OP_NOTIFY_ACK:
      ++ctx->num_read;
      {
	try {
	  uint64_t notify_id = 0;
	  uint64_t watch_cookie = 0;
	  decode(notify_id, bp);
	  decode(watch_cookie, bp);
	  bufferlist reply_bl;
	  if (!bp.end()) {
	    decode(reply_bl, bp);
	  }
	  //tracepoint(osd, do_osd_op_pre_notify_ack, soid.oid.name.c_str(), soid.snap.val, notify_id, watch_cookie, "Y");
	  OpContext::NotifyAck ack(notify_id, watch_cookie, reply_bl);
	  ctx->notify_acks.push_back(ack);
	}
	catch (const ceph::buffer::error& e) {
	  //tracepoint(osd, do_osd_op_pre_notify_ack, soid.oid.name.c_str(), soid.snap.val, op.watch.cookie, 0, "N");
	  OpContext::NotifyAck ack(
	    // op.watch.cookie is actually the notify_id for historical reasons
	    op.watch.cookie
	  );
	  ctx->notify_acks.push_back(ack);
	}
      }
      break;

    case CEPH_OSD_OP_SETALLOCHINT:
      ++ctx->num_write;
      result = 0;
      {
	//tracepoint(osd, do_osd_op_pre_setallochint, soid.oid.name.c_str(), soid.snap.val, op.alloc_hint.expected_object_size, op.alloc_hint.expected_write_size);
	maybe_create_new_object(ctx);
	oi.expected_object_size = op.alloc_hint.expected_object_size;
	oi.expected_write_size = op.alloc_hint.expected_write_size;
	oi.alloc_hint_flags = op.alloc_hint.flags;
	t->set_alloc_hint(soid, op.alloc_hint.expected_object_size,
	  op.alloc_hint.expected_write_size,
	  op.alloc_hint.flags);
      }
      break;


      // --- WRITES ---

      // -- object data --

    case CEPH_OSD_OP_WRITE:
      ++ctx->num_write;
      result = 0;
      { // write
	//__u32 seq = oi.truncate_seq;
	//tracepoint(osd, do_osd_op_pre_write, soid.oid.name.c_str(), soid.snap.val, oi.size, seq, op.extent.offset, op.extent.length, op.extent.truncate_size, op.extent.truncate_seq);
	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
	}

	if (pool.info.has_flag(pg_pool_t::FLAG_WRITE_FADVISE_DONTNEED))
	  op.flags = op.flags | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

	if (pool.info.requires_aligned_append() &&
	  (op.extent.offset % pool.info.required_alignment() != 0)) {
	  result = -EOPNOTSUPP;
	  break;
	}

	/*if (!obs.exists) {
	  if (pool.info.requires_aligned_append() && op.extent.offset) {
	    result = -EOPNOTSUPP;
	    break;
	  }
	} else if (op.extent.offset != oi.size &&
	  pool.info.requires_aligned_append()) {
	  result = -EOPNOTSUPP;
	  break;
	}*/

	/*if (seq && (seq > op.extent.truncate_seq) &&
	  (op.extent.offset + op.extent.length > oi.size)) {
	  // old write, arrived after trimtrunc
	  op.extent.length = (op.extent.offset > oi.size ? 0 : oi.size - op.extent.offset);
	  dout(10) << " old truncate_seq " << op.extent.truncate_seq << " < current " << seq
	    << ", adjusting write length to " << op.extent.length << dendl;
	  bufferlist t;
	  t.substr_of(osd_op.indata, 0, op.extent.length);
	  osd_op.indata.swap(t);
	}
	if (op.extent.truncate_seq > seq) {
	  // write arrives before trimtrunc
	  if (obs.exists && !oi.is_whiteout()) {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
	      << ", truncating to " << op.extent.truncate_size << dendl;
	    t->truncate(soid, op.extent.truncate_size);
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	    if (oi.size > op.extent.truncate_size) {
	      interval_set<uint64_t> trim;
	      trim.insert(op.extent.truncate_size,
		oi.size - op.extent.truncate_size);
	      ctx->modified_ranges.union_of(trim);
	      ctx->clean_regions.mark_data_region_dirty(op.extent.truncate_size, oi.size - op.extent.truncate_size);
	      oi.clear_data_digest();
	    }
	    if (op.extent.truncate_size != oi.size) {
	      truncate_update_size_and_usage(ctx->delta_stats,
		oi,
		op.extent.truncate_size);
	    }
	  } else {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
	      << ", but object is new" << dendl;
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	  }
	}*/
	result = check_offset_and_length(
	  op.extent.offset, op.extent.length,
	  static_cast<Option::size_t>(osd->osd_max_object_size), get_dpp());
	if (result < 0)
	  break;

	maybe_create_new_object(ctx);

	if (op.extent.length == 0) {
	  //FIXME: revise
	  t->truncate(
	    soid, op.extent.offset);
	  /*if (op.extent.offset > oi.size) {
	    t->truncate(
	      soid, op.extent.offset);
	    truncate_update_size_and_usage(ctx->delta_stats, oi,
	      op.extent.offset);
	  } else {
	    t->nop(soid);
	  }*/
	} else {
	  t->write(
	    soid, op.extent.offset, op.extent.length, osd_op.indata, op.flags);
	}

	/*if (op.extent.offset == 0 && op.extent.length >= oi.size
	  && !skip_data_digest) {
	  obs.oi.set_data_digest(osd_op.indata.crc32c(-1));
	} else if (op.extent.offset == oi.size && obs.oi.is_data_digest()) {
	  if (skip_data_digest) {
	    obs.oi.clear_data_digest();
	  } else {
	    obs.oi.set_data_digest(osd_op.indata.crc32c(obs.oi.data_digest));
	  }
	} else {
	  obs.oi.clear_data_digest();
	}*/
	write_update_size_and_usage(ctx->delta_stats, oi,
	  op.extent.offset, op.extent.length, false);
	/*ctx->clean_regions.mark_data_region_dirty(op.extent.offset, op.extent.length);
	dout(10) << "clean_regions modified" << ctx->clean_regions << dendl;*/
      }
      break;

    case CEPH_OSD_OP_WRITEFULL:
      ++ctx->num_write;
      result = 0;
      { // write full object
	//tracepoint(osd, do_osd_op_pre_writefull, soid.oid.name.c_str(), soid.snap.val, oi.size, 0, op.extent.length);
	dout(10) << __func__ << " write full 0~" << op.extent.length << dendl;

	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
	}
	result = check_offset_and_length(
	  0, op.extent.length,
	  static_cast<Option::size_t>(osd->osd_max_object_size), get_dpp());
	if (result < 0)
	  break;

	if (pool.info.has_flag(pg_pool_t::FLAG_WRITE_FADVISE_DONTNEED))
	  op.flags = op.flags | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

	maybe_create_new_object(ctx);
	if (obs.exists && op.extent.length < oi.size) {
	  t->truncate(soid, op.extent.length);
	}

	if (op.extent.length) {
	  t->write(soid, 0, op.extent.length, osd_op.indata, op.flags);
	}
	/*if (!skip_data_digest) {
	  obs.oi.set_data_digest(osd_op.indata.crc32c(-1));
	} else {
	  obs.oi.clear_data_digest();
	}
	ctx->clean_regions.mark_data_region_dirty(0,
	  std::max((uint64_t)op.extent.length, oi.size));*/
	write_update_size_and_usage(ctx->delta_stats, oi,
	  0, op.extent.length, true);
      }
      dout(10) << __func__ << " write full " << op.extent.length << dendl;
      break;

    case CEPH_OSD_OP_WRITESAME:
      ++ctx->num_write;
      //tracepoint(osd, do_osd_op_pre_writesame, soid.oid.name.c_str(), soid.snap.val, oi.size, op.writesame.offset, op.writesame.length, op.writesame.data_length);
      result = do_writesame(ctx, osd_op);
      break;

    case CEPH_OSD_OP_ROLLBACK:
      ++ctx->num_write;
      /*tracepoint(osd, do_osd_op_pre_rollback, soid.oid.name.c_str(), soid.snap.val);
      result = _rollback_to(ctx, osd_op);*/
      result = -ENOTSUP;
      break;

    case CEPH_OSD_OP_ZERO:
      //tracepoint(osd, do_osd_op_pre_zero, soid.oid.name.c_str(), soid.snap.val, op.extent.offset, op.extent.length);
      /*if (pool.info.requires_aligned_append()) {
	result = -EOPNOTSUPP;
	break;
      }*/
      ++ctx->num_write;
      { // zero
	result = check_offset_and_length(
	  op.extent.offset, op.extent.length,
	  static_cast<Option::size_t>(osd->osd_max_object_size), get_dpp());
	if (result < 0)
	  break;

	/*if (op.extent.length && obs.exists && !oi.is_whiteout())*/ {
	  t->zero(soid, op.extent.offset, op.extent.length);
	  /*interval_set<uint64_t> ch;
	  ch.insert(op.extent.offset, op.extent.length);
	  ctx->modified_ranges.union_of(ch);
	  ctx->clean_regions.mark_data_region_dirty(op.extent.offset, op.extent.length);*/
	  ctx->delta_stats.num_wr++;
	  //oi.clear_data_digest();
	/*} else {
	  // no-op*/
	}
      }
      break;
    case CEPH_OSD_OP_CREATE:
      ++ctx->num_write;
      result = 0;
      {
	//tracepoint(osd, do_osd_op_pre_create, soid.oid.name.c_str(), soid.snap.val);
	if (obs.exists && !oi.is_whiteout() &&
	  (op.flags & CEPH_OSD_OP_FLAG_EXCL)) {
	  result = -EEXIST; // this is an exclusive create
	} else {
	  if (osd_op.indata.length()) {
	    auto p = osd_op.indata.cbegin();
	    string category;
	    try {
	      decode(category, p);
	    }
	    catch (ceph::buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    // category is no longer implemented.
	  }
	  maybe_create_new_object(ctx);
	  t->nop(soid);
	}
      }
      break;

    case CEPH_OSD_OP_TRIMTRUNC:
      op.extent.offset = op.extent.truncate_size;
      // falling through

    case CEPH_OSD_OP_TRUNCATE:
      //tracepoint(osd, do_osd_op_pre_truncate, soid.oid.name.c_str(), soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset, op.extent.length, op.extent.truncate_size, op.extent.truncate_seq);
      if (pool.info.requires_aligned_append()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	// truncate
	/*if (!obs.exists || oi.is_whiteout()) {
	  dout(10) << " object dne, truncate is a no-op" << dendl;
	  break;
	}*/

	result = check_offset_and_length(
	  op.extent.offset, op.extent.length,
	  static_cast<Option::size_t>(osd->osd_max_object_size), get_dpp());
	if (result < 0)
	  break;

	/*if (op.extent.truncate_seq) {
	  ceph_assert(op.extent.offset == op.extent.truncate_size);
	  if (op.extent.truncate_seq <= oi.truncate_seq) {
	    dout(10) << " truncate seq " << op.extent.truncate_seq << " <= current " << oi.truncate_seq
	      << ", no-op" << dendl;
	    break; // old
	  }
	  dout(10) << " truncate seq " << op.extent.truncate_seq << " > current " << oi.truncate_seq
	    << ", truncating" << dendl;
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}*/

	maybe_create_new_object(ctx);
	t->truncate(soid, op.extent.offset);
	/*if (oi.size > op.extent.offset) {
	  interval_set<uint64_t> trim;
	  trim.insert(op.extent.offset, oi.size - op.extent.offset);
	  ctx->modified_ranges.union_of(trim);
	  ctx->clean_regions.mark_data_region_dirty(op.extent.offset, oi.size - op.extent.offset);
	} else if (oi.size < op.extent.offset) {
	  ctx->clean_regions.mark_data_region_dirty(oi.size, op.extent.offset - oi.size);
	} */

	if (op.extent.offset != oi.size) {
	  truncate_update_size_and_usage(ctx->delta_stats,
	    oi,
	    op.extent.offset);
	}
	ctx->delta_stats.num_wr++;
	// do no set exists, or we will break above DELETE -> TRUNCATE munging.

	//oi.clear_data_digest();
      }
      break;

    case CEPH_OSD_OP_DELETE:
      ++ctx->num_write;
      result = 0;
      /*tracepoint(osd, do_osd_op_pre_delete, soid.oid.name.c_str(), soid.snap.val);*/
      {
	result = _delete_oid(ctx);
      }
      break;

    case CEPH_OSD_OP_WATCH:
      ++ctx->num_write;
      result = 0;
      {
	/*tracepoint(osd, do_osd_op_pre_watch, soid.oid.name.c_str(), soid.snap.val,
	  op.watch.cookie, op.watch.op);*/
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	result = 0;
	uint64_t cookie = op.watch.cookie;
	entity_name_t entity = ctx->reqid.name;
	ObjectContextRef obc = ctx->obc;

	dout(10) << "watch " << ceph_osd_watch_op_name(op.watch.op)
	  << ": ctx->obc=" << (void*)obc.get() << " cookie=" << cookie
	  << " oi.version=" << oi.version.version << " ctx->at_version=" << ctx->at_version << dendl;
	dout(10) << "watch: oi.user_version=" << oi.user_version << dendl;
	dout(10) << "watch: peer_addr="
	  << ctx->op->get_req()->get_connection()->get_peer_addr() << dendl;

	uint32_t timeout = cct->_conf->osd_client_watch_timeout;
	if (op.watch.timeout != 0) {
	  timeout = op.watch.timeout;
	}

	watch_info_t w(cookie, timeout,
	  ctx->op->get_req()->get_connection()->get_peer_addr());
	if (op.watch.op == CEPH_OSD_WATCH_OP_WATCH ||
	  op.watch.op == CEPH_OSD_WATCH_OP_LEGACY_WATCH) {
	  if (oi.watchers.count(make_pair(cookie, entity))) {
	    dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  } else {
	    dout(10) << " registered new watch " << w << " by " << entity << dendl;
	    oi.watchers[make_pair(cookie, entity)] = w;
	    t->nop(soid);  // make sure update the object_info on disk!
	  }
	  bool will_ping = (op.watch.op == CEPH_OSD_WATCH_OP_WATCH);
	  ctx->watch_connects.push_back(make_pair(w, will_ping));
	} else if (op.watch.op == CEPH_OSD_WATCH_OP_RECONNECT) {
	  if (!oi.watchers.count(make_pair(cookie, entity))) {
	    result = -ENOTCONN;
	    break;
	  }
	  dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  ctx->watch_connects.push_back(make_pair(w, true));
	} else if (op.watch.op == CEPH_OSD_WATCH_OP_PING) {
	  /* Note: WATCH with PING doesn't cause may_write() to return true,
	   * so if there is nothing else in the transaction, this is going
	   * to run do_osd_op_effects, but not write out a log entry */
	  if (!oi.watchers.count(make_pair(cookie, entity))) {
	    result = -ENOTCONN;
	    break;
	  }
	  map<pair<uint64_t, entity_name_t>, WatchRef>::iterator p =
	    obc->watchers.find(make_pair(cookie, entity));
	  if (p == obc->watchers.end() ||
	    !p->second->is_connected()) {
	    // client needs to reconnect
	    result = -ETIMEDOUT;
	    break;
	  }
	  dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  p->second->got_ping(ceph_clock_now());
	  result = 0;
	} else if (op.watch.op == CEPH_OSD_WATCH_OP_UNWATCH) {
	  map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator oi_iter =
	    oi.watchers.find(make_pair(cookie, entity));
	  if (oi_iter != oi.watchers.end()) {
	    dout(10) << " removed watch " << oi_iter->second << " by "
	      << entity << dendl;
	    oi.watchers.erase(oi_iter);
	    t->nop(soid);  // update oi on disk
	    ctx->watch_disconnects.push_back(
	      watch_disconnect_t(cookie, entity, false));
	  }
	  else {
	    dout(10) << " can't remove: no watch by " << entity << dendl;
	  }
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_PIN:
      /*tracepoint(osd, do_osd_op_pre_cache_pin, soid.oid.name.c_str(), soid.snap.val);
      if ((!pool.info.is_tier() ||
	pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE)) {
	result = -EINVAL;
	dout(10) << " pin object is only allowed on the cache tier " << dendl;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}

	if (!oi.is_cache_pinned()) {
	  oi.set_flag(object_info_t::FLAG_CACHE_PIN);
	  ctx->modify = true;
	  ctx->delta_stats.num_objects_pinned++;
	  ctx->delta_stats.num_wr++;
	}
      }*/
      result = -ENOTSUP;
      break;

    case CEPH_OSD_OP_CACHE_UNPIN:
      //tracepoint(osd, do_osd_op_pre_cache_unpin, soid.oid.name.c_str(), soid.snap.val);
      /*if ((!pool.info.is_tier() ||
	pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE)) {
	result = -EINVAL;
	dout(10) << " pin object is only allowed on the cache tier " << dendl;
	break;
      }*/
      ++ctx->num_write;
      /*result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}

	if (oi.is_cache_pinned()) {
	  oi.clear_flag(object_info_t::FLAG_CACHE_PIN);
	  ctx->modify = true;
	  ctx->delta_stats.num_objects_pinned--;
	  ctx->delta_stats.num_wr++;
	}
      }*/
      result = -ENOTSUP;
      break;

    case CEPH_OSD_OP_SET_REDIRECT:
      ++ctx->num_write;
      result = -ENOTSUP;
      /*result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}

	object_t target_name;
	object_locator_t target_oloc;
	snapid_t target_snapid = (uint64_t)op.copy_from.snapid;
	version_t target_version = op.copy_from.src_version;
	try {
	  decode(target_name, bp);
	  decode(target_oloc, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	pg_t raw_pg;
	get_osdmap()->object_locator_to_pg(target_name, target_oloc, raw_pg);
	hobject_t target(target_name, target_oloc.key, target_snapid,
	  raw_pg.ps(), raw_pg.pool(),
	  target_oloc.nspace);
	if (target == soid) {
	  dout(20) << " set-redirect self is invalid" << dendl;
	  result = -EINVAL;
	  break;
	}

	bool need_reference = (osd_op.op.flags & CEPH_OSD_OP_FLAG_WITH_REFERENCE);
	bool has_reference = (oi.flags & object_info_t::FLAG_REDIRECT_HAS_REFERENCE);
	if (has_reference) {
	  result = -EINVAL;
	  dout(5) << " the object is already a manifest " << dendl;
	  break;
	}
	if (op_finisher == nullptr && need_reference) {
	  // start
	  ctx->op_finishers[ctx->current_osd_subop_num].reset(
	    new SetManifestFinisher(osd_op));
	  ManifestOpRef mop = std::make_shared<ManifestOp>(new RefCountCallback(ctx, osd_op));
	  auto* fin = new C_SetManifestRefCountDone(this, soid, 0);
	  ceph_tid_t tid = refcount_manifest(soid, target,
	    refcount_t::INCREMENT_REF, fin, std::nullopt);
	  fin->tid = tid;
	  mop->num_chunks++;
	  mop->tids[0] = tid;
	  manifest_ops[soid] = mop;
	  ctx->obc->start_block();
	  result = -EINPROGRESS;
	} else {
	  // finish
	  if (op_finisher) {
	    result = op_finisher->execute();
	    ceph_assert(result == 0);
	  }

	  if (!oi.has_manifest() && !oi.manifest.is_redirect())
	    ctx->delta_stats.num_objects_manifest++;

	  oi.set_flag(object_info_t::FLAG_MANIFEST);
	  oi.manifest.redirect_target = target;
	  oi.manifest.type = object_manifest_t::TYPE_REDIRECT;
	  t->truncate(soid, 0);
	  ctx->clean_regions.mark_data_region_dirty(0, oi.size);
	  if (oi.is_omap() && pool.info.supports_omap()) {
	    t->omap_clear(soid);
	    obs.oi.clear_omap_digest();
	    obs.oi.clear_flag(object_info_t::FLAG_OMAP);
	    ctx->clean_regions.mark_omap_dirty();
	  }

	  write_update_size_and_usage(ctx->delta_stats, oi,
	    0, oi.size, false);
	  ctx->delta_stats.num_bytes -= oi.size;
	  oi.size = 0;
	  oi.new_object();
	  oi.user_version = target_version;
	  ctx->user_at_version = target_version;
	  // rm_attrs
	  map<string, bufferlist, less<>> rmattrs;
	  result = getattrs_maybe_cache(ctx->obc, &rmattrs);
	  if (result < 0) {
	    dout(10) << __func__ << " error: " << cpp_strerror(result) << dendl;
	    return result;
	  }
	  map<string, bufferlist>::iterator iter;
	  for (iter = rmattrs.begin(); iter != rmattrs.end(); ++iter) {
	    const string& name = iter->first;
	    t->rmattr(soid, name);
	  }
	  if (!has_reference && need_reference) {
	    oi.set_flag(object_info_t::FLAG_REDIRECT_HAS_REFERENCE);
	  }
	  dout(10) << "set-redirect oid:" << oi.soid << " user_version: " << oi.user_version << dendl;
	  if (op_finisher) {
	    ctx->op_finishers.erase(ctx->current_osd_subop_num);
	  }
	}
      }*/

      break;

    case CEPH_OSD_OP_SET_CHUNK:
      ++ctx->num_write;
      result = -ENOTSUP;
      /*result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (oi.manifest.is_redirect()) {
	  result = -EINVAL;
	  goto fail;
	}

	object_locator_t tgt_oloc;
	uint64_t src_offset, src_length, tgt_offset;
	object_t tgt_name;
	try {
	  decode(src_offset, bp);
	  decode(src_length, bp);
	  decode(tgt_oloc, bp);
	  decode(tgt_name, bp);
	  decode(tgt_offset, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}

	if (!src_length) {
	  result = -EINVAL;
	  goto fail;
	}
	if (src_offset + src_length > oi.size) {
	  result = -ERANGE;
	  goto fail;
	}
	if (!(osd_op.op.flags & CEPH_OSD_OP_FLAG_WITH_REFERENCE)) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (pool.info.is_erasure()) {
	  result = -EOPNOTSUPP;
	  break;
	}

	for (auto& p : oi.manifest.chunk_map) {
	  interval_set<uint64_t> chunk;
	  chunk.insert(p.first, p.second.length);
	  if (chunk.intersects(src_offset, src_length)) {
	    dout(20) << __func__ << " overlapped !! offset: " << src_offset << " length: " << src_length
	      << " chunk_info: " << p << dendl;
	    result = -EOPNOTSUPP;
	    goto fail;
	  }
	}

	pg_t raw_pg;
	chunk_info_t chunk_info;
	get_osdmap()->object_locator_to_pg(tgt_name, tgt_oloc, raw_pg);
	hobject_t target(tgt_name, tgt_oloc.key, snapid_t(),
	  raw_pg.ps(), raw_pg.pool(),
	  tgt_oloc.nspace);
	bool has_reference = (oi.manifest.chunk_map.find(src_offset) != oi.manifest.chunk_map.end()) &&
	  (oi.manifest.chunk_map[src_offset].test_flag(chunk_info_t::FLAG_HAS_REFERENCE));
	if (has_reference) {
	  result = -EINVAL;
	  dout(5) << " the object is already a manifest " << dendl;
	  break;
	}
	chunk_info.oid = target;
	chunk_info.offset = tgt_offset;
	chunk_info.length = src_length;
	if (op_finisher == nullptr) {
	  // start
	  ctx->op_finishers[ctx->current_osd_subop_num].reset(
	    new SetManifestFinisher(osd_op));
	  object_manifest_t set_chunk;
	  bool need_inc_ref = false;
	  set_chunk.chunk_map[src_offset] = chunk_info;
	  need_inc_ref = inc_refcount_by_set(ctx, set_chunk, osd_op);
	  if (need_inc_ref) {
	    result = -EINPROGRESS;
	    break;
	  }
	}
	if (op_finisher) {
	  result = op_finisher->execute();
	  ceph_assert(result == 0);
	}

	oi.manifest.chunk_map[src_offset] = chunk_info;
	if (!oi.has_manifest() && !oi.manifest.is_chunked())
	  ctx->delta_stats.num_objects_manifest++;
	oi.set_flag(object_info_t::FLAG_MANIFEST);
	oi.manifest.type = object_manifest_t::TYPE_CHUNKED;
	if (!has_reference) {
	  oi.manifest.chunk_map[src_offset].set_flag(chunk_info_t::FLAG_HAS_REFERENCE);
	}
	ctx->modify = true;
	ctx->cache_operation = true;

	dout(10) << "set-chunked oid:" << oi.soid << " user_version: " << oi.user_version
	  << " chunk_info: " << chunk_info << dendl;
	if (op_finisher) {
	  ctx->op_finishers.erase(ctx->current_osd_subop_num);
	}
      }*/

      break;

    case CEPH_OSD_OP_TIER_PROMOTE:
      ++ctx->num_write;
      result = -ENOTSUP;
      /*result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (!obs.oi.has_manifest()) {
	  result = 0;
	  break;
	}

	if (op_finisher == nullptr) {
	  PromoteManifestCallback* cb;
	  object_locator_t my_oloc;
	  hobject_t src_hoid;

	  if (obs.oi.manifest.is_chunked()) {
	    src_hoid = obs.oi.soid;
	  }
	  else if (obs.oi.manifest.is_redirect()) {
	    object_locator_t src_oloc(obs.oi.manifest.redirect_target);
	    my_oloc = src_oloc;
	    src_hoid = obs.oi.manifest.redirect_target;
	  }
	  else {
	    ceph_abort_msg("unrecognized manifest type");
	  }
	  cb = new PromoteManifestCallback(ctx->obc, this, ctx);
	  ctx->op_finishers[ctx->current_osd_subop_num].reset(
	    new PromoteFinisher(cb));
	  unsigned flags = CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY |
	    CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
	    CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE |
	    CEPH_OSD_COPY_FROM_FLAG_RWORDERED;
	  unsigned src_fadvise_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL;
	  start_copy(cb, ctx->obc, src_hoid, my_oloc, 0, flags,
	    obs.oi.soid.snap == CEPH_NOSNAP,
	    src_fadvise_flags, 0);

	  dout(10) << "tier-promote oid:" << oi.soid << " manifest: " << obs.oi.manifest << dendl;
	  result = -EINPROGRESS;
	}
	else {
	  result = op_finisher->execute();
	  ceph_assert(result == 0);
	  ctx->op_finishers.erase(ctx->current_osd_subop_num);
	}
      }*/

      break;

    case CEPH_OSD_OP_TIER_FLUSH:
      ++ctx->num_write;
      result = -ENOTSUP;
      /*result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::octopus) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (!obs.oi.has_manifest()) {
	  result = 0;
	  break;
	}

	if (oi.is_dirty()) {
	  result = start_flush(ctx->op, ctx->obc, true, NULL, std::nullopt);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	}
	else {
	  result = 0;
	}
      }	*/

      break;

    case CEPH_OSD_OP_TIER_EVICT:
      ++ctx->num_write;
      result = -ENOTSUP;
      /*result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::octopus) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (!obs.oi.has_manifest()) {
	  result = -EINVAL;
	  break;
	}

	// The chunks already has a reference, so it is just enough to invoke truncate if necessary
	uint64_t chunk_length = 0;
	for (auto p : obs.oi.manifest.chunk_map) {
	  chunk_length += p.second.length;
	}
	if (chunk_length == obs.oi.size) {
	  for (auto& p : obs.oi.manifest.chunk_map) {
	    p.second.set_flag(chunk_info_t::FLAG_MISSING);
	  }
	  // punch hole
	  t->zero(soid, 0, oi.size);
	  oi.clear_data_digest();
	  ctx->delta_stats.num_wr++;
	  ctx->cache_operation = true;
	}
	osd->logger->inc(l_osd_tier_evict);
      }	*/

      break;

    case CEPH_OSD_OP_UNSET_MANIFEST:
      ++ctx->num_write;
      result = -ENOTSUP;
      /*result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (!oi.has_manifest()) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}

	dec_all_refcount_manifest(oi, ctx);

	oi.clear_flag(object_info_t::FLAG_MANIFEST);
	oi.manifest = object_manifest_t();
	ctx->delta_stats.num_objects_manifest--;
	ctx->delta_stats.num_wr++;
	ctx->modify = true;
      }*/

      break;

      // -- object attrs --

    case CEPH_OSD_OP_SETXATTR:
      ++ctx->num_write;
      result = 0;
      {
	if (cct->_conf->osd_max_attr_size > 0 &&
	  op.xattr.value_len > cct->_conf->osd_max_attr_size) {
	  //tracepoint(osd, do_osd_op_pre_setxattr, soid.oid.name.c_str(), soid.snap.val, "???");
	  result = -EFBIG;
	  break;
	}
	unsigned max_name_len =
	  std::min<uint64_t>(osd->store->get_max_attr_name_length(),
	    cct->_conf->osd_max_attr_name_len);
	if (op.xattr.name_len > max_name_len) {
	  result = -ENAMETOOLONG;
	  break;
	}
	maybe_create_new_object(ctx);
	string aname;
	bp.copy(op.xattr.name_len, aname);
	//tracepoint(osd, do_osd_op_pre_setxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	string name = "_" + aname;
	bufferlist bl;
	bp.copy(op.xattr.value_len, bl);
	t->setattr(soid, name, bl);
	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      ++ctx->num_write;
      result = 0;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	//tracepoint(osd, do_osd_op_pre_rmxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	/*if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}*/
	string name = "_" + aname;
	t->rmattr(soid, name);
	ctx->delta_stats.num_wr++;
      }
      break;


      // -- fancy writers --
    case CEPH_OSD_OP_APPEND:
    /*{
      tracepoint(osd, do_osd_op_pre_append, soid.oid.name.c_str(), soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset, op.extent.length, op.extent.truncate_size, op.extent.truncate_seq);
      // just do it inline; this works because we are happy to execute
      // fancy op on replicas as well.
      vector<OSDOp> nops(1);
      OSDOp& newop = nops[0];
      newop.op.op = CEPH_OSD_OP_WRITE;
      newop.op.extent.offset = oi.size;
      newop.op.extent.length = op.extent.length;
      newop.op.extent.truncate_seq = oi.truncate_seq;
      newop.indata = osd_op.indata;
      result = do_osd_ops(ctx, nops);
      osd_op.outdata = std::move(newop.outdata);
    }*/
    result = -ENOTSUP; // FIXME: need object size
    break;

    case CEPH_OSD_OP_STARTSYNC:
      result = 0;
      t->nop(soid);
      break;

      // -- trivial map --
    case CEPH_OSD_OP_TMAPGET:
      //tracepoint(osd, do_osd_op_pre_tmapget, soid.oid.name.c_str(), soid.snap.val);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      {
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_SYNC_READ;
	newop.op.extent.offset = 0;
	newop.op.extent.length = 0;
	result = do_osd_ops(ctx, nops);
	osd_op.outdata = std::move(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_TMAPPUT:
      //tracepoint(osd, do_osd_op_pre_tmapput, soid.oid.name.c_str(), soid.snap.val);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      {
	//_dout_lock.Lock();
	//osd_op.data.hexdump(*_dout);
	//_dout_lock.Unlock();

	// verify sort order
	bool unsorted = false;
	if (true) {
	  bufferlist header;
	  decode(header, bp);
	  uint32_t n;
	  decode(n, bp);
	  string last_key;
	  while (n--) {
	    string key;
	    decode(key, bp);
	    dout(10) << "tmapput key " << key << dendl;
	    bufferlist val;
	    decode(val, bp);
	    if (key < last_key) {
	      dout(10) << "TMAPPUT is unordered; resorting" << dendl;
	      unsorted = true;
	      break;
	    }
	    last_key = key;
	  }
	}

	// write it
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITEFULL;
	newop.op.extent.offset = 0;
	newop.op.extent.length = osd_op.indata.length();
	newop.indata = osd_op.indata;

	if (unsorted) {
	  bp = osd_op.indata.begin();
	  bufferlist header;
	  map<string, bufferlist> m;
	  decode(header, bp);
	  decode(m, bp);
	  ceph_assert(bp.end());
	  bufferlist newbl;
	  encode(header, newbl);
	  encode(m, newbl);
	  newop.indata = newbl;
	}
	result = do_osd_ops(ctx, nops);
	ceph_assert(result == 0);
      }
      break;

    case CEPH_OSD_OP_TMAPUP:
      /*tracepoint(osd, do_osd_op_pre_tmapup, soid.oid.name.c_str(), soid.snap.val);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = do_tmapup(ctx, bp, osd_op);*/
      result = -EOPNOTSUPP;
      break;

    case CEPH_OSD_OP_TMAP2OMAP:
      /*++ctx->num_write;
      tracepoint(osd, do_osd_op_pre_tmap2omap, soid.oid.name.c_str(), soid.snap.val);
      result = do_tmap2omap(ctx, op.tmap2omap.flags);*/
      result = -EOPNOTSUPP;
      break;

      // OMAP Read ops
    case CEPH_OSD_OP_OMAPGETKEYS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	try {
	  decode(start_after, bp);
	  decode(max_return, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  //tracepoint(osd, do_osd_op_pre_omapgetkeys, soid.oid.name.c_str(), soid.snap.val, "???", 0);
	  goto fail;
	}
	if (max_return > cct->_conf->osd_max_omap_entries_per_request) {
	  max_return = cct->_conf->osd_max_omap_entries_per_request;
	}
	//tracepoint(osd, do_osd_op_pre_omapgetkeys, soid.oid.name.c_str(), soid.snap.val, start_after.c_str(), max_return);

	bufferlist bl;
	uint32_t num = 0;
	bool truncated = false;
	if (oi.is_omap()) {
	  ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	    ch, ghobject_t(soid)
	  );
	  ceph_assert(iter);
	  iter->upper_bound(start_after);
	  for (num = 0; iter->valid(); ++num, iter->next()) {
	    if (num >= max_return ||
	      bl.length() >= cct->_conf->osd_max_omap_bytes_per_request) {
	      truncated = true;
	      break;
	    }
	    encode(iter->key(), bl);
	  }
	} // else return empty out_set
	encode(num, osd_op.outdata);
	osd_op.outdata.claim_append(bl);
	encode(truncated, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	string filter_prefix;
	try {
	  decode(start_after, bp);
	  decode(max_return, bp);
	  decode(filter_prefix, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  //tracepoint(osd, do_osd_op_pre_omapgetvals, soid.oid.name.c_str(), soid.snap.val, "???", 0, "???");
	  goto fail;
	}
	if (max_return > cct->_conf->osd_max_omap_entries_per_request) {
	  max_return = cct->_conf->osd_max_omap_entries_per_request;
	}
	//tracepoint(osd, do_osd_op_pre_omapgetvals, soid.oid.name.c_str(), soid.snap.val, start_after.c_str(), max_return, filter_prefix.c_str());

	uint32_t num = 0;
	bool truncated = false;
	bufferlist bl;
	if (oi.is_omap()) {
	  ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	    ch, ghobject_t(soid)
	  );
	  if (!iter) {
	    result = -ENOENT;
	    goto fail;
	  }
	  iter->upper_bound(start_after);
	  if (filter_prefix > start_after) iter->lower_bound(filter_prefix);
	  for (num = 0;
	    iter->valid() &&
	    iter->key().substr(0, filter_prefix.size()) == filter_prefix;
	    ++num, iter->next()) {
	    dout(20) << "Found key " << iter->key() << dendl;
	    if (num >= max_return ||
	      bl.length() >= cct->_conf->osd_max_omap_bytes_per_request) {
	      truncated = true;
	      break;
	    }
	    encode(iter->key(), bl);
	    encode(iter->value(), bl);
	  }
	} // else return empty out_set
	encode(num, osd_op.outdata);
	osd_op.outdata.claim_append(bl);
	encode(truncated, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETHEADER:
      //tracepoint(osd, do_osd_op_pre_omapgetheader, soid.oid.name.c_str(), soid.snap.val);
      if (!oi.is_omap()) {
	// return empty header
	break;
      }
      ++ctx->num_read;
      {
	osd->store->omap_get_header(ch, ghobject_t(soid), &osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
      ++ctx->num_read;
      {
	set<string> keys_to_get;
	try {
	  decode(keys_to_get, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  //tracepoint(osd, do_osd_op_pre_omapgetvalsbykeys, soid.oid.name.c_str(), soid.snap.val, "???");
	  goto fail;
	}
	//tracepoint(osd, do_osd_op_pre_omapgetvalsbykeys, soid.oid.name.c_str(), soid.snap.val, list_entries(keys_to_get).c_str());
	map<string, bufferlist> out;
	if (oi.is_omap()) {
	  osd->store->omap_get_values(ch, ghobject_t(soid), keys_to_get, &out);
	} // else return empty omap entries
	encode(out, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAP_CMP:
      ++ctx->num_read;
      {
	/*if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  //tracepoint(osd, do_osd_op_pre_omap_cmp, soid.oid.name.c_str(), soid.snap.val, "???");
	  break;
	}*/
	map<string, pair<bufferlist, int> > assertions;
	try {
	  decode(assertions, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  //tracepoint(osd, do_osd_op_pre_omap_cmp, soid.oid.name.c_str(), soid.snap.val, "???");
	  goto fail;
	}
	//tracepoint(osd, do_osd_op_pre_omap_cmp, soid.oid.name.c_str(), soid.snap.val, list_keys(assertions).c_str());

	map<string, bufferlist> out;

	if (oi.is_omap()) {
	  set<string> to_get;
	  for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	    i != assertions.end();
	    ++i)
	    to_get.insert(i->first);
	  int r = osd->store->omap_get_values(ch, ghobject_t(soid),
	    to_get, &out);
	  if (r < 0) {
	    result = r;
	    break;
	  }
	} // else leave out empty

	//Should set num_rd_kb based on encode length of map
	ctx->delta_stats.num_rd++;

	int r = 0;
	bufferlist empty;
	for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	  i != assertions.end();
	  ++i) {
	  auto out_entry = out.find(i->first);
	  bufferlist& bl = (out_entry != out.end()) ?
	    out_entry->second : empty;
	  switch (i->second.second) {
	  case CEPH_OSD_CMPXATTR_OP_EQ:
	    if (!(bl == i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_LT:
	    if (!(bl < i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_GT:
	    if (!(bl > i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  default:
	    r = -EINVAL;
	    break;
	  }
	  if (r < 0)
	    break;
	}
	if (r < 0) {
	  result = r;
	}
      }
      break;

      // OMAP Write ops
    case CEPH_OSD_OP_OMAPSETVALS:
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	//tracepoint(osd, do_osd_op_pre_omapsetvals, soid.oid.name.c_str(), soid.snap.val);
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	maybe_create_new_object(ctx);
	bufferlist to_set_bl;
	try {
	  decode_str_str_map_to_bl(bp, &to_set_bl);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  //tracepoint(osd, do_osd_op_pre_omapsetvals, soid.oid.name.c_str(), soid.snap.val);
	  goto fail;
	}
	//tracepoint(osd, do_osd_op_pre_omapsetvals, soid.oid.name.c_str(), soid.snap.val);
	if (cct->_conf->subsys.should_gather<dout_subsys, 20>()) {
	  dout(20) << "setting vals: " << dendl;
	  map<string, bufferlist> to_set;
	  bufferlist::const_iterator pt = to_set_bl.begin();
	  decode(to_set, pt);
	  for (map<string, bufferlist>::iterator i = to_set.begin();
	    i != to_set.end();
	    ++i) {
	    dout(20) << "\t" << i->first << dendl;
	  }
	}
	t->omap_setkeys(soid, to_set_bl);
	//ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
	ctx->delta_stats.num_wr_kb += shift_round_up(to_set_bl.length(), 10);
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_OMAPSETHEADER:
      //tracepoint(osd, do_osd_op_pre_omapsetheader, soid.oid.name.c_str(), soid.snap.val);
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	maybe_create_new_object(ctx);
	t->omap_setheader(soid, osd_op.indata);
	//ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_OMAPCLEAR:
      //tracepoint(osd, do_osd_op_pre_omapclear, soid.oid.name.c_str(), soid.snap.val);
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	/*if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}*/
	if (oi.is_omap()) {
	  t->omap_clear(soid);
	 // ctx->clean_regions.mark_omap_dirty();
	  ctx->delta_stats.num_wr++;
	  obs.oi.clear_omap_digest();
	  obs.oi.clear_flag(object_info_t::FLAG_OMAP);
	}
	t->omap_clear(soid);
      }
      break;

    case CEPH_OSD_OP_OMAPRMKEYS:
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	//tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	/*if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  //tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	  break;
	}*/
	bufferlist to_rm_bl;
	try {
	  decode_str_set_to_bl(bp, &to_rm_bl);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  //tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	  goto fail;
	}
	//tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	t->omap_rmkeys(soid, to_rm_bl);
	//ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
      }
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_OMAPRMKEYRANGE:
      //tracepoint(osd, do_osd_op_pre_omaprmkeyrange, soid.oid.name.c_str(), soid.snap.val);
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	/*if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}*/
	std::string key_begin, key_end;
	try {
	  decode(key_begin, bp);
	  decode(key_end, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t->omap_rmkeyrange(soid, key_begin, key_end);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_COPY_GET:
      result = -ENOTSUP;
      /*++ctx->num_read;
      tracepoint(osd, do_osd_op_pre_copy_get, soid.oid.name.c_str(),
	soid.snap.val);
      if (op_finisher == nullptr) {
	result = do_copy_get(ctx, bp, osd_op, ctx->obc);
      } else {
	result = op_finisher->execute();
      }*/
      break;

    case CEPH_OSD_OP_COPY_FROM:
    case CEPH_OSD_OP_COPY_FROM2:
      result = -ENOTSUP;
      /*
      ++ctx->num_write;
      result = 0;
      {
	object_t src_name;
	object_locator_t src_oloc;
	uint32_t truncate_seq = 0;
	uint64_t truncate_size = 0;
	bool have_truncate = false;
	snapid_t src_snapid = (uint64_t)op.copy_from.snapid;
	version_t src_version = op.copy_from.src_version;

	if ((op.op == CEPH_OSD_OP_COPY_FROM2) &&
	  (op.copy_from.flags & ~CEPH_OSD_COPY_FROM_FLAGS)) {
	  dout(20) << "invalid copy-from2 flags 0x"
	    << std::hex << (int)op.copy_from.flags << std::dec << dendl;
	  result = -EINVAL;
	  break;
	}
	try {
	  decode(src_name, bp);
	  decode(src_oloc, bp);
	  // check if client sent us truncate_seq and truncate_size
	  if ((op.op == CEPH_OSD_OP_COPY_FROM2) &&
	    (op.copy_from.flags & CEPH_OSD_COPY_FROM_FLAG_TRUNCATE_SEQ)) {
	    decode(truncate_seq, bp);
	    decode(truncate_size, bp);
	    have_truncate = true;
	  }
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd,
	    do_osd_op_pre_copy_from,
	    soid.oid.name.c_str(),
	    soid.snap.val,
	    "???",
	    0,
	    "???",
	    "???",
	    0,
	    src_snapid,
	    src_version);
	  goto fail;
	}
	tracepoint(osd,
	  do_osd_op_pre_copy_from,
	  soid.oid.name.c_str(),
	  soid.snap.val,
	  src_name.name.c_str(),
	  src_oloc.pool,
	  src_oloc.key.c_str(),
	  src_oloc.nspace.c_str(),
	  src_oloc.hash,
	  src_snapid,
	  src_version);
	if (op_finisher == nullptr) {
	  // start
	  pg_t raw_pg;
	  get_osdmap()->object_locator_to_pg(src_name, src_oloc, raw_pg);
	  hobject_t src(src_name, src_oloc.key, src_snapid,
	    raw_pg.ps(), raw_pg.pool(),
	    src_oloc.nspace);
	  if (src == soid) {
	    dout(20) << " copy from self is invalid" << dendl;
	    result = -EINVAL;
	    break;
	  }
	  CopyFromCallback* cb = new CopyFromCallback(ctx, osd_op);
	  if (have_truncate)
	    cb->set_truncate(truncate_seq, truncate_size);
	  ctx->op_finishers[ctx->current_osd_subop_num].reset(
	    new CopyFromFinisher(cb));
	  start_copy(cb, ctx->obc, src, src_oloc, src_version,
	    op.copy_from.flags,
	    false,
	    op.copy_from.src_fadvise_flags,
	    op.flags);
	  result = -EINPROGRESS;
	}
	else {
	  // finish
	  result = op_finisher->execute();
	  ceph_assert(result == 0);

	  // COPY_FROM cannot be executed multiple times -- it must restart
	  ctx->op_finishers.erase(ctx->current_osd_subop_num);
	}
      }*/
      break;

    default:
      //tracepoint(osd, do_osd_op_pre_unknown, soid.oid.name.c_str(), soid.snap.val, op.op, ceph_osd_op_name(op.op));
      dout(1) << "unrecognized osd op " << op.op
	<< " " << ceph_osd_op_name(op.op)
	<< dendl;
      result = -EOPNOTSUPP;
    }

  fail:
    osd_op.rval = result;
    //tracepoint(osd, do_osd_op_post, soid.oid.name.c_str(), soid.snap.val, op.op, ceph_osd_op_name(op.op), op.flags, result);
    if (result < 0 && (op.flags & CEPH_OSD_OP_FLAG_FAILOK) &&
      result != -EAGAIN && result != -EINPROGRESS)
      result = 0;

    if (result < 0)
      break;
  }
  if (result < 0) {
    dout(10) << __func__ << " error: " << cpp_strerror(result) << dendl;
  }
  return result;
}

void TransparentPG::write_update_size_and_usage(
  object_stat_sum_t& delta_stats,
  object_info_t& oi,
  uint64_t offset,
  uint64_t length,
  bool write_full)
{
  if (write_full ||
      (offset + length > oi.size && length)) {
    uint64_t new_size = offset + length;
    delta_stats.num_bytes -= oi.size;
    delta_stats.num_bytes += new_size;
    oi.size = new_size;
  }

  delta_stats.num_wr++;
  delta_stats.num_wr_kb += shift_round_up(length, 10);
}

void TransparentPG::truncate_update_size_and_usage(
  object_stat_sum_t& delta_stats,
  object_info_t& oi,
  uint64_t truncate_size)
{
  if (oi.size != truncate_size) {
    delta_stats.num_bytes -= oi.size;
    delta_stats.num_bytes += truncate_size;
    oi.size = truncate_size;
  }
}

void TransparentPG::complete_disconnect_watches(
  ObjectContextRef obc,
  const list<watch_disconnect_t> &to_disconnect)
{
  for (list<watch_disconnect_t>::const_iterator i =
         to_disconnect.begin();
       i != to_disconnect.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->cookie, i->name);
    auto watchers_entry = obc->watchers.find(watcher);
    if (watchers_entry != obc->watchers.end()) {
      WatchRef watch = watchers_entry->second;
      dout(10) << "do_osd_op_effects disconnect watcher " << watcher << dendl;
      obc->watchers.erase(watcher);
      watch->remove(i->send_disconnect);
    } else {
      dout(10) << "do_osd_op_effects disconnect failed to find watcher "
               << watcher << dendl;
    }
  }
}

void TransparentPG::do_osd_op_effects(OpContext *ctx, const ConnectionRef& conn)
{
  entity_name_t entity = ctx->reqid.name;
  dout(15) << "do_osd_op_effects " << entity << " con " << conn.get() << dendl;

  // disconnects first
  complete_disconnect_watches(ctx->obc, ctx->watch_disconnects);

  ceph_assert(conn);

  auto session = conn->get_priv();
  if (!session)
    return;

  for (list<pair<watch_info_t,bool> >::iterator i = ctx->watch_connects.begin();
       i != ctx->watch_connects.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->first.cookie, entity);
    dout(15) << "do_osd_op_effects applying watch connect on session "
             << session.get() << " watcher " << watcher << dendl;
    WatchRef watch;
    if (ctx->obc->watchers.count(watcher)) {
      dout(15) << "do_osd_op_effects found existing watch watcher " << watcher
               << dendl;
      watch = ctx->obc->watchers[watcher];
    } else {
      dout(15) << "do_osd_op_effects new watcher " << watcher
               << dendl;
      watch = Watch::makeWatchRef(
        this, osd, ctx->obc, i->first.timeout_seconds,
        i->first.cookie, entity, conn->get_peer_addr());
      ctx->obc->watchers.insert(
        make_pair(
          watcher,
          watch));
    }
    watch->connect(conn, i->second);
  }

  for (list<notify_info_t>::iterator p = ctx->notifies.begin();
       p != ctx->notifies.end();
       ++p) {
    dout(10) << "do_osd_op_effects, notify " << *p << dendl;
    ConnectionRef conn(ctx->op->get_req()->get_connection());
    NotifyRef notif(
      Notify::makeNotifyRef(
        conn,
        ctx->reqid.name.num(),
        p->bl,
        p->timeout,
        p->cookie,
        p->notify_id,
        ctx->obc->obs.oi.user_version,
        osd));
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
           ctx->obc->watchers.begin();
         i != ctx->obc->watchers.end();
         ++i) {
      dout(10) << "starting notify on watch " << i->first << dendl;
      i->second->start_notify(notif);
    }
    notif->init();
  }

  for (list<OpContext::NotifyAck>::iterator p = ctx->notify_acks.begin();
       p != ctx->notify_acks.end();
       ++p) {
    if (p->watch_cookie)
      dout(10) << "notify_ack " << make_pair(*(p->watch_cookie), p->notify_id) << dendl;
    else
      dout(10) << "notify_ack " << make_pair("NULL", p->notify_id) << dendl;
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
           ctx->obc->watchers.begin();
         i != ctx->obc->watchers.end();
         ++i) {
      if (i->first.second != entity) continue;
      if (p->watch_cookie &&
          *(p->watch_cookie) != i->first.first) continue;
      dout(10) << "acking notify on watch " << i->first << dendl;
      i->second->notify_ack(p->notify_id, p->reply_bl);
    }
  }
}

inline int TransparentPG::_delete_oid(OpContext *ctx)
{
  //SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  PGTransaction* t = ctx->op_t.get();

/*  // cache: cache: set whiteout on delete?
  bool whiteout = false;
  if (pool.info.cache_mode != pg_pool_t::CACHEMODE_NONE
      && !no_whiteout
      && !try_no_whiteout) {
    whiteout = true;
  }

  // in luminous or later, we can't delete the head if there are
  // clones. we trust the caller passing no_whiteout has already
  // verified they don't exist.
  if (!snapset.clones.empty() ||
      (!ctx->snapc.snaps.empty() && ctx->snapc.snaps[0] > snapset.seq)) {
    if (no_whiteout) {
      dout(20) << __func__ << " has or will have clones but no_whiteout=1"
               << dendl;
    } else {
      dout(20) << __func__ << " has or will have clones; will whiteout"
               << dendl;
      whiteout = true;
    }
  }*/
  dout(20) << __func__ << " " << soid
           << dendl;
  if (!obs.exists) {
    return -ENOENT;
  }

  t->remove(soid);

/*  if (oi.size > 0) {
    interval_set<uint64_t> ch;
    ch.insert(0, oi.size);
    ctx->modified_ranges.union_of(ch);
    ctx->clean_regions.mark_data_region_dirty(0, oi.size);
  }

  ctx->clean_regions.mark_omap_dirty();*/
  ctx->delta_stats.num_wr++;
  if (soid.is_snap()) {
    ceph_assert(ctx->obc->ssc->snapset.clone_overlap.count(soid.snap));
    ctx->delta_stats.num_bytes -= ctx->obc->ssc->snapset.get_clone_bytes(soid.snap);
  } else {
    ctx->delta_stats.num_bytes -= oi.size;
  }
  oi.size = 0;
  oi.new_object();

  // disconnect all watchers
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator p =
         oi.watchers.begin();
       p != oi.watchers.end();
       ++p) {
    dout(20) << __func__ << " will disconnect watcher " << p->first << dendl;
    ctx->watch_disconnects.push_back(
      watch_disconnect_t(p->first.first, p->first.second, true));
  }
  oi.watchers.clear();

/*  if (whiteout) {
    dout(20) << __func__ << " setting whiteout on " << soid << dendl;
    oi.set_flag(object_info_t::FLAG_WHITEOUT);
    ctx->delta_stats.num_whiteouts++;
    t->create(soid);
    osd->logger->inc(l_osd_tier_whiteout);
    return 0;
  }*/

/*  if (oi.has_manifest()) {
    ctx->delta_stats.num_objects_manifest--;
    dec_all_refcount_manifest(oi, ctx);
  }*/

  // delete the head
  ctx->delta_stats.num_objects--;
/*  if (soid.is_snap())
    ctx->delta_stats.num_object_clones--;*/
  if (oi.is_whiteout()) {
    dout(20) << __func__ << " deleting whiteout on " << soid << dendl;
    ctx->delta_stats.num_whiteouts--;
    oi.clear_flag(object_info_t::FLAG_WHITEOUT);
  }
/*  if (oi.is_cache_pinned()) {
    ctx->delta_stats.num_objects_pinned--;
  }*/
  obs.exists = false;
  return 0;
}

int TransparentPG::get_manifest_ref_count(ObjectContextRef obc,
                                          std::string& fp_oid, OpRequestRef op) {
  return 0; //FIXME???
}

/*TransparentPG::OpContextUPtr TransparentPG::simple_opc_create(ObjectContextRef obc)
{
  dout(20) << __func__ << " " << obc->obs.oi.soid << dendl;
  ceph_tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContextUPtr ctx(new OpContext(OpRequestRef(), reqid, nullptr, obc, this));
  ctx->op_t.reset(new PGTransaction());
  ctx->mtime = ceph_clock_now();
  return ctx;
}

void TransparentPG::simple_opc_submit(OpContextUPtr ctx)
{
  RepGather *repop = new_repop(ctx.get(), ctx->reqid.tid);
  dout(20) << __func__ << " " << repop << dendl;
  issue_repop(repop, ctx.get());
  eval_repop(repop);
  recovery_state.update_trim_to();
  repop->put();
}*/

