// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#ifndef CEPH_TRANSPARENTPG_H
#define CEPH_TRANSPARENTPG_H

//#include <boost/tuple/tuple.hpp>
#include <map>
#include <string>
#include <functional>
#include <memory>
#include "include/ceph_assert.h"
//#include "DynamicPerfStats.h"
#include "OSD.h"
#include "PG.h"
#include "osd/scrubber/pg_scrubber.h"
/*#include "Watch.h"
#include "TierAgentState.h"
#include "messages/MOSDOpReply.h"
#include "common/Checksummer.h"
#include "common/sharedptr_registry.hpp"
#include "common/shared_cache.hpp"
#include "PGTransaction.h"
#include "cls/cas/cls_cas_ops.h"*/

#include "TransparentBackend.h"

/**
 * The derivative of PgScrubber that is used by TransparentPG.
 */
class TransparentScrub : public PgScrubber {
public:
  explicit TransparentScrub(PG* pg) : PgScrubber(pg)
  {
  }

  void _scrub_finish() final
  {
  }

  bool get_store_errors(const scrub_ls_arg_t& arg,
    scrub_ls_result_t& res_inout) const final
  {
    return false;
  }

  void stats_of_handled_objects(const object_stat_sum_t& delta_stats,
    const hobject_t& soid) final
  {
  }

private:

  void _scrub_clear_state() final
  {
  }
};

class TransparentPG : public PG, public PGBackend::Listener {
  struct OpContext;
  using OpContextUPtr = std::unique_ptr<OpContext>;
public:
  TransparentPG(OSDService* o, OSDMapRef curmap,
    const PGPool& _pool,
    const std::map<std::string, std::string>& ec_profile, spg_t p) :
    PG(o, curmap, _pool, p),
    pgbackend(
      PGBackend::build_pg_backend(
	_pool.info, ec_profile, this, coll_t(p), ch, o->store, cct)),
    object_contexts(o->cct, o->cct->_conf->osd_pg_object_context_cache_count)
/*    new_backfill(false),
    temp_seq(0),
    snap_trimmer_machine(this)*/
  {
    recovery_state.set_backend_predicates(
      pgbackend->get_is_readable_predicate(),
      pgbackend->get_is_recoverable_predicate());

    //snap_trimmer_machine.initiate(); // FIXME TRANSPARENT
    m_scrubber = std::make_unique<TransparentScrub>(this);
  }

  PGBackend* get_pgbackend() override {
    return pgbackend.get();
  }

  const PGBackend* get_pgbackend() const override {
    return pgbackend.get();
  }

  /// Debugging
  DoutPrefixProvider* get_dpp() override {
    return this;
  }

  /// Recovery

  /**
   * Called with the transaction recovering oid
   */
  void on_local_recover(
    const hobject_t& oid,
    const ObjectRecoveryInfo& recovery_info,
    ObjectContextRef obc,
    bool is_delete,
    ObjectStore::Transaction* t
  ) final
  {
  }

  /**
   * Called when transaction recovering oid is durable and
   * applied on all replicas
   */
  void on_global_recover(
    const hobject_t& oid,
    const object_stat_sum_t& stat_diff,
    bool is_delete
  ) final
  {
  }

  /**
   * Called when peer is recovered
   */
  void on_peer_recover(
    pg_shard_t peer,
    const hobject_t& oid,
    const ObjectRecoveryInfo& recovery_info
  ) final
  {
  }

  void begin_peer_recover(
    pg_shard_t peer,
    const hobject_t oid) final
  {
  }

  void apply_stats(
    const hobject_t& soid,
    const object_stat_sum_t& delta_stats) final
  {
  }

  /**
   * Called when a read from a std::set of replicas/primary fails
   */
  void on_failed_pull(
    const std::set<pg_shard_t>& from,
    const hobject_t& soid,
    const eversion_t& v
  ) final
  {
  }

  /**
   * Called when a pull on soid cannot be completed due to
   * down peers
   */
  void cancel_pull(
    const hobject_t& soid) final
  {
  }

  /**
   * Called to remove an object.
   */
  void remove_missing_object(
    const hobject_t& oid,
    eversion_t v,
    Context* on_complete) final
  {
  }

  /**
   * Bless a context
   *
   * Wraps a context in whatever outer layers the parent usually
   * uses to call into the PGBackend
   */
  Context* bless_context(Context* c) override
  {
    ceph_assert(false);
    return nullptr;
  }
  GenContext<ThreadPool::TPHandle&>* bless_gencontext(
    GenContext<ThreadPool::TPHandle&>* c) override
  {
    ceph_assert(false);
    return nullptr;
  }
  GenContext<ThreadPool::TPHandle&>* bless_unlocked_gencontext(
    GenContext<ThreadPool::TPHandle&>* c) override
  {
    ceph_assert(false);
    return nullptr;
  }

  void send_message(int to_osd, Message* m) override {
    osd->send_message_osd_cluster(to_osd, m, get_osdmap_epoch());
  }
  void queue_transaction(ObjectStore::Transaction&& t,
    OpRequestRef op) override {
    osd->store->queue_transaction(ch, std::move(t), op);
  }
  void queue_transactions(std::vector<ObjectStore::Transaction>& tls,
    OpRequestRef op) override {
    osd->store->queue_transactions(ch, tls, op, NULL);
  }
  epoch_t get_interval_start_epoch() const override {
    //return info.history.same_interval_since;
    ceph_assert(false);
    return epoch_t();
  }
  epoch_t get_last_peering_reset_epoch() const override {
    ceph_assert(false);
    return epoch_t();
  }

  const std::set<pg_shard_t>& get_acting_recovery_backfill_shards() const override
  {
    ceph_assert(false);
    return dummy_pg_shard_set;
  }
  const std::set<pg_shard_t>& get_acting_shards() const override
  {
    ceph_assert(false);
    return dummy_pg_shard_set;
  }

  const std::set<pg_shard_t>& get_backfill_shards() const override
  {
    ceph_assert(false);
    return dummy_pg_shard_set;
  }

  std::ostream& gen_dbg_prefix(std::ostream& out) const override {
    return gen_prefix(out);
  }
  const std::map<hobject_t, std::set<pg_shard_t>>& get_missing_loc_shards()
    const override
  {
    ceph_assert(false);
    return dummy_loc_shards;
  }

  const pg_missing_tracker_t& get_local_missing() const override
  {
    ceph_assert(false);
    return dummy_local_missing;
  }
  void add_local_next_event(const pg_log_entry_t& e) override
  {
  }
  const std::map<pg_shard_t, pg_missing_t>& get_shard_missing()
    const override
  {
    ceph_assert(false);
    static std::map<pg_shard_t, pg_missing_t> dummy;
    return dummy;
  }
  using PGBackend::Listener::get_shard_missing;
  const std::map<pg_shard_t, pg_info_t>& get_shard_info() const override
  {
    ceph_assert(false);
    static std::map<pg_shard_t, pg_info_t> dummy;
    return dummy;
  }
  using PGBackend::Listener::get_shard_info;

  const PGLog& get_log() const
  {
    ceph_assert(false);
    static PGLog l(cct);
    return l;
  }
  bool pgb_is_primary() const override {
    return is_primary();
  }
  const OSDMapRef& pgb_get_osdmap() const override final {
    return get_osdmap();
  }
  epoch_t pgb_get_osdmap_epoch() const override final {
    return get_osdmap_epoch();
  }
  const pg_info_t& get_info() const override {
    return info;
  }
  const pg_pool_t& get_pool() const override {
    return pool.info;
  }

  ObjectContextRef get_obc(
    const hobject_t& hoid,
    const std::map<std::string, ceph::buffer::list, std::less<>>& attrs) override
  {
    ceph_assert(false);
    return ObjectContextRef();
  }

  bool try_lock_for_read(
    const hobject_t& hoid,
    ObcLockManager& manager) override
  {
    ceph_assert(false);
  }

  void release_locks(ObcLockManager& manager) override
  {
    ceph_assert(false);
  }

  void op_applied(
    const eversion_t& applied_version) override
  {
  }

  bool should_send_op(
    pg_shard_t peer,
    const hobject_t& hoid) override
  {
    ceph_assert(false);
    return false;
  }

  bool pg_is_repair() const override {
    return is_repair();
  }

  bool pg_is_undersized() const override {
    return is_undersized();
  }

  void log_operation(
    std::vector<pg_log_entry_t>&& logv,
    const std::optional<pg_hit_set_history_t>& hset_history,
    const eversion_t& trim_to,
    const eversion_t& roll_forward_to,
    const eversion_t& min_last_complete_ondisk,
    bool transaction_applied,
    ObjectStore::Transaction& t,
    bool async = false) override
  {
    ceph_assert(false);
  }

  void pgb_set_object_snap_mapping(
    const hobject_t& soid,
    const std::set<snapid_t>& snaps,
    ObjectStore::Transaction* t) override {
    return update_object_snap_mapping(t, soid, snaps);
  }
  void pgb_clear_object_snap_mapping(
    const hobject_t& soid,
    ObjectStore::Transaction* t) override {
    return clear_object_snap_mapping(t, soid);
  }

   void update_peer_last_complete_ondisk(
    pg_shard_t fromosd,
    eversion_t lcod) override
  {
    ceph_assert(false);
  }

  void update_last_complete_ondisk(
    eversion_t lcod) override
  {
    ceph_assert(false);
  }

  void update_stats(
    const pg_stat_t& stat) override
  {
    ceph_assert(false);
  }

  void schedule_recovery_work(
    GenContext<ThreadPool::TPHandle&>* c) override
  {
    ceph_assert(false);
  }

  pg_shard_t whoami_shard() const override {
    return pg_whoami;
  }

  spg_t primary_spg_t() const override {
    return spg_t(info.pgid.pgid, get_primary().shard);
  }
  pg_shard_t primary_shard() const override {
    return get_primary();
  }
  uint64_t min_peer_features() const override {
    return recovery_state.get_min_peer_features();
  }
  uint64_t min_upacting_features() const override {
    return recovery_state.get_min_upacting_features();
  }
  hobject_t get_temp_recovery_object(const hobject_t& target,
    eversion_t version) override
  {
    ceph_assert(false);
    return hobject_t();
  }

  void send_message_osd_cluster(
    int peer, Message* m, epoch_t from_epoch) override {
    osd->send_message_osd_cluster(peer, m, from_epoch);
  }
  void send_message_osd_cluster(
    std::vector<std::pair<int, Message*>>& messages, epoch_t from_epoch) override {
    osd->send_message_osd_cluster(messages, from_epoch);
  }
  void send_message_osd_cluster(
    MessageRef m, Connection* con) override {
    osd->send_message_osd_cluster(m, con);
  }
  void send_message_osd_cluster(
    Message* m, const ConnectionRef& con) override {
    osd->send_message_osd_cluster(m, con);
  }
  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch) override
  {
    return osd->get_con_osd_cluster(peer, from_epoch);
  }
  entity_name_t get_cluster_msgr_name() override {
    return osd->get_cluster_msgr_name();
  }

  PerfCounters* get_logger() override { return osd->logger; }

  ceph_tid_t get_tid() override { return osd->get_tid(); }

  OstreamTemp clog_error() override { return osd->clog->error(); }
  OstreamTemp clog_warn() override { return osd->clog->warn(); }

  bool check_failsafe_full()
  {
    ceph_assert(false);
    return false;
  }

  void inc_osd_stat_repaired() override
  {
  }
  bool pg_is_remote_backfilling() override
  {
    return false;
  }
  void pg_add_local_num_bytes(int64_t num_bytes) override
  {
  }
  void pg_sub_local_num_bytes(int64_t num_bytes) override
  {
  }
  void pg_add_num_bytes(int64_t num_bytes) override
  {
  }
  void pg_sub_num_bytes(int64_t num_bytes) override
  {
  }
  bool maybe_preempt_replica_scrub(const hobject_t& oid) override
  {
    return false;
  }

  // PeeringState overrides
  void recheck_readable() final
  {
  }
  void check_recovery_sources(const OSDMapRef& newmap) final
  {
  }
  void dump_recovery_info(ceph::Formatter* f) const final
  {
  }

  void on_flushed() final
  {
  }
  void on_change(ObjectStore::Transaction&) final
  {
  }
  void on_activate_complete() final
  {
  }
  void on_removal(ObjectStore::Transaction&) final
  {
  }

  // PG overrides
  void split_colls(
    spg_t child,
    int split_bits,
    int seed,
    const pg_pool_t * pool,
    ObjectStore::Transaction & t) override {
    coll_t target = coll_t(child);
    create_pg_collection(t, child, split_bits);
    t.split_collection(
      coll,
      split_bits,
      seed,
      target);
    init_pg_ondisk(t, child, pool);
  }
  void plpg_on_role_change() final
  {
  }
  void plpg_on_pool_change() final
  {
  }
  bool start_recovery_ops(uint64_t, ThreadPool::TPHandle&, uint64_t*) final
  {
    return false;
  }
  void on_shutdown() final
  {
  }
  void do_request(OpRequestRef& op, ThreadPool::TPHandle& handle) final
  {
    int msg_type = op->get_req()->get_type();
    if (pgbackend->handle_message(op)) {
      return;
    }
    switch (msg_type) {
      case CEPH_MSG_OSD_OP:
	do_op(op);
	break;
    /*case CEPH_MSG_OSD_BACKOFF:
      handle_backoff(op);
      break;
      break;
    case MSG_OSD_PG_SCAN:
      do_scan(op, handle);
      break;

    case MSG_OSD_PG_BACKFILL:
      do_backfill(op);
      break;

    case MSG_OSD_PG_BACKFILL_REMOVE:
      do_backfill_remove(op);
      break;

    case MSG_OSD_SCRUB_RESERVE:
    {
      if (!m_scrubber) {
	osd->reply_op_error(op, -EAGAIN);
	return;
      }
      auto m = op->get_req<MOSDScrubReserve>();
      switch (m->type) {
      case MOSDScrubReserve::REQUEST:
	m_scrubber->handle_scrub_reserve_request(op);
	break;
      case MOSDScrubReserve::GRANT:
	m_scrubber->handle_scrub_reserve_grant(op, m->from);
	break;
      case MOSDScrubReserve::REJECT:
	m_scrubber->handle_scrub_reserve_reject(op, m->from);
	break;
      case MOSDScrubReserve::RELEASE:
	m_scrubber->handle_scrub_reserve_release(op);
	break;
      }
    }
    break;

    case MSG_OSD_REP_SCRUB:
      replica_scrub(op, handle);
      break;

    case MSG_OSD_REP_SCRUBMAP:
      do_replica_scrub_map(op);
      break;

    case MSG_OSD_PG_UPDATE_LOG_MISSING:
      do_update_log_missing(op);
      break;

    case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
      do_update_log_missing_reply(op);
      break;*/

    default:
      ceph_abort_msg("bad message type in do_request");
    }
  }
  void clear_cache() final
  {
  }
  int get_cache_obj_count() final
  {
    return 0;
  }
  void snap_trimmer(epoch_t) final
  {
  }
  void do_command(const std::string_view&,
    const cmdmap_t&,
    const ceph::buffer::v15_2_0::list&,
    std::function<void(int,
    const std::__cxx11::basic_string<char>&, ceph::buffer::v15_2_0::list&)>) final
  {
  }
  bool agent_work(int) final
  {
    ceph_assert(false);
    return false;
  }
  bool agent_work(int, int) final
  {
    ceph_assert(false);
    return false;
  }
  void agent_stop() final
  {
  }
  void agent_delay() final
  {
  }
  void agent_clear() final
  {
  }
  void agent_choose_mode_restart() final
  {
  }
  void check_local() final
  {
  }
  void _clear_recovery_state() final
  {
  }
  void _split_into(pg_t, PG*, unsigned int) final
  {
  }
  bool _range_available_for_scrub(const hobject_t&, const hobject_t&) final
  { 
    return false;
  }
  void kick_snap_trim() final
  {
  }
  void snap_trimmer_scrub_complete() final
  {
  }

  int start_cls_gather(OpContextBase *ctx,
                       const std::set<std::string> &src_objs,
                       const std::string& pool,
                       const char *cls,
                       const char *method, bufferlist& inbl) final
  {
    return -ENOTSUP;
  }
  int get_cls_gathered_data(OpContextBase *ctx,
                            std::map<std::string, bufferlist> *results) final
  {
    return -ENOTSUP;
  }

protected:
  int do_osd_ops(OpContextBase* ctx, std::vector<OSDOp>& ops) override;
  void do_pg_op(OpRequestRef op);

  int prepare_transaction(OpContext* ctx);
  void complete_read_ctx(int result, OpContext* ctx);


  void reply_ctx(OpContext *ctx, int r);
  void close_op_ctx(OpContext* ctx);
  void log_op_stats(const OpRequest& op,
    const uint64_t inb,
    const uint64_t outb);

  void write_update_size_and_usage(object_stat_sum_t& delta_stats,
                                   object_info_t& oi,
                                   uint64_t offset,
                                   uint64_t length, bool write_full);
  inline void truncate_update_size_and_usage(
    object_stat_sum_t& delta_stats,
    object_info_t& oi,
    uint64_t truncate_size);

  int getattrs_maybe_cache(
    //ObjectContextRef obc,
    const hobject_t& soid,
    std::map<std::string, bufferlist, std::less<>>* out);
  int do_read(OpContext* ctx, OSDOp& osd_op);
  int do_sparse_read(OpContext* ctx, OSDOp& osd_op);
  int do_writesame(OpContext* ctx, OSDOp& osd_op);

  int do_extent_cmp(OpContext* ctx, OSDOp& osd_op);
  int finish_extent_cmp(OSDOp& osd_op, const ceph::buffer::list& read_bl);

  int do_xattr_cmp_u64(int op, uint64_t v1, ceph::buffer::list& xattr);
  int do_xattr_cmp_str(int op, std::string& v1s, ceph::buffer::list& xattr);

  void populate_obc_watchers(ObjectContextRef obc);
  void check_blocklisted_obc_watchers(ObjectContextRef obc);
  void check_blocklisted_watchers() override;
  void get_watchers(std::list<obj_watch_item_t> *ls) override;
  void get_obc_watchers(ObjectContextRef obc, std::list<obj_watch_item_t> &pg_watchers);
  void handle_watch_timeout(WatchRef watch) override;

protected:

  ObjectContextRef create_object_context(const object_info_t& oi);
  ObjectContextRef get_object_context(
    const hobject_t& soid,
    bool can_create,
    const std::map<std::string, ceph::buffer::list, std::less<>> *attrs = 0
    );

/*  void context_registry_on_change();
  void object_context_destructor_callback(ObjectContext *obc);
  class C_PG_ObjectContext;
*/
  int find_object_context(const hobject_t& oid,
                          ObjectContextRef *pobc,
                          bool can_create,
                          bool map_snapid_to_clone=false,
                          hobject_t *missing_oid=NULL);

private:
  std::set<pg_shard_t> dummy_pg_shard_set;
  std::map<hobject_t, std::set<pg_shard_t>> dummy_loc_shards;
  pg_missing_tracker_t dummy_local_missing;

  boost::scoped_ptr<PGBackend> pgbackend;
  SharedLRU<hobject_t, ObjectContext> object_contexts;

  void maybe_create_new_object(OpContext *ctx, bool ignore_transaction = false);
  void do_op(OpRequestRef& op);
  void execute_ctx(OpContext* ctx);
  void finish_ctx(OpContext* ctx, int log_op_type, int result = 0);

  void complete_disconnect_watches(
    ObjectContextRef obc,
    const std::list<watch_disconnect_t> &to_disconnect);

  void do_osd_op_effects(OpContext *ctx, const ConnectionRef& conn);

  int _delete_oid(OpContext *ctx);

  int get_manifest_ref_count(ObjectContextRef obc,
                             std::string& fp_oid, OpRequestRef op) override;

/*  OpContextUPtr simple_opc_create(ObjectContextRef obc);
  void simple_opc_submit(OpContextUPtr ctx);*/
};

#endif
