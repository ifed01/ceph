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

#ifndef CEPH_OSD_DAOSTORE_H
#define CEPH_OSD_DAOSTORE_H

#include "acconfig.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
//#include <ratio>
#include <mutex>
#include <condition_variable>
#include <set>
#include <string>
#include <vector>
#include <map>

/*
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/circular_buffer.hpp>

#include "include/cpp-btree/btree_set.h"
*/
#include "include/ceph_assert.h"
/*#include "include/interval_set.h"
#include "include/unordered_map.h"
#include "include/mempool.h"
#include "include/hash.h"
#include "common/bloom_filter.hpp"*/
#include "common/Finisher.h"
/*#include "common/ceph_mutex.h"
#include "common/Throttle.h"*/
#include "include/unordered_set.h"
#include "common/perf_counters.h"
#include "os/ObjectStore.h"
#include "osd/osd_types.h"

#include "daostore_types.h"
#include "DaosInterface.h"

namespace daostore {

enum {
  l_daostore_first = 792430,
  l_daostore_submit_lat,
  l_daostore_commit_lat,
  l_daostore_onode_hits,
  l_daostore_onode_misses,
  l_daostore_last
};

//FIXME: do we really use it????
//#define META_POOL_ID ((uint64_t)-1ull)

class DaoStore : public ObjectStore,
		  public md_config_obs_t {
  // -----------------------------------------------------
  // types
public:
  // config observer
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string> &changed) override;

  struct Collection;
  struct CollectionRef : public boost::intrusive_ptr<Collection> {
    friend bool operator<(const CollectionRef& a, const CollectionRef& b) {
      return a->cid < b->cid;
    }
    friend bool operator<(const CollectionRef& a, const coll_t& cid_b) {
      return a->cid < cid_b;
    }
    friend bool operator<(const coll_t& cid_a, const CollectionRef& b) {
      return cid_a < b->cid;
    }
    CollectionRef() : boost::intrusive_ptr<Collection>(nullptr) {}
    CollectionRef(Collection* c) : boost::intrusive_ptr<Collection>(c) {}
  };
  class OpSequencer;
  struct OpSequencerRef : public boost::intrusive_ptr<OpSequencer> {
    friend bool operator<(const OpSequencerRef& a, const OpSequencerRef& b) {
      return a->get_cid() < b->get_cid();
    }
    friend bool operator<(const OpSequencerRef& a, const coll_t& cid_b) {
      return a->get_cid() < cid_b;
    }
    friend bool operator<(const coll_t& cid_a, const OpSequencerRef& b) {
      return cid_a < b->get_cid();
    }
    OpSequencerRef() : boost::intrusive_ptr<OpSequencer>() {}
    OpSequencerRef(OpSequencer* op) : boost::intrusive_ptr<OpSequencer>(op) {}
    OpSequencerRef(const OpSequencerRef& op) : boost::intrusive_ptr<OpSequencer>(op) {}
    bool operator==(const OpSequencerRef& other) const {
      return get() == other.get();
    }
  };

  //struct OnodeSpace;
  /// an in-memory object
  struct Onode {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref;  ///< reference count
    Collection *c;
    ghobject_t oid;

    /// key where we are stored
    //FIXME: change mempool
    mempool::bluestore_cache_meta::string key;

    boost::intrusive::list_member_hook<> lru_item;

    bool exists = false;      ///< true if object logically exists
/*    bool cached;            ///< Onode is logically in the cache
                              /// (it can be pinned and hence physically out
                              /// of it at the moment though)*/

    // track txc's that have not been committed 
    std::atomic<int> flushing_count = {0};
    /*std::atomic<int> waiting_count = {0};
    /// protect flush_txns
    ceph::mutex flush_lock = ceph::make_mutex("BlueStore::Onode::flush_lock");
    ceph::condition_variable flush_cond;   ///< wait here for uncommitted txns
  */
    Onode(Collection *c, const ghobject_t& o, std::string_view k)
      : nref(0),
	c(c),
	oid(o),
	key(k)
	/*exists(false),
        cached(false),*/ {
    }

    static Onode* decode(
      CollectionRef c,
      const ghobject_t& oid,
      const std::string& key,
      const ceph::buffer::list& v);

    void dump(ceph::Formatter* f) const;

    void flush();
    void get();
    void put();

/*    inline bool put_cache() {
      ceph_assert(!cached);
      cached = true;
      return !pinned;
    }
    inline bool pop_cache() {
      ceph_assert(cached);
      cached = false;
      return !pinned;
    }*/

/*    const std::string& get_omap_prefix();
    void get_omap_header(std::string *out);
    void get_omap_key(const std::string& key, std::string *out);
    void rewrite_omap_key(const std::string& old, std::string *out);
    void get_omap_tail(std::string *out);
    void decode_omap_key(const std::string& key, std::string *user_key);*/

  };
  typedef boost::intrusive_ptr<Onode> OnodeRef;

  /// A generic Cache Shard
/*  struct CacheShard {
    CephContext *cct;
    PerfCounters *logger;

    /// protect lru and other structures
    ceph::recursive_mutex lock = {
      ceph::make_recursive_mutex("BlueStore::CacheShard::lock") };

    std::atomic<uint64_t> max = {0};
    std::atomic<uint64_t> num = {0};

    CacheShard(CephContext* cct) : cct(cct), logger(nullptr) {}
    virtual ~CacheShard() {}

    void set_max(uint64_t max_) {
      max = max_;
    }

    uint64_t _get_num() {
      return num;
    }

    virtual void _trim_to(uint64_t new_size) = 0;
    void _trim() {
      if (cct->_conf->objectstore_blackhole) {
	// do not trim if we are throwing away IOs a layer down
	return;
      }
      _trim_to(max);
    }

    void trim() {
      std::lock_guard l(lock);
      _trim();    
    }
    void flush() {
      std::lock_guard l(lock);
      // we should not be shutting down after the blackhole is enabled
      assert(!cct->_conf->objectstore_blackhole);
      _trim_to(0);
    }

#ifdef DEBUG_CACHE
    virtual void _audit(const char *s) = 0;
#else
    void _audit(const char *s) {}
#endif
  };

  /// A Generic onode Cache Shard
  struct OnodeCacheShard : public CacheShard {
    std::atomic<uint64_t> num_pinned = {0};

    std::array<std::pair<ghobject_t, ceph::mono_clock::time_point>, 64> dumped_onodes;

    virtual void _pin(Onode* o) = 0;
    virtual void _unpin(Onode* o) = 0;

  public:
    OnodeCacheShard(CephContext* cct) : CacheShard(cct) {}
    static OnodeCacheShard *create(CephContext* cct, std::string type,
                                   PerfCounters *logger);
    virtual void _add(Onode* o, int level) = 0;
    virtual void _rm(Onode* o) = 0;
    virtual void _unpin_and_rm(Onode* o) = 0;

    virtual void move_pinned(OnodeCacheShard *to, Onode *o) = 0;
    virtual void add_stats(uint64_t *onodes, uint64_t *pinned_onodes) = 0;
    bool empty() {
      return _get_num() == 0;
    }
  };*/

/*  struct OnodeSpace {
    OnodeCacheShard *cache;

  private:
    /// forward lookups
    mempool::bluestore_cache_meta::unordered_map<ghobject_t,OnodeRef> onode_map;

    friend struct Collection; // for split_cache()
    friend struct Onode; // for put()
    friend struct LruOnodeCacheShard;
    void _remove(const ghobject_t& oid);
  public:
    OnodeSpace(OnodeCacheShard *c) : cache(c) {}
    ~OnodeSpace() {
      clear();
    }

    OnodeRef add(const ghobject_t& oid, OnodeRef& o);
    OnodeRef lookup(const ghobject_t& o);
    void rename(OnodeRef& o, const ghobject_t& old_oid,
		const ghobject_t& new_oid,
		const mempool::bluestore_cache_meta::string& new_okey);
    void clear();
    bool empty();

    template <int LogLevelV>
    void dump(CephContext *cct);

    /// return true if f true for any item
    bool map_any(std::function<bool(Onode*)> f);
  };
  */

  struct Collection : public CollectionImpl {
  private:
    DaoStore *store = nullptr;
    OpSequencerRef osr;
    uint32_t bits;   ///< how many bits of coll pgid are significant
    ceph::shared_mutex lock =
      ceph::make_shared_mutex("DaoStore::Collection::lock", true, false);

    bool _exists = true;

    //pool options
    pool_opts_t pool_opts;
    ContextQueue *commit_queue = nullptr;

    /// forward lookups
    //FIXME: change mempool
    mempool::bluestore_cache_meta::unordered_map<ghobject_t, OnodeRef> onode_map;

    OnodeRef _lookup(const ghobject_t& oid);

/*    OnodeCacheShard* get_onode_cache() const {
      return onode_map.cache;
    }*/

/*    bool contains(const ghobject_t& oid) {
      if (cid.is_meta())
	return oid.hobj.pool == -1;
      spg_t spgid;
      if (cid.is_pg(&spgid))
	return
	  spgid.pgid.contains(cnode.bits, oid) &&
	  oid.shard_id == spgid.shard;
      return false;
    }*/

    //void split_cache(Collection *dest);

    bool flush_commit(Context *c) override;
    void flush() override;
public:
    Collection(DaoStore *store_, daostore_cnode_t cnode) :
      CollectionImpl(store_->cct, cnode.cid),
		      store(store_),
		      bits(cnode.bits)
    {
    }
    bool exists() const {
      return _exists;
    }
    void set_exists(bool b) {
      _exists = b;
    }

    OnodeRef get_onode(const ghobject_t& oid, bool create, bool is_createop = false);

    /// return true if f true for any item
    bool map_any(std::function<bool(Onode*)> f);

    int64_t pool() const {
      return cid.pool();
    }
    daostore_cnode_t get_cnode() const {
      return daostore_cnode_t(cid, bits);
    }

    OpSequencerRef get_osr() {
      return osr;
    }
    void assign_osr(OpSequencerRef _osr) {
      osr = _osr;
    }

    void set_bits(uint32_t _bits) {
      bits = _bits;
    }

    void notify_on_commit(std::list<Context*>& oncommits) {
      ceph_assert(commit_queue);
      commit_queue->queue(oncommits);
    }

    void clear_onodes();

    int collection_list(DaosContainer& daos,
      const ghobject_t& start,
      std::function<bool (const ghobject_t &e)> fn);
  };

/*  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    CollectionRef c;
    OnodeRef o;
    KeyValueDB::Iterator it;
    std::string head, tail;

    std::string _stringify() const;

  public:
    OmapIteratorImpl(CollectionRef c, OnodeRef o, KeyValueDB::Iterator it);
    int seek_to_first() override;
    int upper_bound(const std::string &after) override;
    int lower_bound(const std::string &to) override;
    bool valid() override;
    int next() override;
    std::string key() override;
    ceph::buffer::list value() override;
    std::string tail_key() override {
      return tail;
    }

    int status() override {
      return 0;
    }
  };*/

  struct TransContext {
    enum state_t {
      STATE_PREPARE,
      STATE_DAOS_SUBMITTED,  // submitted to DAOS
      //STATE_DAOS_DONE,
      //STATE_FINISHING,
      STATE_DONE,
    };
  private:
    CollectionRef cref;

    std::list<Context*> oncommits;  ///< more commit completions*/

    ceph::mono_clock::time_point start;
    //ceph::mono_clock::time_point last_stamp;

    state_t state = STATE_PREPARE;

    friend class OpSequencer;
    boost::intrusive::list_member_hook<> sequencer_item;
    uint64_t seq = 0;

  public:
    MEMPOOL_CLASS_HELPERS();

    const char *get_state_name() {
      switch (state) {
      case STATE_PREPARE: return "prepare";
      case STATE_DAOS_SUBMITTED: return "daos_submitted";
      //case STATE_DAOS_DONE: return "daos_done";
      //case STATE_FINISHING: return "finishing";
      case STATE_DONE: return "done";
      }
      return "???";
    }

    inline void set_state(state_t s) {
       state = s;
    }
    inline state_t get_state() {
      return state;
    }
    uint64_t get_seq() const {
      return seq;
    }
    auto get_start() const {
      return start;
    }
    /*CollectionRef get_coll() const {
      return cref;
    }*/
    OpSequencerRef get_osr() const{
      return cref->get_osr();
    }
    void notify_on_commit() {
      cref->notify_on_commit(oncommits);
    }

/*    uint64_t bytes = 0, ios = 0, cost = 0;

    std::set<OnodeRef> onodes;     ///< these need to be updated/written
    std::set<OnodeRef> modified_objects;  ///< objects we modified (and need a ref)*/

    DaosTransaction t;
    std::list<CollectionRef> removed_collections; ///< colls we removed

/*    boost::intrusive::list_member_hook<> deferred_queue_item;
    bluestore_deferred_transaction_t *deferred_txn = nullptr; ///< if any

    interval_set<uint64_t> allocated, released;
    volatile_statfs statfs_delta;	   ///< overall store statistics delta*/
    //uint64_t osd_pool_id = META_POOL_ID;    ///< osd pool id we're operating on

    /*IOContext ioc;
    bool had_ios = false;  ///< true if we submitted IOs before our kv txn
*/
//    uint64_t last_nid = 0;     ///< if non-zero, highest new nid we allocated

    explicit TransContext(CephContext* cct, CollectionRef _cref,
			  std::list<Context*> *on_commits)
      : cref(_cref),
	start(ceph::mono_clock::now()) {
      ceph_assert(cref.get());
      //last_stamp = start;
      if (on_commits) {
	oncommits.swap(*on_commits);
      }
      cref->get_osr()->queue_new(this);
    }
    ~TransContext() {
    }

/*    void write_onode(OnodeRef &o) {
      onodes.insert(o);
    }

    /// note we logically modified object (when onode itself is unmodified)
    void note_modified_object(OnodeRef &o) {
      // onode itself isn't written, though
      modified_objects.insert(o);
    }
    void note_removed_object(OnodeRef& o) {
      modified_objects.insert(o);
      onodes.erase(o);
    }

    void aio_finish(BlueStore *store) override {
      store->txc_aio_finish(this);
    }*/
  };

  class OpSequencer : public RefCountedObject {
    DaoStore* store;
    const uint32_t sequencer_id;
    coll_t cid;

  public:
    ceph::mutex qlock = ceph::make_mutex("DaoStore::OpSequencer::qlock");
    ceph::condition_variable qcond;
    typedef boost::intrusive::list<
      TransContext,
      boost::intrusive::member_hook<
        TransContext,
	boost::intrusive::list_member_hook<>,
	&TransContext::sequencer_item> > q_list_t;
    q_list_t q;  ///< transactions

    uint64_t last_seq = 0;

  /*  std::atomic_int txc_with_unstable_io = {0};  ///< num txcs with unstable io

    std::atomic_int kv_committing_serially = {0};

    std::atomic_int kv_submitted_waiters = {0};
*/
    std::atomic_bool zombie = {false};    ///< in zombie_osr std::set (collection going away)

    uint32_t get_sequencer_id() const {
      return sequencer_id;
    }
    coll_t get_cid() const {
      return cid;
    }

    void queue_new(TransContext *txc) {
      std::lock_guard l(qlock);
      txc->seq = ++last_seq;
      q.push_back(*txc);
    }

    void drain() {
      std::unique_lock l(qlock);
      while (!q.empty())
	qcond.wait(l);
    }

/*    void drain_preceding(TransContext *txc) {
      std::unique_lock l(qlock);
      while (&q.front() != txc)
	qcond.wait(l);
    }

    bool _is_all_kv_submitted() {
      // caller must hold qlock & q.empty() must not empty
      ceph_assert(!q.empty());
      TransContext *txc = &q.back();
      if (txc->get_state() >= TransContext::STATE_KV_SUBMITTED) {
	return true;
      }
      return false;
    }

    void flush() {
      std::unique_lock l(qlock);
      while (true) {
	// std::set flag before the check because the condition
	// may become true outside qlock, and we need to make
	// sure those threads see waiters and signal qcond.
	++kv_submitted_waiters;
	if (q.empty() || _is_all_kv_submitted()) {
	  --kv_submitted_waiters;
	  return;
	}
	qcond.wait(l);
	--kv_submitted_waiters;
      }
    }*/

    void flush_all_but_last() {
      /*std::unique_lock l(qlock);
      assert (q.size() >= 1);
      while (true) {
	// std::set flag before the check because the condition
	// may become true outside qlock, and we need to make
	// sure those threads see waiters and signal qcond.
	++kv_submitted_waiters;
	if (q.size() <= 1) {
	  --kv_submitted_waiters;
	  return;
	} else {
	  auto it = q.rbegin();
	  it++;
	  if (it->get_state() >= TransContext::STATE_KV_SUBMITTED) {
	    --kv_submitted_waiters;
	    return;
          }
	}
	qcond.wait(l);
	--kv_submitted_waiters;
      }*/
    }

    /*bool flush_commit(Context *c) {
      std::lock_guard l(qlock);
      if (q.empty()) {
	return true;
      }
      TransContext *txc = &q.back();
      if (txc->get_state() >= TransContext::STATE_KV_DONE) {
	return true;
      }
      txc->oncommits.push_back(c);
      return false;
    }*/
    OpSequencer(DaoStore *_store, uint32_t _sequencer_id, const coll_t& c)
      : RefCountedObject(_store->cct),
	store(_store), sequencer_id(_sequencer_id), cid(c) {
    }
  private:
    ~OpSequencer() {
      //ceph_assert(q.empty());
    }
  };

/*  struct KVFinalizeThread : public Thread {
    BlueStore *store;
    explicit KVFinalizeThread(BlueStore *s) : store(s) {}
    void *entry() override {
      store->_kv_finalize_thread();
      return NULL;
    }
  };*/

  // --------------------------------------------------------
  // members
private:

  uuid_d fsid;
  int path_fd = -1;  ///< open handle to $path
  int fsid_fd = -1;  ///< open handle (locked) to $path/fsid
  bool mounted = false;
  bool read_only = false;
  //std::string whoami;
  DaosContainer daos_container;

  ceph::shared_mutex coll_lock = ceph::make_shared_mutex("DaoStore::coll_lock");  ///< rwlock to protect coll_map
  //FIXME: adjust mempool
  mempool::bluestore_cache_other::set<CollectionRef, std::less<>> coll_set;
  //  bool collections_had_errors = false;
  std::set<CollectionRef, std::less<>> new_coll_set;

//  std::vector<OnodeCacheShard*> onode_cache_shards;
//  std::vector<BufferCacheShard*> buffer_cache_shards;

  /// protect zombie_osr_set
  ceph::mutex zombie_osr_lock = ceph::make_mutex("BlueStore::zombie_osr_lock");
  uint32_t next_sequencer_id = 0;
  std::set<OpSequencerRef, std::less<>> zombie_osr_set; ///< set of deleted collections

  /*
  std::atomic<uint64_t> nid_last = {0};
  std::atomic<uint64_t> nid_max = {0};
  std::atomic<uint64_t> blobid_last = {0};
  std::atomic<uint64_t> blobid_max = {0};*/

/*  KVFinalizeThread kv_finalize_thread;
  ceph::mutex kv_finalize_lock = ceph::make_mutex("DaoStore::kv_finalize_lock");
  ceph::condition_variable kv_finalize_cond;
  std::deque<TransContext*> kv_committing_to_finalize;   ///< pending finalization
  bool kv_finalize_in_progress = false;*/

  PerfCounters *logger = nullptr;

//  std::list<CollectionRef> removed_collections;

/*  // cache trim control
  uint64_t cache_size = 0;       ///< total cache size
  double cache_meta_ratio = 0;   ///< cache ratio dedicated to metadata
  double cache_kv_ratio = 0;     ///< cache ratio dedicated to kv (e.g., rocksdb)
  double cache_kv_onode_ratio = 0; ///< cache ratio dedicated to kv onodes (e.g., rocksdb onode CF)
  double cache_data_ratio = 0;   ///< cache ratio dedicated to object data
  bool cache_autotune = false;   ///< cache autotune setting
  double cache_autotune_interval = 0; ///< time to wait between cache rebalancing
  uint64_t osd_memory_target = 0;   ///< OSD memory target when autotuning cache
  uint64_t osd_memory_base = 0;     ///< OSD base memory when autotuning cache
  double osd_memory_expected_fragmentation = 0; ///< expected memory fragmentation
  uint64_t osd_memory_cache_min = 0; ///< Min memory to assign when autotuning cache
  double osd_memory_cache_resize_interval = 0; ///< Time to wait between cache resizing 
  double max_defer_interval = 0; ///< Time to wait between last deferred submit
  std::atomic<uint32_t> config_changed = {0}; ///< Counter to determine if there is a configuration change.
*/
/*  typedef std::map<uint64_t, volatile_statfs> osd_pools_map;

  ceph::mutex vstatfs_lock = ceph::make_mutex("BlueStore::vstatfs_lock");
  volatile_statfs vstatfs;
  osd_pools_map osd_pools; // protected by vstatfs_lock as well
*/

/*  struct MempoolThread : public Thread {
  public:
    BlueStore *store;

    ceph::condition_variable cond;
    ceph::mutex lock = ceph::make_mutex("BlueStore::MempoolThread::lock");
    bool stop = false;
    std::shared_ptr<PriorityCache::PriCache> binned_kv_cache = nullptr;
    std::shared_ptr<PriorityCache::PriCache> binned_kv_onode_cache = nullptr;
    std::shared_ptr<PriorityCache::Manager> pcm = nullptr;

    struct MempoolCache : public PriorityCache::PriCache {
      BlueStore *store;
      int64_t cache_bytes[PriorityCache::Priority::LAST+1] = {0};
      int64_t committed_bytes = 0;
      double cache_ratio = 0;

      MempoolCache(BlueStore *s) : store(s) {};

      virtual uint64_t _get_used_bytes() const = 0;

      virtual int64_t request_cache_bytes(
          PriorityCache::Priority pri, uint64_t total_cache) const {
        int64_t assigned = get_cache_bytes(pri);

        switch (pri) {
        // All cache items are currently shoved into the PRI1 priority 
        case PriorityCache::Priority::PRI1:
          {
            int64_t request = _get_used_bytes();
            return(request > assigned) ? request - assigned : 0;
          }
        default:
          break;
        }
        return -EOPNOTSUPP;
      }
 
      virtual int64_t get_cache_bytes(PriorityCache::Priority pri) const {
        return cache_bytes[pri];
      }
      virtual int64_t get_cache_bytes() const { 
        int64_t total = 0;

        for (int i = 0; i < PriorityCache::Priority::LAST + 1; i++) {
          PriorityCache::Priority pri = static_cast<PriorityCache::Priority>(i);
          total += get_cache_bytes(pri);
        }
        return total;
      }
      virtual void set_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
        cache_bytes[pri] = bytes;
      }
      virtual void add_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
        cache_bytes[pri] += bytes;
      }
      virtual int64_t commit_cache_size(uint64_t total_cache) {
        committed_bytes = PriorityCache::get_chunk(
            get_cache_bytes(), total_cache);
        return committed_bytes;
      }
      virtual int64_t get_committed_size() const {
        return committed_bytes;
      }
      virtual double get_cache_ratio() const {
        return cache_ratio;
      }
      virtual void set_cache_ratio(double ratio) {
        cache_ratio = ratio;
      }
      virtual std::string get_cache_name() const = 0;
    };

    struct MetaCache : public MempoolCache {
      MetaCache(BlueStore *s) : MempoolCache(s) {};

      virtual uint64_t _get_used_bytes() const {
        return mempool::bluestore_Buffer::allocated_bytes() +
          mempool::bluestore_Blob::allocated_bytes() +
          mempool::bluestore_Extent::allocated_bytes() +
          mempool::bluestore_cache_meta::allocated_bytes() +
          mempool::bluestore_cache_other::allocated_bytes() +
	   mempool::bluestore_cache_onode::allocated_bytes() +
          mempool::bluestore_SharedBlob::allocated_bytes() +
          mempool::bluestore_inline_bl::allocated_bytes();
      }

      virtual std::string get_cache_name() const {
        return "BlueStore Meta Cache";
      }

      uint64_t _get_num_onodes() const {
        uint64_t onode_num =
            mempool::bluestore_cache_onode::allocated_items();
        return (2 > onode_num) ? 2 : onode_num;
      }

      double get_bytes_per_onode() const {
        return (double)_get_used_bytes() / (double)_get_num_onodes();
      }
    };
    std::shared_ptr<MetaCache> meta_cache;

    struct DataCache : public MempoolCache {
      DataCache(BlueStore *s) : MempoolCache(s) {};

      virtual uint64_t _get_used_bytes() const {
        uint64_t bytes = 0;
        for (auto i : store->buffer_cache_shards) {
          bytes += i->_get_bytes();
        }
        return bytes; 
      }
      virtual std::string get_cache_name() const {
        return "BlueStore Data Cache";
      }
    };
    std::shared_ptr<DataCache> data_cache;

  public:
    explicit MempoolThread(BlueStore *s)
      : store(s),
        meta_cache(new MetaCache(s)),
        data_cache(new DataCache(s)) {}

    void *entry() override;
    void init() {
      ceph_assert(stop == false);
      create("bstore_mempool");
    }
    void shutdown() {
      lock.lock();
      stop = true;
      cond.notify_all();
      lock.unlock();
      join();
    }

  private:
    void _adjust_cache_settings();
    void _update_cache_settings();
    void _resize_shards(bool interval_stats);
  } mempool_thread;
*/
  // --------------------------------------------------------
  // private methods

  void _init_logger();
  void _shutdown_logger();
  int _reload_logger();

  int _open_path();
  void _close_path();
  int _open_fsid(bool create);
  int _lock_fsid();
  int _read_fsid(uuid_d *f);
  int _write_fsid();
  void _close_fsid();
  //FIXME
  int _open_container_and_around(bool read_only);
  void _close_container_and_around();
  int _open_container(bool create) { return 0; }
  void _close_container() { return; }

  int _open_collections();
  int _close_collections();

  //  void _set_finisher_num();
//  void _update_osd_memory_options();

  int _write_daemon_label(CephContext* cct,
			  daostore_bdev_label_t label);
  int _read_daemon_label(CephContext* cct,
			 daostore_bdev_label_t* label);
  /*int _check_or_set_bdev_label(std::string path, uint64_t size, std::string desc,
    bool create);
  int _set_bdev_label_size(const string& path, uint64_t size);*/

private:
  int _open_super_meta();

  CollectionRef _get_collection(const coll_t& cid);
  void _queue_reap_collection(CollectionRef& c);
  void _reap_collections();
  void _update_cache_logger();

  //void _assign_nid(TransContext *txc, OnodeRef o);

  template <int LogLevelV>
  friend void _dump_onode(CephContext *cct, const Onode& o);
  template <int LogLevelV>
  friend void _dump_transaction(CephContext *cct, Transaction *t);

  TransContext *_txc_create(CollectionRef c,
			    std::list<Context*> *on_commits);
//  void _txc_update_store_statfs(TransContext *txc);
  void _txc_add_transaction(TransContext *txc, Transaction *t);
/*  void _txc_calc_cost(TransContext *txc);
  void _txc_write_nodes(TransContext *txc, KeyValueDB::Transaction t);*/
  void _txc_state_proc(TransContext *txc);
private:
  /*void _txc_finish_io(TransContext *txc);
  void _txc_finalize_kv(TransContext *txc, KeyValueDB::Transaction t);
  void _txc_apply_kv(TransContext *txc, bool sync_submit_transaction);
  void _txc_committed_kv(TransContext *txc);
  void _txc_release_alloc(TransContext *txc);
  */
  void _txc_finish(TransContext* txc);
  void _osr_attach(CollectionRef c);
  void _osr_register_zombie(OpSequencerRef osr);
  /*void _osr_drain(OpSequencer *osr);
  void _osr_drain_preceding(TransContext *txc);*/
  void _osr_drain_all();

  /*void _kv_start();
  void _kv_stop();
  void _kv_sync_thread();*/
  void _kv_finalize_thread();

public:
/*  using  per_pool_statfs =
    mempool::bluestore_fsck::map<uint64_t, store_statfs_t>;
*/
private:

/*  void _buffer_cache_write(
    TransContext *txc,
    BlobRef b,
    uint64_t offset,
    ceph::buffer::list& bl,
    unsigned flags) {
    b->shared_blob->bc.write(b->shared_blob->get_cache(), txc->seq, offset, bl,
			     flags);
    txc->shared_blobs_written.insert(b->shared_blob);
  }*/

  int _collection_list(
    Collection *c, const ghobject_t& start, const ghobject_t& end,
    int max, bool legacy, std::vector<ghobject_t> *ls, ghobject_t *next);

/*  template <typename T, typename F>
  T select_option(const std::string& opt_name, T val1, F f) {
    //NB: opt_name reserved for future use
    boost::optional<T> val2 = f();
    if (val2) {
      return *val2;
    }
    return val1;
  }*/

  void _apply_padding(uint64_t head_pad,
		      uint64_t tail_pad,
		      ceph::buffer::list& padded);

  void _record_onode(OnodeRef &o, KeyValueDB::Transaction &txn);

  // -- ondisk version ---
/*public:
  const int32_t latest_ondisk_format = 4;        ///< our version
  const int32_t min_readable_ondisk_format = 1;  ///< what we can read
  const int32_t min_compat_ondisk_format = 3;    ///< who can read us

private:
  int32_t ondisk_format = 0;  ///< value detected on mount

  int _upgrade_super();  ///< upgrade (called during open_super)
  uint64_t _get_ondisk_reserved() const;
  void _prepare_ondisk_format_super(KeyValueDB::Transaction& t);
*/
  // --- public interface ---
public:
  DaoStore(CephContext *cct, const std::string& path);
  DaoStore(CephContext *cct, const std::string& path, uint64_t min_alloc_size); // Ctor for UT only
  ~DaoStore() override;

  std::string get_type() override {
    return "daostore";
  }

  bool needs_journal() override { return false; };
  bool wants_journal() override { return false; };
  bool allows_journal() override { return false; };

  uint64_t get_min_alloc_size() const override {
    return 1;
  }

  int get_devices(std::set<std::string> *ls) override;

  bool is_rotational() override {
    return false;
  }
  bool is_journal_rotational() override {
    return false;
  }
  std::string get_default_device_class() override {
    return "ssd";
  }

  int get_numa_node(
    int *numa_node,
    std::set<int> *nodes,
    std::set<std::string> *failed) override;

  static int get_block_device_fsid(CephContext* cct, const std::string& path,
				   uuid_d *fsid);

  bool test_mount_in_use() override;

public:
  int mount() override;
  int umount() override;

  int write_meta(const std::string& key, const std::string& value) override;
  int read_meta(const std::string& key, std::string *value) override;
/*
  // open in read-only and limited mode
  int cold_open();
  int cold_close();
*/

/*  void set_cache_shards(unsigned num) override;
  void dump_cache_stats(ceph::Formatter *f) override {
    int onode_count = 0, buffers_bytes = 0;
    for (auto i: onode_cache_shards) {
      onode_count += i->_get_num();
    }
    for (auto i: buffer_cache_shards) {
      buffers_bytes += i->_get_bytes();
    }
    f->dump_int("bluestore_onode", onode_count);
    f->dump_int("bluestore_buffers", buffers_bytes);
  }
  void dump_cache_stats(std::ostream& ss) override {
    int onode_count = 0, buffers_bytes = 0;
    for (auto i: onode_cache_shards) {
      onode_count += i->_get_num();
    }
    for (auto i: buffer_cache_shards) {
      buffers_bytes += i->_get_bytes();
    }
    ss << "bluestore_onode: " << onode_count;
    ss << "bluestore_buffers: " << buffers_bytes;
  }*/

  int validate_hobject_key(const hobject_t &obj) const override {
    return 0;
  }
  unsigned get_max_attr_name_length() override {
    return 256;  // arbitrary; there is no real limit internally
  }

  int mkfs() override;
  int mkjournal() override {
    return 0;
  }

  void get_db_statistics(ceph::Formatter *f) override;
  void generate_db_histogram(ceph::Formatter *f) override;
  void _shutdown_cache();
  int flush_cache(std::ostream *os = NULL) override;
  void dump_perf_counters(ceph::Formatter *f) override {
    f->open_object_section("perf_counters");
    logger->dump_formatted(f, false);
    f->close_section();
  }

public:
  int statfs(struct store_statfs_t *buf,
             osd_alert_list_t* alerts = nullptr) override;
  int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
		  bool *per_pool_omap) override;

  void collect_metadata(std::map<std::string,std::string> *pm) override;

  bool exists(CollectionHandle &c, const ghobject_t& oid) override;
  int set_collection_opts(
    CollectionHandle& c,
    const pool_opts_t& opts) override;
  int stat(
    CollectionHandle &c,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false) override;
  int read(
    CollectionHandle &c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    ceph::buffer::list& bl,
    uint32_t op_flags = 0) override;

private:
  struct BSPerfTracker {
    PerfCounters::avg_tracker<uint64_t> os_commit_latency_ns;
    PerfCounters::avg_tracker<uint64_t> os_apply_latency_ns;

    objectstore_perf_stat_t get_cur_stats() const {
      objectstore_perf_stat_t ret;
      ret.os_commit_latency_ns = os_commit_latency_ns.current_avg();
      ret.os_apply_latency_ns = os_apply_latency_ns.current_avg();
      return ret;
    }

    void update_from_perfcounters(PerfCounters& logger) {
      auto l = logger.get_tavg_ns(l_daostore_commit_lat);
      os_commit_latency_ns.consume_next(l);
      os_apply_latency_ns.consume_next(l);
    }
  } perf_tracker;

public:
  int fiemap(CollectionHandle &c, const ghobject_t& oid,
	     uint64_t offset, size_t len, ceph::buffer::list& bl) override;
  int fiemap(CollectionHandle &c, const ghobject_t& oid,
	     uint64_t offset, size_t len, std::map<uint64_t, uint64_t>& destmap) override;

  int readv(
    CollectionHandle &c_,
    const ghobject_t& oid,
    interval_set<uint64_t>& m,
    ceph::buffer::list& bl,
    uint32_t op_flags) override;

  int dump_onode(CollectionHandle &c, const ghobject_t& oid,
    const std::string& section_name, ceph::Formatter *f) override;

  int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name,
	      ceph::buffer::ptr& value) override;

  int getattrs(CollectionHandle &c, const ghobject_t& oid,
	       std::map<std::string,ceph::buffer::ptr, std::less<>>& aset) override;

  int list_collections(std::vector<coll_t>& ls) override;

  CollectionHandle open_collection(const coll_t &c) override;
  CollectionHandle create_new_collection(const coll_t& cid) override;
  void set_collection_commit_queue(const coll_t& cid,
				   ContextQueue *commit_queue) override;

  bool collection_exists(const coll_t& c) override;
  int collection_empty(CollectionHandle& c, bool *empty) override;
  int collection_bits(CollectionHandle& c) override;

  int collection_list(CollectionHandle &c,
		      const ghobject_t& start,
		      const ghobject_t& end,
		      int max,
		      std::vector<ghobject_t> *ls, ghobject_t *next) override;

  int collection_list_legacy(CollectionHandle &c,
                             const ghobject_t& start,
                             const ghobject_t& end,
                             int max,
                             std::vector<ghobject_t> *ls,
                             ghobject_t *next) override;

  int omap_get(
    CollectionHandle &c,     ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    std::map<std::string, ceph::buffer::list> *out /// < [out] Key to value map
    ) override;
  int _omap_get(
    Collection *c,     ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    std::map<std::string, ceph::buffer::list> *out /// < [out] Key to value map
    );
  int _onode_omap_get(
    const OnodeRef &o,           ///< [in] Object containing omap
    ceph::buffer::list *header,          ///< [out] omap header
    std::map<std::string, ceph::buffer::list> *out /// < [out] Key to value map
  );


  /// Get omap header
  int omap_get_header(
    CollectionHandle &c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) override;

  /// Get keys defined on oid
  int omap_get_keys(
    CollectionHandle &c,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    std::set<std::string> *keys      ///< [out] Keys defined on oid
    ) override;

  /// Get key values
  int omap_get_values(
    CollectionHandle &c,         ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const std::set<std::string> &keys,     ///< [in] Keys to get
    std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
    ) override;

#ifdef WITH_SEASTAR
  int omap_get_values(
    CollectionHandle &c,         ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const std::optional<std::string> &start_after,     ///< [in] Keys to get
    std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
    ) override;
#endif

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    CollectionHandle &c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const std::set<std::string> &keys, ///< [in] Keys to check
    std::set<std::string> *out         ///< [out] Subset of keys defined on oid
    ) override;

  ObjectMap::ObjectMapIterator get_omap_iterator(
    CollectionHandle &c,   ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    ) override;

  void set_fsid(uuid_d u) override {
    fsid = u;
  }
  uuid_d get_fsid() override {
    return fsid;
  }

  uint64_t estimate_objects_overhead(uint64_t num_objects) override {
    return 0; //num_objects * 300; //assuming per-object overhead is 300 bytes
  }

  objectstore_perf_stat_t get_cur_stats() override {
    perf_tracker.update_from_perfcounters(*logger);
    return perf_tracker.get_cur_stats();
  }
  const PerfCounters* get_perf_counters() const override {
    return logger;
  }
  PerfCounters* get_perf_counters() {
    return logger;
  }

  int queue_transactions(
    CollectionHandle& ch,
    std::vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL) override;

  // error injection
  void inject_data_error(const ghobject_t& o) override {
  }
  void inject_mdata_error(const ghobject_t& o) override {
  }

  void compact() override {
  }
  bool has_builtin_csum() const override {
    return true;
  }

  inline void log_latency(const char* name,
    int idx,
    const ceph::timespan& lat,
    double lat_threshold,
    const char* info = "") const;

  inline void log_latency_fn(const char* name,
    int idx,
    const ceph::timespan& lat,
    double lat_threshold,
    std::function<std::string (const ceph::timespan& lat)> fn) const;

private:
  /*ceph::mutex qlock = ceph::make_mutex("BlueStore::Alerts::qlock");
  std::string spurious_read_errors_alert;

  void _log_alerts(osd_alert_list_t& alerts);
  void _set_spurious_read_errors_alert(const string& s) {
    std::lock_guard l(qlock);
    spurious_read_errors_alert = s;
  }*/

private:

  void _pad_zeros(ceph::buffer::list *bl, uint64_t *offset,
		  uint64_t chunk_size);

/*  int _touch(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& o);
  int _do_zero(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o,
	       uint64_t offset, size_t len);
  int _zero(TransContext *txc,
	    CollectionRef& c,
	    OnodeRef& o,
	    uint64_t offset, size_t len);
  void _do_truncate(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef o,
		   uint64_t offset,
		   std::set<SharedBlob*> *maybe_unshared_blobs=0);
  int _truncate(TransContext *txc,
		CollectionRef& c,
		OnodeRef& o,
		uint64_t offset);
  int _remove(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& o);
  int _do_remove(TransContext *txc,
		 CollectionRef& c,
		 OnodeRef o);
  int _setattr(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o,
	       const std::string& name,
	       ceph::buffer::ptr& val);
  int _setattrs(TransContext *txc,
		CollectionRef& c,
		OnodeRef& o,
		const std::map<std::string,ceph::buffer::ptr>& aset);
  int _rmattr(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& o,
	      const std::string& name);
  int _rmattrs(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o);
  void _do_omap_clear(TransContext *txc, OnodeRef &o);
  int _omap_clear(TransContext *txc,
		  CollectionRef& c,
		  OnodeRef& o);
  int _omap_setkeys(TransContext *txc,
		    CollectionRef& c,
		    OnodeRef& o,
		    ceph::buffer::list& bl);
  int _omap_setheader(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& o,
		      ceph::buffer::list& header);
  int _omap_rmkeys(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef& o,
		   ceph::buffer::list& bl);
  int _omap_rmkey_range(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o,
			const std::string& first, const std::string& last);
  int _set_alloc_hint(
    TransContext *txc,
    CollectionRef& c,
    OnodeRef& o,
    uint64_t expected_object_size,
    uint64_t expected_write_size,
    uint32_t flags);
  int _do_clone_range(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& oldo,
		      OnodeRef& newo,
		      uint64_t srcoff, uint64_t length, uint64_t dstoff);
  int _clone(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& oldo,
	     OnodeRef& newo);
  int _clone_range(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef& oldo,
		   OnodeRef& newo,
		   uint64_t srcoff, uint64_t length, uint64_t dstoff);
  int _rename(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& oldo,
	      OnodeRef& newo,
	      const ghobject_t& new_oid);*/
  int _create_collection(TransContext *txc, const coll_t &cid,
			 unsigned bits, CollectionRef *c);
  int _remove_collection(TransContext *txc, const coll_t &cid,
                         CollectionRef *c);
  void _do_remove_collection(TransContext *txc, CollectionRef *c);
  /*int _split_collection(TransContext *txc,
			CollectionRef& c,
			CollectionRef& d,
			unsigned bits, int rem);
  int _merge_collection(TransContext *txc,
			CollectionRef *c,
			CollectionRef& d,
			unsigned bits);
  */
};

static inline void intrusive_ptr_add_ref(DaoStore::Onode *o) {
  o->get();
}
static inline void intrusive_ptr_release(DaoStore::Onode *o) {
  o->put();
}

static inline void intrusive_ptr_add_ref(DaoStore::OpSequencer *o) {
  o->get();
}
static inline void intrusive_ptr_release(DaoStore::OpSequencer *o) {
  o->put();
}
} // namespace
#endif
