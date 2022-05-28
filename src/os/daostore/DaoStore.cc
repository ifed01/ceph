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

#include <unistd.h>
#include <stdlib.h>
/*#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <boost/container/flat_set.hpp>
#include "boost/algorithm/string.hpp"

#include "include/cpp-btree/btree_set.h"
*/
/*
#include "include/compat.h"
#include "include/intarith.h"
#include "include/stringify.h"
#include "include/str_map.h"
#include "include/util.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/url_escape.h"
#include "perfglue/heap_profiler.h"
#include "common/numa.h"*/
#include "common/errno.h"
#include "include/encoding.h"
#include "include/denc.h"
#include "common/pretty_binary.h"

#include "DaoStore.h"
#include "DaoStore_naming.h"

using namespace daostore;
//FIXME
#define dout_context cct
#define dout_subsys ceph_subsys_bluestore

// bluestore_cache_onode
/*MEMPOOL_DEFINE_OBJECT_FACTORY(BlueStore::Onode, bluestore_onode,
			      bluestore_cache_onode);
*/
// daostore_txc
MEMPOOL_DEFINE_OBJECT_FACTORY(DaoStore::TransContext, daostore_transcontext,
			      bluestore_txc); //FIXME: pool name
/*using std::deque;
using std::min;
using std::make_pair;
using std::numeric_limits;
using std::pair;
using std::list;
using std::map;
using std::max;
using std::ostream;
using std::ostringstream;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::coarse_mono_clock;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_timespan;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;*/

#define OBJECT_MAX_SIZE 0xffffffff // 32 bits

/*static void get_coll_range(const coll_t& cid, int bits,
  ghobject_t *temp_start, ghobject_t *temp_end,
  ghobject_t *start, ghobject_t *end)
{
  spg_t pgid;
  if (cid.is_pg(&pgid)) {
    start->shard_id = pgid.shard;
    *temp_start = *start;

    start->hobj.pool = pgid.pool();
    temp_start->hobj.pool = -2ll - pgid.pool();

    *end = *start;
    *temp_end = *temp_start;

    uint32_t reverse_hash = hobject_t::_reverse_bits(pgid.ps());
    start->hobj.set_bitwise_key_u32(reverse_hash);
    temp_start->hobj.set_bitwise_key_u32(reverse_hash);

    uint64_t end_hash = reverse_hash  + (1ull << (32 - bits));
    if (end_hash > 0xffffffffull)
      end_hash = 0xffffffffull;

    end->hobj.set_bitwise_key_u32(end_hash);
    temp_end->hobj.set_bitwise_key_u32(end_hash);
  } else {
    start->shard_id = shard_id_t::NO_SHARD;
    start->hobj.pool = -1ull;

    *end = *start;
    start->hobj.set_bitwise_key_u32(0);
    end->hobj.set_bitwise_key_u32(0xffffffff);

    // no separate temp section
    *temp_start = *end;
    *temp_end = *end;
  }

  start->generation = 0;
  end->generation = 0;
  temp_start->generation = 0;
  temp_end->generation = 0;
}*/

/*template<typename S>
static void _key_encode_prefix(const ghobject_t& oid, S *key)
{
  _key_encode_shard(oid.shard_id, key);
  _key_encode_u64(oid.hobj.pool + 0x8000000000000000ull, key);
  _key_encode_u32(oid.hobj.get_bitwise_key_u32(), key);
}

static const char *_key_decode_prefix(const char *p, ghobject_t *oid)
{
  p = _key_decode_shard(p, &oid->shard_id);

  uint64_t pool;
  p = _key_decode_u64(p, &pool);
  oid->hobj.pool = pool - 0x8000000000000000ull;

  unsigned hash;
  p = _key_decode_u32(p, &hash);

  oid->hobj.set_bitwise_key_u32(hash);

  return p;
}

#define ENCODED_KEY_PREFIX_LEN (1 + 8 + 4)

template<typename S>
static int get_key_object(const S& key, ghobject_t *oid)
{
  int r;
  const char *p = key.c_str();

  if (key.length() < ENCODED_KEY_PREFIX_LEN)
    return -1;

  p = _key_decode_prefix(p, oid);

  if (key.length() == ENCODED_KEY_PREFIX_LEN)
    return -2;

  r = decode_escaped(p, &oid->hobj.nspace);
  if (r < 0)
    return -2;
  p += r + 1;

  string k;
  r = decode_escaped(p, &k);
  if (r < 0)
    return -3;
  p += r + 1;
  if (*p == '=') {
    // no key
    ++p;
    oid->hobj.oid.name = k;
  } else if (*p == '<' || *p == '>') {
    // key + name
    ++p;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -5;
    p += r + 1;
    oid->hobj.set_key(k);
  } else {
    // malformed
    return -6;
  }

  p = _key_decode_u64(p, &oid->hobj.snap.val);
  p = _key_decode_u64(p, &oid->generation);

  if (*p != ONODE_KEY_SUFFIX) {
    return -7;
  }
  p++;
  if (*p) {
    // if we get something other than a null terminator here,
    // something goes wrong.
    return -8;
  }

  return 0;
}

template<typename S>
static void get_object_key(CephContext *cct, const ghobject_t& oid, S *key)
{
  key->clear();

  size_t max_len = ENCODED_KEY_PREFIX_LEN +
                  (oid.hobj.nspace.length() * 3 + 1) +
                  (oid.hobj.get_key().length() * 3 + 1) +
                   1 + // for '<', '=', or '>'
                  (oid.hobj.oid.name.length() * 3 + 1) +
                   8 + 8 + 1;
  key->reserve(max_len);

  _key_encode_prefix(oid, key);

  append_escaped(oid.hobj.nspace, key);

  if (oid.hobj.get_key().length()) {
    // is a key... could be < = or >.
    append_escaped(oid.hobj.get_key(), key);
    // (ASCII chars < = and > sort in that order, yay)
    int r = oid.hobj.get_key().compare(oid.hobj.oid.name);
    if (r) {
      key->append(r > 0 ? ">" : "<");
      append_escaped(oid.hobj.oid.name, key);
    } else {
      // same as no key
      key->append("=");
    }
  } else {
    // no key
    append_escaped(oid.hobj.oid.name, key);
    key->append("=");
  }

  _key_encode_u64(oid.hobj.snap, key);
  _key_encode_u64(oid.generation, key);

  key->push_back(ONODE_KEY_SUFFIX);

  // sanity check
  if (true) {
    ghobject_t t;
    int r = get_key_object(*key, &t);
    if (r || t != oid) {
      derr << "  r " << r << dendl;
      derr << "key " << pretty_binary_string(*key) << dendl;
      derr << "oid " << oid << dendl;
      derr << "  t " << t << dendl;
      ceph_assert(r == 0 && t == oid);
    }
  }
}*/

/*template <int LogLevelV>
void _dump_onode(CephContext *cct, const BlueStore::Onode& o)
{
  if (!cct->_conf->subsys.should_gather<ceph_subsys_bluestore, LogLevelV>())
    return;
  dout(LogLevelV) << __func__ << " " << &o << " " << o.oid
		  << " nid " << o.onode.nid
		  << " size 0x" << std::hex << o.onode.size
		  << " (" << std::dec << o.onode.size << ")"
		  << " expected_object_size " << o.onode.expected_object_size
		  << " expected_write_size " << o.onode.expected_write_size
		  << " in " << o.onode.extent_map_shards.size() << " shards"
		  << ", " << o.extent_map.spanning_blob_map.size()
		  << " spanning blobs"
		  << dendl;
  for (auto p = o.onode.attrs.begin();
       p != o.onode.attrs.end();
       ++p) {
    dout(LogLevelV) << __func__ << "  attr " << p->first
		    << " len " << p->second.length() << dendl;
  }
  _dump_extent_map<LogLevelV>(cct, o.extent_map);
}
*/
template <int LogLevelV>
void _dump_transaction(CephContext *cct, ObjectStore::Transaction *t)
{
  dout(LogLevelV) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  f.open_object_section("transaction");
  t->dump(&f);
  f.close_section();
  f.flush(*_dout);
  *_dout << dendl;
}

/*
namespace {

 *
 * Due to a bug in key string encoding (see a comment for append_escaped)
 * the KeyValueDB iterator does not lexicographically sort the same
 * way that ghobject_t does: objects with the same hash may have wrong order.
 *
 * This is the iterator wrapper that fixes the keys order.
 *

class CollectionListIterator {
public:
  CollectionListIterator(const KeyValueDB::Iterator &it)
    : m_it(it) {
  }
  virtual ~CollectionListIterator() {
  }

  virtual bool valid() const = 0;
  virtual const ghobject_t &oid() const = 0;
  virtual void lower_bound(const ghobject_t &oid) = 0;
  virtual void upper_bound(const ghobject_t &oid) = 0;
  virtual void next() = 0;

  virtual int cmp(const ghobject_t &oid) const = 0;

  bool is_ge(const ghobject_t &oid) const {
    return cmp(oid) >= 0;
  }

  bool is_lt(const ghobject_t &oid) const {
    return cmp(oid) < 0;
  }

protected:
  KeyValueDB::Iterator m_it;
};

class SimpleCollectionListIterator : public CollectionListIterator {
public:
  SimpleCollectionListIterator(CephContext *cct, const KeyValueDB::Iterator &it)
    : CollectionListIterator(it), m_cct(cct) {
  }

  bool valid() const override {
    return m_it->valid();
  }

  const ghobject_t &oid() const override {
    ceph_assert(valid());

    return m_oid;
  }

  void lower_bound(const ghobject_t &oid) override {
    string key;
    get_object_key(m_cct, oid, &key);

    m_it->lower_bound(key);
    get_oid();
  }

  void upper_bound(const ghobject_t &oid) override {
    string key;
    get_object_key(m_cct, oid, &key);

    m_it->upper_bound(key);
    get_oid();
  }

  void next() override {
    ceph_assert(valid());

    m_it->next();
    get_oid();
  }

  int cmp(const ghobject_t &oid) const override {
    ceph_assert(valid());

    string key;
    get_object_key(m_cct, oid, &key);

    return m_it->key().compare(key);
  }

private:
  CephContext *m_cct;
  ghobject_t m_oid;

  void get_oid() {
    m_oid = ghobject_t();
    while (m_it->valid() && is_extent_shard_key(m_it->key())) {
      m_it->next();
    }
    if (!valid()) {
      return;
    }

    int r = get_key_object(m_it->key(), &m_oid);
    ceph_assert(r == 0);
  }
};

class SortedCollectionListIterator : public CollectionListIterator {
public:
  SortedCollectionListIterator(const KeyValueDB::Iterator &it)
    : CollectionListIterator(it), m_chunk_iter(m_chunk.end()) {
  }

  bool valid() const override {
    return m_chunk_iter != m_chunk.end();
  }

  const ghobject_t &oid() const override {
    ceph_assert(valid());

    return m_chunk_iter->first;
  }

  void lower_bound(const ghobject_t &oid) override {
    std::string key;
    _key_encode_prefix(oid, &key);

    m_it->lower_bound(key);
    m_chunk_iter = m_chunk.end();
    if (!get_next_chunk()) {
      return;
    }

    if (this->oid().shard_id != oid.shard_id ||
        this->oid().hobj.pool != oid.hobj.pool ||
        this->oid().hobj.get_bitwise_key_u32() != oid.hobj.get_bitwise_key_u32()) {
      return;
    }

    m_chunk_iter = m_chunk.lower_bound(oid);
    if (m_chunk_iter == m_chunk.end()) {
      get_next_chunk();
    }
  }

  void upper_bound(const ghobject_t &oid) override {
    lower_bound(oid);

    if (valid() && this->oid() == oid) {
      next();
    }
  }

  void next() override {
    ceph_assert(valid());

    m_chunk_iter++;
    if (m_chunk_iter == m_chunk.end()) {
      get_next_chunk();
    }
  }

  int cmp(const ghobject_t &oid) const override {
    ceph_assert(valid());

    if (this->oid() < oid) {
      return -1;
    }
    if (this->oid() > oid) {
      return 1;
    }
    return 0;
  }

private:
  std::map<ghobject_t, std::string> m_chunk;
  std::map<ghobject_t, std::string>::iterator m_chunk_iter;

  bool get_next_chunk() {
    while (m_it->valid() && is_extent_shard_key(m_it->key())) {
      m_it->next();
    }

    if (!m_it->valid()) {
      return false;
    }

    ghobject_t oid;
    int r = get_key_object(m_it->key(), &oid);
    ceph_assert(r == 0);

    m_chunk.clear();
    while (true) {
      m_chunk.insert({oid, m_it->key()});

      do {
        m_it->next();
      } while (m_it->valid() && is_extent_shard_key(m_it->key()));

      if (!m_it->valid()) {
        break;
      }

      ghobject_t next;
      r = get_key_object(m_it->key(), &next);
      ceph_assert(r == 0);
      if (next.shard_id != oid.shard_id ||
          next.hobj.pool != oid.hobj.pool ||
          next.hobj.get_bitwise_key_u32() != oid.hobj.get_bitwise_key_u32()) {
        break;
      }
      oid = next;
    }

    m_chunk_iter = m_chunk.begin();
    return true;
  }
};

} // anonymous namespace
*/

/*// LruOnodeCacheShard
struct LruOnodeCacheShard : public BlueStore::OnodeCacheShard {
  typedef boost::intrusive::list<
    BlueStore::Onode,
    boost::intrusive::member_hook<
      BlueStore::Onode,
      boost::intrusive::list_member_hook<>,
      &BlueStore::Onode::lru_item> > list_t;

  list_t lru;

  explicit LruOnodeCacheShard(CephContext *cct) : BlueStore::OnodeCacheShard(cct) {}

  void _add(BlueStore::Onode* o, int level) override
  {
    if (o->put_cache()) {
      (level > 0) ? lru.push_front(*o) : lru.push_back(*o);
    } else {
      ++num_pinned;
    }
    ++num; // we count both pinned and unpinned entries
    dout(20) << __func__ << " " << this << " " << o->oid << " added, num=" << num << dendl;
  }
  void _rm(BlueStore::Onode* o) override
  {
    if (o->pop_cache()) {
      lru.erase(lru.iterator_to(*o));
    } else {
      ceph_assert(num_pinned);
      --num_pinned;
    }
    ceph_assert(num);
    --num;
    dout(20) << __func__ << " " << this << " " << " " << o->oid << " removed, num=" << num << dendl;
  }
  void _pin(BlueStore::Onode* o) override
  {
    lru.erase(lru.iterator_to(*o));
    ++num_pinned;
    dout(20) << __func__ << this << " " << " " << " " << o->oid << " pinned" << dendl;
  }
  void _unpin(BlueStore::Onode* o) override
  {
    lru.push_front(*o);
    ceph_assert(num_pinned);
    --num_pinned;
    dout(20) << __func__ << this << " " << " " << " " << o->oid << " unpinned" << dendl;
  }
  void _unpin_and_rm(BlueStore::Onode* o) override
  {
    o->pop_cache();
    ceph_assert(num_pinned);
    --num_pinned;
    ceph_assert(num);
    --num;
  }
  void _trim_to(uint64_t new_size) override
  {
    if (new_size >= lru.size()) {
      return; // don't even try
    } 
    uint64_t n = lru.size() - new_size;
    auto p = lru.end();
    ceph_assert(p != lru.begin());
    --p;
    ceph_assert(num >= n);
    num -= n;
    while (n-- > 0) {
      BlueStore::Onode *o = &*p;
      dout(20) << __func__ << "  rm " << o->oid << " "
               << o->nref << " " << o->cached << " " << o->pinned << dendl;
      if (p != lru.begin()) {
        lru.erase(p--);
      } else {
        ceph_assert(n == 0);
        lru.erase(p);
      }
      auto pinned = !o->pop_cache();
      ceph_assert(!pinned);
      o->c->onode_map._remove(o->oid);
    }
  }
  void move_pinned(OnodeCacheShard *to, BlueStore::Onode *o) override
  {
    if (to == this) {
      return;
    }
    ceph_assert(o->cached);
    ceph_assert(o->pinned);
    ceph_assert(num);
    ceph_assert(num_pinned);
    --num_pinned;
    --num;
    ++to->num_pinned;
    ++to->num;
  }
  void add_stats(uint64_t *onodes, uint64_t *pinned_onodes) override
  {
    *onodes += num;
    *pinned_onodes += num_pinned;
  }
};

// OnodeCacheShard
BlueStore::OnodeCacheShard *BlueStore::OnodeCacheShard::create(
    CephContext* cct,
    string type,
    PerfCounters *logger)
{
  BlueStore::OnodeCacheShard *c = nullptr;
  // Currently we only implement an LRU cache for onodes
  c = new LruOnodeCacheShard(cct);
  c->logger = logger;
  return c;
}*/

/*
// OnodeSpace

#undef dout_prefix
#define dout_prefix *_dout << "daostore.OnodeSpace(" << this << " in " << cache << ") "

BlueStore::OnodeRef BlueStore::OnodeSpace::add(const ghobject_t& oid,
  OnodeRef& o)
{
  std::lock_guard l(cache->lock);
  auto p = onode_map.find(oid);
  if (p != onode_map.end()) {
    ldout(cache->cct, 30) << __func__ << " " << oid << " " << o
			  << " raced, returning existing " << p->second
			  << dendl;
    return p->second;
  }
  ldout(cache->cct, 20) << __func__ << " " << oid << " " << o << dendl;
  onode_map[oid] = o;
  cache->_add(o.get(), 1);
  cache->_trim();
  return o;
}

void BlueStore::OnodeSpace::_remove(const ghobject_t& oid)
{
  ldout(cache->cct, 20) << __func__ << " " << oid << " " << dendl;
  onode_map.erase(oid);
}

BlueStore::OnodeRef BlueStore::OnodeSpace::lookup(const ghobject_t& oid)
{
  ldout(cache->cct, 30) << __func__ << dendl;
  OnodeRef o;
  bool hit = false;

  {
    std::lock_guard l(cache->lock);
    ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
    if (p == onode_map.end()) {
      ldout(cache->cct, 30) << __func__ << " " << oid << " miss" << dendl;
    } else {
      ldout(cache->cct, 30) << __func__ << " " << oid << " hit " << p->second
                            << " " << p->second->nref
                            << " " << p->second->cached
                            << " " << p->second->pinned
			    << dendl;
      // This will pin onode and implicitly touch the cache when Onode
      // eventually will become unpinned
      o = p->second;
      ceph_assert(!o->cached || o->pinned);

      hit = true;
    }
  }

  if (hit) {
    cache->logger->inc(l_bluestore_onode_hits);
  } else {
    cache->logger->inc(l_bluestore_onode_misses);
  }
  return o;
}

void BlueStore::OnodeSpace::clear()
{
  std::lock_guard l(cache->lock);
  ldout(cache->cct, 10) << __func__ << " " << onode_map.size()<< dendl;
  for (auto &p : onode_map) {
    cache->_rm(p.second.get());
  }
  onode_map.clear();
}

bool BlueStore::OnodeSpace::empty()
{
  std::lock_guard l(cache->lock);
  return onode_map.empty();
}

void BlueStore::OnodeSpace::rename(
  OnodeRef& oldo,
  const ghobject_t& old_oid,
  const ghobject_t& new_oid,
  const mempool::bluestore_cache_meta::string& new_okey)
{
  std::lock_guard l(cache->lock);
  ldout(cache->cct, 30) << __func__ << " " << old_oid << " -> " << new_oid
			<< dendl;
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator po, pn;
  po = onode_map.find(old_oid);
  pn = onode_map.find(new_oid);
  ceph_assert(po != pn);

  ceph_assert(po != onode_map.end());
  if (pn != onode_map.end()) {
    ldout(cache->cct, 30) << __func__ << "  removing target " << pn->second
			  << dendl;
    cache->_rm(pn->second.get());
    onode_map.erase(pn);
  }
  OnodeRef o = po->second;

  // install a non-existent onode at old location
  oldo.reset(new Onode(o->c, old_oid, o->key));
  po->second = oldo;
  cache->_add(oldo.get(), 1);
  // add at new position and fix oid, key.
  // This will pin 'o' and implicitly touch cache
  // when it will eventually become unpinned
  onode_map.insert(make_pair(new_oid, o));
  ceph_assert(o->pinned);

  o->oid = new_oid;
  o->key = new_okey;
  cache->_trim();
}

bool BlueStore::OnodeSpace::map_any(std::function<bool(Onode*)> f)
{
  std::lock_guard l(cache->lock);
  ldout(cache->cct, 20) << __func__ << dendl;
  for (auto& i : onode_map) {
    if (f(i.second.get())) {
      return true;
    }
  }
  return false;
}

template <int LogLevelV = 30>
void BlueStore::OnodeSpace::dump(CephContext *cct)
{
  for (auto& i : onode_map) {
    ldout(cct, LogLevelV) << i.first << " : " << i.second
      << " " << i.second->nref
      << " " << i.second->cached
      << " " << i.second->pinned
      << dendl;
  }
}
 */

/*// Onode

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.onode(" << this << ")." << __func__ << " "

//
// A tricky thing about Onode's ref counter is that we do an additional
// increment when newly pinned instance is detected. And -1 on unpin.
// This prevents from a conflict with a delete call (when nref == 0).
// The latter might happen while the thread is in unpin() function
// (and e.g. waiting for lock acquisition) since nref is already
// decremented. And another 'putting' thread on the instance will release it.
//
void BlueStore::Onode::get() {
  if (++nref >= 2 && !pinned) {
    OnodeCacheShard* ocs = c->get_onode_cache();
    ocs->lock.lock();
    // It is possible that during waiting split_cache moved us to different OnodeCacheShard.
    while (ocs != c->get_onode_cache()) {
      ocs->lock.unlock();
      ocs = c->get_onode_cache();
      ocs->lock.lock();
    }
    bool was_pinned = pinned;
    pinned = nref >= 2;
    // additional increment for newly pinned instance
    bool r = !was_pinned && pinned;
    if (r) {
      ++nref;
    }
    if (cached && r) {
      ocs->_pin(this);
    }
    ocs->lock.unlock();
  }
}
void BlueStore::Onode::put() {
  int n = --nref;
  if (n == 2) {
    OnodeCacheShard* ocs = c->get_onode_cache();
    ocs->lock.lock();
    // It is possible that during waiting split_cache moved us to different OnodeCacheShard.
    while (ocs != c->get_onode_cache()) {
      ocs->lock.unlock();
      ocs = c->get_onode_cache();
      ocs->lock.lock();
    }
    bool need_unpin = pinned;
    pinned = pinned && nref > 2; // intentionally use > not >= as we have
                                 // +1 due to pinned state
    need_unpin = need_unpin && !pinned;
    if (cached && need_unpin) {
      if (exists) {
        ocs->_unpin(this);
      } else {
        ocs->_unpin_and_rm(this);
        // remove will also decrement nref and delete Onode
        c->onode_map._remove(oid);
      }
    }
    // additional decrement for newly unpinned instance
    // should be the last action since Onode can be released
    // at any point after this decrement
    if (need_unpin) {
      n = --nref;
    }
    ocs->lock.unlock();
  }
  if (n == 0) {
    delete this;
  }
}

BlueStore::Onode* BlueStore::Onode::decode(
  CollectionRef c,
  const ghobject_t& oid,
  const string& key,
  const bufferlist& v)
{
  Onode* on = new Onode(c.get(), oid, key);
  on->exists = true;
  auto p = v.front().begin_deep();
  on->onode.decode(p);
  for (auto& i : on->onode.attrs) {
    i.second.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
  }

  // initialize extent_map
  on->extent_map.decode_spanning_blobs(p);
  if (on->onode.extent_map_shards.empty()) {
    denc(on->extent_map.inline_bl, p);
    on->extent_map.decode_some(on->extent_map.inline_bl);
    on->extent_map.inline_bl.reassign_to_mempool(
      mempool::mempool_bluestore_cache_data);
  } else {
    on->extent_map.init_shards(false, false);
  }
  return on;
}

void BlueStore::Onode::flush()
{
  if (flushing_count.load()) {
    ldout(c->store->cct, 20) << __func__ << " cnt:" << flushing_count << dendl;
    waiting_count++;
    std::unique_lock l(flush_lock);
    while (flushing_count.load()) {
      flush_cond.wait(l);
    }
    waiting_count--;
  }
  ldout(c->store->cct, 20) << __func__ << " done" << dendl;
}

void BlueStore::Onode::dump(Formatter* f) const
{
  onode.dump(f);
  extent_map.dump(f);
}


const string& BlueStore::Onode::get_omap_prefix()
{
  if (onode.is_pgmeta_omap()) {
    return PREFIX_PGMETA_OMAP;
  }
  if (onode.is_perpg_omap()) {
    return PREFIX_PERPG_OMAP;
  }
  if (onode.is_perpool_omap()) {
    return PREFIX_PERPOOL_OMAP;
  }
  return PREFIX_OMAP;
}

// '-' < '.' < '~'

void BlueStore::Onode::get_omap_header(string *out)
{
  if (!onode.is_pgmeta_omap()) {
    if (onode.is_perpg_omap()) {
      _key_encode_u64(c->pool(), out);
      _key_encode_u32(oid.hobj.get_bitwise_key_u32(), out);
    } else if (onode.is_perpool_omap()) {
      _key_encode_u64(c->pool(), out);
    }
  }
  _key_encode_u64(onode.nid, out);
  out->push_back('-');
}

void BlueStore::Onode::get_omap_key(const string& key, string *out)
{
  if (!onode.is_pgmeta_omap()) {
    if (onode.is_perpg_omap()) {
      _key_encode_u64(c->pool(), out);
      _key_encode_u32(oid.hobj.get_bitwise_key_u32(), out);
    } else if (onode.is_perpool_omap()) {
      _key_encode_u64(c->pool(), out);
    }
  }
  _key_encode_u64(onode.nid, out);
  out->push_back('.');
  out->append(key);
}

void BlueStore::Onode::rewrite_omap_key(const string& old, string *out)
{
  if (!onode.is_pgmeta_omap()) {
    if (onode.is_perpg_omap()) {
      _key_encode_u64(c->pool(), out);
      _key_encode_u32(oid.hobj.get_bitwise_key_u32(), out);
    } else if (onode.is_perpool_omap()) {
      _key_encode_u64(c->pool(), out);
    }
  }
  _key_encode_u64(onode.nid, out);
  out->append(old.c_str() + out->length(), old.size() - out->length());
}

void BlueStore::Onode::get_omap_tail(string *out)
{
  if (!onode.is_pgmeta_omap()) {
    if (onode.is_perpg_omap()) {
      _key_encode_u64(c->pool(), out);
      _key_encode_u32(oid.hobj.get_bitwise_key_u32(), out);
    } else if (onode.is_perpool_omap()) {
      _key_encode_u64(c->pool(), out);
    }
  }
  _key_encode_u64(onode.nid, out);
  out->push_back('~');
}

void BlueStore::Onode::decode_omap_key(const string& key, string *user_key)
{
  size_t pos = sizeof(uint64_t) + 1;
  if (!onode.is_pgmeta_omap()) {
    if (onode.is_perpg_omap()) {
      pos += sizeof(uint64_t) + sizeof(uint32_t);
    } else if (onode.is_perpool_omap()) {
      pos += sizeof(uint64_t);
    }
  }
  *user_key = key.substr(pos);
}


// =======================================================
// WriteContext
 
/// Checks for writes to the same pextent within a blob
bool BlueStore::WriteContext::has_conflict(
  BlobRef b,
  uint64_t loffs,
  uint64_t loffs_end,
  uint64_t min_alloc_size)
{
  ceph_assert((loffs % min_alloc_size) == 0);
  ceph_assert((loffs_end % min_alloc_size) == 0);
  for (auto w : writes) {
    if (b == w.b) {
      auto loffs2 = p2align(w.logical_offset, min_alloc_size);
      auto loffs2_end = p2roundup(w.logical_offset + w.length0, min_alloc_size);
      if ((loffs <= loffs2 && loffs_end > loffs2) ||
          (loffs >= loffs2 && loffs < loffs2_end)) {
        return true;
      }
    }
  }
  return false;
}*/
 

// Collection

#undef dout_prefix
#define dout_prefix *_dout << "daostore(" << store->path << ").collection(" << cid << " " << this << ") "

/*ol BlueStore::Collection::flush_commit(Context *c)
{
  return osr->flush_commit(c);
}

void BlueStore::Collection::flush()
{
  osr->flush();
}

void BlueStore::Collection::flush_all_but_last()
{
  osr->flush_all_but_last();
}
*/

DaoStore::OnodeRef DaoStore::Collection::get_onode(
  const ghobject_t& oid,
  bool create,
  bool is_createop)
{
  ceph_assert(create ? ceph_mutex_is_wlocked(lock) : ceph_mutex_is_locked(lock));

  spg_t pgid;
  if (cid.is_pg(&pgid)) {
    if (!oid.match(bits, pgid.ps())) {
      lderr(store->cct) << __func__ << " oid " << oid << " not part of "
			<< pgid << " bits " << bits << dendl;
      ceph_abort();
    }
  }

  OnodeRef o = _lookup(oid);
  if (o)
    return o;

  std::string key;
  get_object_key(store->cct, oid, &key);

  ldout(store->cct, 20) << __func__ << " oid " << oid << " key "
			<< pretty_binary_string(key) << dendl;

  bufferlist v;
  int r = -ENOENT;
  Onode *on;
/*  if (!is_createop) {
    r = store->db->get(PREFIX_OBJ, key.c_str(), key.size(), &v);
    ldout(store->cct, 20) << " r " << r << " v.len " << v.length() << dendl;
  }*/
  if (v.length() == 0) {
    ceph_assert(r == -ENOENT);
    if (!create)
      return OnodeRef();

    // new object, new onode
    on = new Onode(this, oid, key);
  } else {
    // loaded
    ceph_assert(r >= 0);
    on = Onode::decode(this, oid, key, v);
  }
  o.reset(on);
  onode_map.emplace(oid, o);
  return o;
}

bool DaoStore::Collection::map_any(std::function<bool(Onode*)> f)
{
  std::lock_guard l(lock);
  ldout(store->cct, 20) << __func__ << dendl;
  for (auto& i : onode_map) {
    if (f(i.second.get())) {
      return true;
    }
  }
  return false;
}

int DaoStore::Collection::collection_list(DaosContainer& daos,
					  const ghobject_t& start,
					  std::function<bool(const ghobject_t& e)> fn)
{
  flush();
  int r;
  {
    std::shared_lock l(lock);
    //fn(e);
  }
  return r;
}

void DaoStore::Collection::clear_onodes()
{
  std::lock_guard l(lock);
  onode_map.clear();
}

DaoStore::OnodeRef DaoStore::Collection::_lookup(const ghobject_t& oid)
{
  ldout(store->cct, 30) << __func__ << dendl;
  OnodeRef o;
  bool hit = false;

  {
    auto p = onode_map.find(oid);
    if (p == onode_map.end()) {
      ldout(store->cct, 30) << __func__ << " " << oid << " miss" << dendl;
    }
    else {
      ldout(store->cct, 30) << __func__ << " " << oid << " hit " << p->second
	<< " " << p->second->nref
	/*<< " " << p->second->cached
	<< " " << p->second->pinned*/
	<< dendl;
      // This will pin onode and implicitly touch the cache when Onode
      // eventually will become unpinned
      o = p->second;
      //ceph_assert(!o->cached || o->pinned);

      hit = true;
    }
  }

  store->get_perf_counters()->inc(hit ? l_daostore_onode_hits : l_daostore_onode_misses);
  return o;
}

// =======================================================

// MempoolThread

/*#undef dout_prefix
#define dout_prefix *_dout << "bluestore.MempoolThread(" << this << ") "
#undef dout_context
#define dout_context store->cct

void *BlueStore::MempoolThread::entry()
{
  std::unique_lock l{lock};

  uint32_t prev_config_change = store->config_changed.load();
  uint64_t base = store->osd_memory_base;
  double fragmentation = store->osd_memory_expected_fragmentation;
  uint64_t target = store->osd_memory_target;
  uint64_t min = store->osd_memory_cache_min;
  uint64_t max = min;

  // When setting the maximum amount of memory to use for cache, first 
  // assume some base amount of memory for the OSD and then fudge in
  // some overhead for fragmentation that scales with cache usage.
  uint64_t ltarget = (1.0 - fragmentation) * target;
  if (ltarget > base + min) {
    max = ltarget - base;
  }

  binned_kv_cache = store->db->get_priority_cache();
  binned_kv_onode_cache = store->db->get_priority_cache(PREFIX_OBJ);
  if (store->cache_autotune && binned_kv_cache != nullptr) {
    pcm = std::make_shared<PriorityCache::Manager>(
        store->cct, min, max, target, true, "bluestore-pricache");
    pcm->insert("kv", binned_kv_cache, true);
    pcm->insert("meta", meta_cache, true);
    pcm->insert("data", data_cache, true);
    if (binned_kv_onode_cache != nullptr) {
      pcm->insert("kv_onode", binned_kv_onode_cache, true);
    }
  }

  utime_t next_balance = ceph_clock_now();
  utime_t next_resize = ceph_clock_now();
  utime_t next_deferred_force_submit = ceph_clock_now();
  utime_t alloc_stats_dump_clock = ceph_clock_now();

  bool interval_stats_trim = false;
  while (!stop) {
    // Update pcm cache settings if related configuration was changed
    uint32_t cur_config_change = store->config_changed.load();
    if (cur_config_change != prev_config_change) {
      _update_cache_settings();
      prev_config_change = cur_config_change;
    }

    // Before we trim, check and see if it's time to rebalance/resize.
    double autotune_interval = store->cache_autotune_interval;
    double resize_interval = store->osd_memory_cache_resize_interval;
    double max_defer_interval = store->max_defer_interval;

    double alloc_stats_dump_interval =
      store->cct->_conf->bluestore_alloc_stats_dump_interval;

    if (alloc_stats_dump_interval > 0 &&
        alloc_stats_dump_clock + alloc_stats_dump_interval < ceph_clock_now()) {
      store->_record_allocation_stats();
      alloc_stats_dump_clock = ceph_clock_now();
    }
    if (autotune_interval > 0 && next_balance < ceph_clock_now()) {
      _adjust_cache_settings();

      // Log events at 5 instead of 20 when balance happens.
      interval_stats_trim = true;

      if (pcm != nullptr) {
        pcm->balance();
      }

      next_balance = ceph_clock_now();
      next_balance += autotune_interval;
    }
    if (resize_interval > 0 && next_resize < ceph_clock_now()) {
      if (ceph_using_tcmalloc() && pcm != nullptr) {
        pcm->tune_memory();
      }
      next_resize = ceph_clock_now();
      next_resize += resize_interval;
    }

    if (max_defer_interval > 0 &&
	next_deferred_force_submit < ceph_clock_now()) {
      if (store->get_deferred_last_submitted() + max_defer_interval <
	  ceph_clock_now()) {
	store->deferred_try_submit();
      }
      next_deferred_force_submit = ceph_clock_now();
      next_deferred_force_submit += max_defer_interval/3;
    }

    // Now Resize the shards 
    _resize_shards(interval_stats_trim);
    interval_stats_trim = false;

    store->_update_cache_logger();
    auto wait = ceph::make_timespan(
      store->cct->_conf->bluestore_cache_trim_interval);
    cond.wait_for(l, wait);
  }
  // do final dump
  store->_record_allocation_stats();
  stop = false;
  pcm = nullptr;
  return NULL;
}

void BlueStore::MempoolThread::_adjust_cache_settings()
{
  if (binned_kv_cache != nullptr) {
    binned_kv_cache->set_cache_ratio(store->cache_kv_ratio);
  }
  if (binned_kv_onode_cache != nullptr) {
    binned_kv_onode_cache->set_cache_ratio(store->cache_kv_onode_ratio);
  }
  meta_cache->set_cache_ratio(store->cache_meta_ratio);
  data_cache->set_cache_ratio(store->cache_data_ratio);
}

void BlueStore::MempoolThread::_resize_shards(bool interval_stats)
{
  size_t onode_shards = store->onode_cache_shards.size();
  size_t buffer_shards = store->buffer_cache_shards.size();
  int64_t kv_used = store->db->get_cache_usage();
  int64_t kv_onode_used = store->db->get_cache_usage(PREFIX_OBJ);
  int64_t meta_used = meta_cache->_get_used_bytes();
  int64_t data_used = data_cache->_get_used_bytes();

  uint64_t cache_size = store->cache_size;
  int64_t kv_alloc =
     static_cast<int64_t>(store->cache_kv_ratio * cache_size); 
  int64_t kv_onode_alloc =
     static_cast<int64_t>(store->cache_kv_onode_ratio * cache_size);
  int64_t meta_alloc =
     static_cast<int64_t>(store->cache_meta_ratio * cache_size);
  int64_t data_alloc =
     static_cast<int64_t>(store->cache_data_ratio * cache_size);

  if (pcm != nullptr && binned_kv_cache != nullptr) {
    cache_size = pcm->get_tuned_mem();
    kv_alloc = binned_kv_cache->get_committed_size();
    meta_alloc = meta_cache->get_committed_size();
    data_alloc = data_cache->get_committed_size();
    if (binned_kv_onode_cache != nullptr) {
      kv_onode_alloc = binned_kv_onode_cache->get_committed_size();
    }
  }
  
  if (interval_stats) {
    dout(5) << __func__  << " cache_size: " << cache_size
                  << " kv_alloc: " << kv_alloc
                  << " kv_used: " << kv_used
                  << " kv_onode_alloc: " << kv_onode_alloc
                  << " kv_onode_used: " << kv_onode_used
                  << " meta_alloc: " << meta_alloc
                  << " meta_used: " << meta_used
                  << " data_alloc: " << data_alloc
                  << " data_used: " << data_used << dendl;
  } else {
    dout(20) << __func__  << " cache_size: " << cache_size
                   << " kv_alloc: " << kv_alloc
                   << " kv_used: " << kv_used
                   << " kv_onode_alloc: " << kv_onode_alloc
                   << " kv_onode_used: " << kv_onode_used
                   << " meta_alloc: " << meta_alloc
                   << " meta_used: " << meta_used
                   << " data_alloc: " << data_alloc
                   << " data_used: " << data_used << dendl;
  }

  uint64_t max_shard_onodes = static_cast<uint64_t>(
      (meta_alloc / (double) onode_shards) / meta_cache->get_bytes_per_onode());
  uint64_t max_shard_buffer = static_cast<uint64_t>(data_alloc / buffer_shards);

  dout(30) << __func__ << " max_shard_onodes: " << max_shard_onodes
                 << " max_shard_buffer: " << max_shard_buffer << dendl;

  for (auto i : store->onode_cache_shards) {
    i->set_max(max_shard_onodes);
  }
  for (auto i : store->buffer_cache_shards) {
    i->set_max(max_shard_buffer);
  }
}

void BlueStore::MempoolThread::_update_cache_settings()
{
  // Nothing to do if pcm is not used.
  if (pcm == nullptr) {
    return;
  }

  uint64_t target = store->osd_memory_target;
  uint64_t base = store->osd_memory_base;
  uint64_t min = store->osd_memory_cache_min;
  uint64_t max = min;
  double fragmentation = store->osd_memory_expected_fragmentation;

  uint64_t ltarget = (1.0 - fragmentation) * target;
  if (ltarget > base + min) {
    max = ltarget - base;
  }

  // set pcm cache levels
  pcm->set_target_memory(target);
  pcm->set_min_memory(min);
  pcm->set_max_memory(max);

  dout(5) << __func__  << " updated pcm target: " << target
                << " pcm min: " << min
                << " pcm max: " << max
                << dendl;
}
*/
// =======================================================
/*
// OmapIteratorImpl

#undef dout_prefix
#define dout_prefix *_dout << "bluestore.OmapIteratorImpl(" << this << ") "

BlueStore::OmapIteratorImpl::OmapIteratorImpl(
  CollectionRef c, OnodeRef o, KeyValueDB::Iterator it)
  : c(c), o(o), it(it)
{
  std::shared_lock l(c->lock);
  if (o->onode.has_omap()) {
    o->get_omap_key(string(), &head);
    o->get_omap_tail(&tail);
    it->lower_bound(head);
  }
}

string BlueStore::OmapIteratorImpl::_stringify() const
{
  stringstream s;
  s << " omap_iterator(cid = " << c->cid
    <<", oid = " << o->oid << ")";
  return s.str();
}

int BlueStore::OmapIteratorImpl::seek_to_first()
{
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  if (o->onode.has_omap()) {
    it->lower_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  c->store->log_latency(
    __func__,
    l_bluestore_omap_seek_to_first_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age);

  return 0;
}

int BlueStore::OmapIteratorImpl::upper_bound(const string& after)
{
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  if (o->onode.has_omap()) {
    string key;
    o->get_omap_key(after, &key);
    ldout(c->store->cct,20) << __func__ << " after " << after << " key "
			    << pretty_binary_string(key) << dendl;
    it->upper_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  c->store->log_latency_fn(
    __func__,
    l_bluestore_omap_upper_bound_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age,
    [&] (const ceph::timespan& lat) {
      return ", after = " + after +
	_stringify();
    }
  );
  return 0;
}

int BlueStore::OmapIteratorImpl::lower_bound(const string& to)
{
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  if (o->onode.has_omap()) {
    string key;
    o->get_omap_key(to, &key);
    ldout(c->store->cct,20) << __func__ << " to " << to << " key "
			    << pretty_binary_string(key) << dendl;
    it->lower_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  c->store->log_latency_fn(
    __func__,
    l_bluestore_omap_lower_bound_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age,
    [&] (const ceph::timespan& lat) {
      return ", to = " + to +
	_stringify();
    }
  );
  return 0;
}

bool BlueStore::OmapIteratorImpl::valid()
{
  std::shared_lock l(c->lock);
  bool r = o->onode.has_omap() && it && it->valid() &&
    it->raw_key().second < tail;
  if (it && it->valid()) {
    ldout(c->store->cct,20) << __func__ << " is at "
			    << pretty_binary_string(it->raw_key().second)
			    << dendl;
  }
  return r;
}

int BlueStore::OmapIteratorImpl::next()
{
  int r = -1;
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  if (o->onode.has_omap()) {
    it->next();
    r = 0;
  }
  c->store->log_latency(
    __func__,
    l_bluestore_omap_next_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age);

  return r;
}

string BlueStore::OmapIteratorImpl::key()
{
  std::shared_lock l(c->lock);
  ceph_assert(it->valid());
  string db_key = it->raw_key().second;
  string user_key;
  o->decode_omap_key(db_key, &user_key);

  return user_key;
}

bufferlist BlueStore::OmapIteratorImpl::value()
{
  std::shared_lock l(c->lock);
  ceph_assert(it->valid());
  return it->value();
}
*/

// =====================================

#undef dout_prefix
#define dout_prefix *_dout << "daostore(" << path << ") "
#undef dout_context
#define dout_context cct


DaoStore::DaoStore(CephContext *cct, const std::string& path)
  : DaoStore(cct, path, 0) {}

DaoStore::DaoStore(CephContext *cct,
  const std::string& path,
  uint64_t _min_alloc_size)
  : ObjectStore(cct, path)/*,
    finisher(cct, "commit_finisher", "cfin"),
    kv_finalize_thread(this),
    min_alloc_size(_min_alloc_size),
    min_alloc_size_order(ctz(_min_alloc_size)),
    mempool_thread(this)*/
{
  _init_logger();
  cct->_conf.add_observer(this);
  //set_cache_shards(1);
}

DaoStore::~DaoStore()
{
  cct->_conf.remove_observer(this);
  _shutdown_logger();
  ceph_assert(!mounted);
  ceph_assert(fsid_fd < 0);
  ceph_assert(path_fd < 0);
/*  for (auto i : onode_cache_shards) {
    delete i;
  }
  onode_cache_shards.clear();*/
}

const char **DaoStore::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    NULL
  };
  return KEYS;
}

void DaoStore::handle_conf_change(const ConfigProxy& conf,
				   const std::set<std::string> &changed)
{
/*  if (changed.count("bluestore_warn_on_legacy_statfs")) {
    _check_legacy_statfs_alert();
  }*/
}

void DaoStore::_init_logger()
{
  PerfCountersBuilder b(cct, "daostore",
                        l_daostore_first, l_daostore_last);

  logger = b.create_perf_counters();

  b.add_time_avg(l_daostore_submit_lat, "submit_lat",
    "Average submit latency",
    "s_l", PerfCountersBuilder::PRIO_CRITICAL);
  b.add_time_avg(l_daostore_commit_lat, "commit_lat",
    "Average commit latency",
    "c_l", PerfCountersBuilder::PRIO_CRITICAL);
  b.add_u64_counter(l_daostore_onode_hits, "onode_hits",
    "Sum for onode-lookups hit in the cache");
  b.add_u64_counter(l_daostore_onode_misses, "_onode_misses",
    "Sum for onode-lookups missed in the cache");

  cct->get_perfcounters_collection()->add(logger);
}

int DaoStore::_reload_logger()
{
/*  struct store_statfs_t store_statfs;
  int r = statfs(&store_statfs);
  if (r >= 0) {
    logger->set(l_bluestore_allocated, store_statfs.allocated);
    logger->set(l_bluestore_stored, store_statfs.data_stored);
    logger->set(l_bluestore_compressed, store_statfs.data_compressed);
    logger->set(l_bluestore_compressed_allocated, store_statfs.data_compressed_allocated);
    logger->set(l_bluestore_compressed_original, store_statfs.data_compressed_original);
  }
  return r;*/
  return 0;
}

void DaoStore::_shutdown_logger()
{
  cct->get_perfcounters_collection()->remove(logger);
  delete logger;
}

/*void DaoStore::set_cache_shards(unsigned num)
{
  dout(10) << __func__ << " " << num << dendl;
  size_t oold = onode_cache_shards.size();
  size_t bold = buffer_cache_shards.size();
  ceph_assert(num >= oold && num >= bold);
  onode_cache_shards.resize(num);
  buffer_cache_shards.resize(num);
  for (unsigned i = oold; i < num; ++i) {
    onode_cache_shards[i] =
	OnodeCacheShard::create(cct, cct->_conf->bluestore_cache_type,
				 logger);
  }
  for (unsigned i = bold; i < num; ++i) {
    buffer_cache_shards[i] =
	BufferCacheShard::create(cct, cct->_conf->bluestore_cache_type,
				 logger);
  }
}*/

/*int DaoStore::get_block_device_fsid(CephContext* cct, const std::string& path,
				     uuid_d *fsid)
{
  bluestore_bdev_label_t label;
  int r = _read_bdev_label(cct, path, &label);
  if (r < 0)
    return r;
  *fsid = label.osd_uuid;
  return 0;
}*/

int DaoStore::_open_path()
{
  // sanity check(s)
  ceph_assert(path_fd < 0);
  path_fd = TEMP_FAILURE_RETRY(::open(path.c_str(), O_DIRECTORY|O_CLOEXEC));
  if (path_fd < 0) {
    int r = -errno;
    derr << __func__ << " unable to open " << path << ": " << cpp_strerror(r)
	 << dendl;
    return r;
  }
  return 0;
}

void DaoStore::_close_path()
{
  VOID_TEMP_FAILURE_RETRY(::close(path_fd));
  path_fd = -1;
}

int DaoStore::_open_fsid(bool create)
{
  ceph_assert(fsid_fd < 0);
  int flags = O_RDWR|O_CLOEXEC;
  if (create)
    flags |= O_CREAT;
  fsid_fd = ::openat(path_fd, "fsid", flags, 0644);
  if (fsid_fd < 0) {
    int err = -errno;
    derr << __func__ << " " << cpp_strerror(err) << dendl;
    return err;
  }
  return 0;
}

int DaoStore::_read_fsid(uuid_d *uuid)
{
  char fsid_str[40];
  memset(fsid_str, 0, sizeof(fsid_str));
  int ret = safe_read(fsid_fd, fsid_str, sizeof(fsid_str));
  if (ret < 0) {
    derr << __func__ << " failed: " << cpp_strerror(ret) << dendl;
    return ret;
  }
  if (ret > 36)
    fsid_str[36] = 0;
  else
    fsid_str[ret] = 0;
  if (!uuid->parse(fsid_str)) {
    derr << __func__ << " unparsable uuid " << fsid_str << dendl;
    return -EINVAL;
  }
  return 0;
}

int DaoStore::_write_fsid()
{
  int r = ::ftruncate(fsid_fd, 0);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fsid truncate failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  std::string str = stringify(fsid) + "\n";
  r = safe_write(fsid_fd, str.c_str(), str.length());
  if (r < 0) {
    derr << __func__ << " fsid write failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  r = ::fsync(fsid_fd);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fsid fsync failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void DaoStore::_close_fsid()
{
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
}

int DaoStore::_lock_fsid()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  int r = ::fcntl(fsid_fd, F_SETLK, &l);
  if (r < 0) {
    int err = errno;
    derr << __func__ << " failed to lock " << path << "/fsid"
	 << " (is another ceph-osd still running?)"
	 << cpp_strerror(err) << dendl;
    return -err;
  }
  return 0;
}
int DaoStore::_write_daemon_label(CephContext* cct,
				  daostore_bdev_label_t label)
{
  if (fsid.is_zero() || !daos_container.ready()) {
    dout(5) << __func__ << " falling back to local label" << dendl;
    return -1;
  }
  dout(10) << __func__ << " label " << label << dendl;
  bufferlist bl;
  encode(label, bl);
  uint32_t crc = bl.crc32c(-1);
  ceph::encode(crc, bl);
  int r = daos_container.set_key(
    SUPER_DKEY,
    build_full_akey(fsid.to_string(), SUPERBLOCK_LABEL_SUFFIX),
    bl);
  if (r < 0) {
    derr << __func__ << " failed to write label for " << fsid << ": " << cpp_strerror(r)
      << dendl;
    return -1;
  }
  return 0;
}

int DaoStore::_read_daemon_label(CephContext* cct,
			         daostore_bdev_label_t* label)
{
  if (fsid.is_zero() || !daos_container.ready()) {
    dout(5) << __func__ << " falling back to local label" << dendl;
    return -1;
  }
  dout(10) << __func__ << dendl;
  bufferlist bl;
  int r = daos_container.get_key(
    SUPER_DKEY, 
    build_full_akey(fsid.to_string(), SUPERBLOCK_LABEL_SUFFIX),
    &bl);
  if (r < 0) {
    derr << __func__ << " failed to read label for " << fsid << ": " << cpp_strerror(r)
        << dendl;
    return -1;
  }

  uint32_t crc, expected_crc;
  auto p = bl.cbegin();
  try {
    decode(*label, p);
    bufferlist t;
    t.substr_of(bl, 0, p.get_off());
    crc = t.crc32c(-1);
    ceph::decode(expected_crc, p);
  } catch (ceph::buffer::error& e) {
    derr << __func__ << " unable to decode label at offset " << p.get_off()
      << ": " << e.what()
      << dendl;
    return -1;
  }
  if (crc != expected_crc) {
    derr << __func__ << " bad crc on label, expected " << expected_crc
      << " != actual " << crc << dendl;
    return -1;
  }
  dout(10) << __func__ << " got " << *label << dendl;
  return 0;
}


int DaoStore::_open_collections()
{
  if (!coll_set.empty()) {
    // could be opened from another path
    dout(20) << __func__ << " collections are already opened, nothing to do" << dendl;
    return 0;
  }

  dout(10) << __func__ << dendl;
  daos_container.get_every_key(
    SUPER_DKEY,
    build_full_akey(fsid.to_string(), CNODES_SUFFIX),
    [&](const bufferlist& b) {
      daostore_cnode_t cnode;
      auto p = b.cbegin();
      try {
	decode(cnode, p);
	CollectionRef c(new Collection(this, cnode));
	dout(20) << __func__ << " opened " << c
	  << " " << cnode << dendl;
	_osr_attach(c);
	coll_set.emplace(c);
      } catch (ceph::buffer::error& e) {
	derr << __func__ << " failed to decode cnode, content:\n";
	b.hexdump(*_dout);
	*_dout << dendl;
      }
      return true;
    });
  return 0;
}

/*
void DaoStore::_open_statfs()
{
  osd_pools.clear();
  vstatfs.reset();

  bufferlist bl;
  int r = db->get(PREFIX_STAT, BLUESTORE_GLOBAL_STATFS_KEY, &bl);
  if (r >= 0) {
    per_pool_stat_collection = false;
    if (size_t(bl.length()) >= sizeof(vstatfs.values)) {
      auto it = bl.cbegin();
      vstatfs.decode(it);
      dout(10) << __func__ << " store_statfs is found" << dendl;
    } else {
      dout(10) << __func__ << " store_statfs is corrupt, using empty" << dendl;
    }
    _check_legacy_statfs_alert();
  } else {
    per_pool_stat_collection = true;
    dout(10) << __func__ << " per-pool statfs is enabled" << dendl;
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_STAT, KeyValueDB::ITERATOR_NOCACHE);
    for (it->upper_bound(std::string());
	 it->valid();
	 it->next()) {

      uint64_t pool_id;
      int r = get_key_pool_stat(it->key(), &pool_id);
      ceph_assert(r == 0);

      bufferlist bl;
      bl = it->value();
      auto p = bl.cbegin();
      auto& st = osd_pools[pool_id];
      try {
        st.decode(p);
        vstatfs += st;

        dout(30) << __func__ << " pool " << pool_id
		 << " statfs " << st << dendl;
      } catch (ceph::buffer::error& e) {
        derr << __func__ << " failed to decode pool stats, key:"
             << pretty_binary_string(it->key()) << dendl;
      }   
    }
  }
  dout(30) << __func__ << " statfs " << vstatfs << dendl;

}*/

int DaoStore::mkfs()
{
  dout(1) << __func__ << " path " << path << dendl;
  int r;
  uuid_d old_fsid;

  if (cct->_conf->osd_max_object_size > OBJECT_MAX_SIZE) {
    derr << __func__ << " osd_max_object_size "
	 << cct->_conf->osd_max_object_size << " > bluestore max "
	 << OBJECT_MAX_SIZE << dendl;
    return -EINVAL;
  }

  {
    std::string done;
    r = read_meta("mkfs_done", &done);
    if (r == 0) {
      dout(1) << __func__ << " already created" << dendl;
      /*if (cct->_conf->bluestore_fsck_on_mkfs) {
        r = fsck(cct->_conf->bluestore_fsck_on_mkfs_deep);
        if (r < 0) {
          derr << __func__ << " fsck found fatal error: " << cpp_strerror(r)
               << dendl;
          return r;
        }
        if (r > 0) {
          derr << __func__ << " fsck found " << r << " errors" << dendl;
          r = -EIO;
        }
      }
      return r; // idempotent*/
    }
  }

  auto finalization_guard = make_scope_guard([&] {
      if (r < 0) {
	derr << __func__ << " failed, " << cpp_strerror(r) << dendl;
      } else {
	dout(0) << __func__ << " success" << dendl;
      }
    });

  {
    std::string type;
    r = read_meta("type", &type);
    if (r == 0) {
      if (type != "daostore") {
	derr << __func__ << " expected daostore, but type is " << type << dendl;
	r = -EIO;
	return r;
      }
    } else {
      r = write_meta("type", "daostore");
      if (r < 0)
        return r;
    }
  }
  
  r = _open_path();
    if (r < 0)
      return r;
  auto close_path_guard = make_scope_guard([&] {
      _close_path();
    });
  r = _open_fsid(true);
  if (r < 0)
    return r;
  auto close_fsid_guard = make_scope_guard([&] {
      _close_fsid();
    });

  r = _lock_fsid();
  if (r < 0)
    return r;

  r = _read_fsid(&old_fsid);
  if (r < 0 || old_fsid.is_zero()) {
    if (fsid.is_zero()) {
      fsid.generate_random();
      dout(1) << __func__ << " generated fsid " << fsid << dendl;
    } else {
      dout(1) << __func__ << " using provided fsid " << fsid << dendl;
    }
    // we'll write it later.
  } else {
    if (!fsid.is_zero() && fsid != old_fsid) {
      derr << __func__ << " on-disk fsid " << old_fsid
	   << " != provided " << fsid << dendl;
      r = -EINVAL;
      return r;
    }
    fsid = old_fsid;
  }
  r = _open_container(true);
  if (r < 0) {
    return r;
  }
  auto close_container_guard = make_scope_guard([&] {
      _close_container();
    });

    /*if (r == 0 &&
      cct->_conf->bluestore_fsck_on_mkfs) {
    int rc = fsck(cct->_conf->bluestore_fsck_on_mkfs_deep);
    if (rc < 0)
      return rc;
    if (rc > 0) {
      derr << __func__ << " fsck found " << rc << " errors" << dendl;
      r = -EIO;
    }
  }*/

  if (r == 0) {
    // indicate success by writing the 'mkfs_done' file
    r = write_meta("mkfs_done", "yes");
  }
  return r;
}

int DaoStore::mount()
{
  dout(5) << __func__ << dendl;

  if (cct->_conf->osd_max_object_size > OBJECT_MAX_SIZE) {
    derr << __func__ << " osd_max_object_size "
	 << cct->_conf->osd_max_object_size << " > bluestore max "
	 << OBJECT_MAX_SIZE << dendl;
    return -EINVAL;
  }

  int r = 0;
  auto finalization_guard = make_scope_guard([&] {
      if (r < 0) {
	derr << __func__ << " failed, " << cpp_strerror(r) << dendl;
      } else {
	dout(0) << __func__ << " success" << dendl;
      }
    });

  r = _open_container_and_around(false);
  if (r < 0) {
    return r;
  }
  auto close_container_guard = make_scope_guard([&] {
      if (!mounted)
        _close_container_and_around();
    });

  r = _open_collections();
  if (r < 0) {
    return r;
  }
  auto close_collections_guard = make_scope_guard([&] {
    if (!mounted)
      _close_collections();
  });

  r = _reload_logger();
  if (r < 0) {
    return r;
  }

/*  _kv_start();
  auto stop_kv = make_scope_guard([&] {
    if (!mounted) {
      _kv_stop();
    }
  });*/
//  mempool_thread.init();

  mounted = true;
  return 0;
}

int DaoStore::umount()
{
  dout(5) << __func__ << dendl;
  ceph_assert(mounted);
  _osr_drain_all();

  mounted = false;

/*  mempool_thread.shutdown();
    dout(20) << __func__ << " stopping kv thread" << dendl;
    _shutdown_cache();
  dout(20) << __func__ << " closing" << dendl;*/

  _close_container_and_around();

  /*if (cct->_conf->bluestore_fsck_on_umount) {
    int rc = fsck(cct->_conf->bluestore_fsck_on_umount_deep);
    if (rc < 0)
      return rc;
    if (rc > 0) {
      derr << __func__ << " fsck found " << rc << " errors" << dendl;
      return -EIO;
    }
  }*/

  return 0;
}

int DaoStore::write_meta(const std::string& key, const std::string& value)
{
  daostore_bdev_label_t label;
  int r = _read_daemon_label(cct, &label);
  if (r >= 0) {
    label.meta[key] = value;
    r = _write_daemon_label(cct, label);
    ceph_assert(r == 0);
  }
  return ObjectStore::write_meta(key, value);
}

int DaoStore::read_meta(const std::string& key, std::string* value)
{
  daostore_bdev_label_t label;
  int r = _read_daemon_label(cct, &label);
  if (r < 0) {
    return ObjectStore::read_meta(key, value);
  }
  auto i = label.meta.find(key);
  if (i == label.meta.end()) {
    return ObjectStore::read_meta(key, value);
  }
  *value = i->second;
  return 0;
}

/*
* opens DAOS container and other stuff
*/
int DaoStore::_open_container_and_around(bool _read_only)
{
  bool ok = false;
  dout(5) << __func__ << " read_only=" << read_only << dendl;
  {
    std::string type;
    int r = read_meta("type", &type);
    if (r < 0) {
      derr << __func__ << " failed to load os-type: " << cpp_strerror(r)
	<< dendl;
      return r;
    }

    if (type != "daostore") {
      derr << __func__ << " expected daostore, but type is " << type << dendl;
      return -EIO;
    }
  }
  int r = 0;
  auto finalization_guard = make_scope_guard([&] {
      if (r < 0) {
	derr << __func__ << " failed, " << cpp_strerror(r) << dendl;
      } else {
	dout(0) << __func__ << " success" << dendl;
      }
    });

  r = _open_path();
  if (r < 0)
    return r;

  auto close_path_guard = make_scope_guard([&] {
      if (!ok)
        _close_path();
    });

  r = _open_fsid(false);
  if (r < 0)
    return r;

  auto close_fsid_guard = make_scope_guard([&] {
      if (!ok)
        _close_fsid();
    });

  r = _read_fsid(&fsid);
  if (r < 0)
    return r;

  r = _lock_fsid();
  if (r < 0)
    return r;

  r = _open_container(false);
  if (r < 0)
    return r;

  auto close_container_guard = make_scope_guard([&] {
      if (!ok)
        _close_container();
    });

  read_only = _read_only;
  ok = true;
  /*r = _open_super_meta();
  if (r < 0) {
    goto out_db;
  }*/
  return r;
}

void DaoStore::_close_container_and_around()
{
  _close_container();
  _close_fsid();
  _close_path();
}

void DaoStore::collect_metadata(std::map<std::string, std::string> *pm)
{
  dout(10) << __func__ << dendl;
  /*bdev->collect_metadata("bluestore_bdev_", pm);
  if (bluefs) {
    (*pm)["bluefs"] = "1";
    // this value is for backward compatibility only
    (*pm)["bluefs_single_shared_device"] = \
      stringify((int)bluefs_layout.single_shared_device());
    (*pm)["bluefs_dedicated_db"] = \
       stringify((int)bluefs_layout.dedicated_db);
    (*pm)["bluefs_dedicated_wal"] = \
       stringify((int)bluefs_layout.dedicated_wal);
    bluefs->collect_metadata(pm, bluefs_layout.shared_bdev);
  } else {
    (*pm)["bluefs"] = "0";
  }

  // report numa mapping for underlying devices
  int node = -1;
  std::set<int> nodes;
  std::set<std::string> failed;
  int r = get_numa_node(&node, &nodes, &failed);
  if (r >= 0) {
    if (!failed.empty()) {
      (*pm)["objectstore_numa_unknown_devices"] = stringify(failed);
    }
    if (!nodes.empty()) {
      dout(1) << __func__ << " devices span numa nodes " << nodes << dendl;
      (*pm)["objectstore_numa_nodes"] = stringify(nodes);
    }
    if (node >= 0) {
      (*pm)["objectstore_numa_node"] = stringify(node);
    }
  }*/
}

int DaoStore::get_numa_node(
  int *final_node,
  std::set<int> *out_nodes,
  std::set<std::string> *out_failed)
{
  /*int node = -1;
  std::set<std::string> devices;
  get_devices(&devices);
  std::set<int> nodes;
  std::set<std::string> failed;
  for (auto& devname : devices) {
    int n;
    BlkDev bdev(devname);
    int r = bdev.get_numa_node(&n);
    if (r < 0) {
      dout(10) << __func__ << " bdev " << devname << " can't detect numa_node"
	       << dendl;
      failed.insert(devname);
      continue;
    }
    dout(10) << __func__ << " bdev " << devname << " on numa_node " << n
	     << dendl;
    nodes.insert(n);
    if (node < 0) {
      node = n;
    }
  }
  if (node >= 0 && nodes.size() == 1 && failed.empty()) {
    *final_node = node;
  }
  if (out_nodes) {
    *out_nodes = nodes;
  }
  if (out_failed) {
    *out_failed = failed;
  }*/
  return 0;
}

int DaoStore::get_devices(std::set<std::string> *ls)
{
  ls->emplace("daos"); // FIXME
  return 0;
}

int DaoStore::statfs(struct store_statfs_t *buf,
		      osd_alert_list_t* alerts)
{
/*  if (alerts) {
    alerts->clear();
    _log_alerts(*alerts);
  }
  _get_statfs_overall(buf);
  {
    std::lock_guard l(vstatfs_lock);
    buf->allocated = vstatfs.allocated();
    buf->data_stored = vstatfs.stored();
    buf->data_compressed = vstatfs.compressed();
    buf->data_compressed_original = vstatfs.compressed_original();
    buf->data_compressed_allocated = vstatfs.compressed_allocated();
  }*/

  dout(20) << __func__ << " " << *buf << dendl;
  return 0;
}

int DaoStore::pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
			   bool *out_per_pool_omap)
{
  dout(20) << __func__ << " pool " << pool_id<< dendl;

/*  if (!per_pool_stat_collection) {
    dout(20) << __func__ << " not supported in legacy mode " << dendl;
    return -ENOTSUP;
  }
  buf->reset();

  {
    std::lock_guard l(vstatfs_lock);
    osd_pools[pool_id].publish(buf);
  }

  std::string key_prefix;
  _key_encode_u64(pool_id, &key_prefix);
  *out_per_pool_omap = per_pool_omap != OMAP_BULK;
  if (*out_per_pool_omap) {
    auto prefix = per_pool_omap == OMAP_PER_POOL ?
      PREFIX_PERPOOL_OMAP :
      PREFIX_PERPG_OMAP;
    buf->omap_allocated = db->estimate_prefix_size(prefix, key_prefix);
  }*/

  dout(10) << __func__ << *buf << dendl;
  return 0;
}

// ---------------
// cache

DaoStore::CollectionRef DaoStore::_get_collection(const coll_t& cid)
{
  std::shared_lock l(coll_lock);
  auto cp = coll_set.find(cid);
  if (cp == coll_set.end())
    return CollectionRef();
  return *cp;
}
/*
void DaoStore::_queue_reap_collection(CollectionRef& c)
{
  dout(10) << __func__ << " " << c << " " << c->cid << dendl;
  // _reap_collections and this in the same thread,
  // so no need a lock.
  removed_collections.push_back(c);
}

void DaoStore::_reap_collections()
{

  list<CollectionRef> removed_colls;
  {
    // _queue_reap_collection and this in the same thread.
    // So no need a lock.
    if (!removed_collections.empty())
      removed_colls.swap(removed_collections);
    else
      return;
  }

  list<CollectionRef>::iterator p = removed_colls.begin();
  while (p != removed_colls.end()) {
    CollectionRef c = *p;
    dout(10) << __func__ << " " << c << " " << c->cid << dendl;
    if (c->map_any([&](Onode* o) {
	  ceph_assert(!o->exists);
	  if (o->flushing_count.load()) {
	    dout(10) << __func__ << " " << c << " " << c->cid << " " << o->oid
		     << " flush_txns " << o->flushing_count << dendl;
	    return true;
	  }
	  return false;
	})) {
      ++p;
      continue;
    }
    c->clear_onodes();
    p = removed_colls.erase(p);
    dout(10) << __func__ << " " << c << " " << c->cid << " done" << dendl;
  }
  if (removed_colls.empty()) {
    dout(10) << __func__ << " all reaped" << dendl;
  } else {
    removed_collections.splice(removed_collections.begin(), removed_colls);
  }
}*/

/*void DaoStore::_update_cache_logger()
{
  uint64_t num_onodes = 0;
  uint64_t num_pinned_onodes = 0;
  uint64_t num_extents = 0;
  uint64_t num_blobs = 0;
  uint64_t num_buffers = 0;
  uint64_t num_buffer_bytes = 0;
  for (auto c : onode_cache_shards) {
    c->add_stats(&num_onodes, &num_pinned_onodes);
  }
  for (auto c : buffer_cache_shards) {
    c->add_stats(&num_extents, &num_blobs,
                 &num_buffers, &num_buffer_bytes);
  }
  logger->set(l_bluestore_onodes, num_onodes);
  logger->set(l_bluestore_pinned_onodes, num_pinned_onodes);
  logger->set(l_bluestore_extents, num_extents);
  logger->set(l_bluestore_blobs, num_blobs);
  logger->set(l_bluestore_buffers, num_buffers);
  logger->set(l_bluestore_buffer_bytes, num_buffer_bytes);
}*/

// ---------------
// read operations

ObjectStore::CollectionHandle DaoStore::open_collection(const coll_t& cid)
{
  return _get_collection(cid);
}

ObjectStore::CollectionHandle DaoStore::create_new_collection(
  const coll_t& cid)
{
  daostore_cnode_t cnode;
  cnode.cid = cid;
  CollectionRef c(new Collection(this, cnode));
  std::unique_lock l{coll_lock};
  new_coll_set.emplace(c);
  _osr_attach(c.get());
  return c;
}

/*void DaoStore::set_collection_commit_queue(
    const coll_t& cid,
    ContextQueue *commit_queue)
{
  if (commit_queue) {
    std::shared_lock l(coll_lock);
    if (coll_map.count(cid)) {
      coll_map[cid]->commit_queue = commit_queue;
    } else if (new_coll_map.count(cid)) {
      new_coll_map[cid]->commit_queue = commit_queue;
    }
  }
}

bool DaoStore::exists(CollectionHandle &c_, const ghobject_t& oid)
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return false;

  bool r = true;

  {
    std::shared_lock l(c->lock);
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists)
      r = false;
  }
  return r;
}

int DaoStore::stat(
  CollectionHandle &c_,
  const ghobject_t& oid,
  struct stat *st,
  bool allow_eio)
{
  Collection *c = static_cast<Collection *>(c_.get());
  if (!c->exists)
    return -ENOENT;
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;

  {
    std::shared_lock l(c->lock);
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists)
      return -ENOENT;
    st->st_size = o->onode.size;
    st->st_blksize = 4096;
    st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
    st->st_nlink = 1;
  }
  return r;
}
int DaoStore::set_collection_opts(
  CollectionHandle& ch,
  const pool_opts_t& opts)
{
  Collection *c = static_cast<Collection *>(ch.get());
  dout(15) << __func__ << " " << ch->cid << " options " << opts << dendl;
  if (!c->exists())
    return -ENOENT;
  std::unique_lock l{c->lock};
  c->pool_opts = opts;
}

int DaoStore::read(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags)
{
  auto start = mono_clock::now();
  Collection *c = static_cast<Collection *>(c_.get());
  const coll_t &cid = c->get_cid();
  dout(15) << __func__ << " " << cid << " " << oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << dendl;
  if (!c->exists)
    return -ENOENT;

  bl.clear();
  int r;
  {
    std::shared_lock l(c->lock);
    auto start1 = mono_clock::now();
    OnodeRef o = c->get_onode(oid, false);
    log_latency("get_onode@read",
      l_bluestore_read_onode_meta_lat,
      mono_clock::now() - start1,
      cct->_conf->bluestore_log_op_age);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }

    if (offset == length && offset == 0)
      length = o->onode.size;

    r = _do_read(c, o, offset, length, bl, op_flags);
    if (r == -EIO) {
      logger->inc(l_bluestore_read_eio);
    }
  }

 out:
  if (r >= 0 && _debug_data_eio(oid)) {
    r = -EIO;
    derr << __func__ << " " << c->cid << " " << oid << " INJECT EIO" << dendl;
  }
  dout(10) << __func__ << " " << cid << " " << oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << " = " << r << dendl;
  log_latency(__func__,
    l_bluestore_read_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age);
  return r;
}*/

// this stores fiemap into interval_set, other variations
// use it internally
/*int DaoStore::_fiemap(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  interval_set<uint64_t>& destset)
{
  Collection *c = static_cast<Collection *>(c_.get());
  if (!c->exists)
    return -ENOENT;
  {
    std::shared_lock l(c->lock);

    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
      return -ENOENT;
    }
    _dump_onode<30>(cct, *o);

    dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	     << " size 0x" << o->onode.size << std::dec << dendl;

    boost::intrusive::set<Extent>::iterator ep, eend;
    if (offset >= o->onode.size)
      goto out;

    if (offset + length > o->onode.size) {
      length = o->onode.size - offset;
    }

    o->extent_map.fault_range(db, offset, length);
    eend = o->extent_map.extent_map.end();
    ep = o->extent_map.seek_lextent(offset);
    while (length > 0) {
      dout(20) << __func__ << " offset " << offset << dendl;
      if (ep != eend && ep->logical_offset + ep->length <= offset) {
        ++ep;
        continue;
      }

      uint64_t x_len = length;
      if (ep != eend && ep->logical_offset <= offset) {
        uint64_t x_off = offset - ep->logical_offset;
        x_len = std::min(x_len, ep->length - x_off);
        dout(30) << __func__ << " lextent 0x" << std::hex << offset << "~"
	         << x_len << std::dec << " blob " << ep->blob << dendl;
        destset.insert(offset, x_len);
        length -= x_len;
        offset += x_len;
        if (x_off + x_len == ep->length)
	  ++ep;
        continue;
      }
      if (ep != eend &&
	  ep->logical_offset > offset &&
	  ep->logical_offset - offset < x_len) {
        x_len = ep->logical_offset - offset;
      }
      offset += x_len;
      length -= x_len;
    }
  }

 out:
  dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << " size = 0x(" << destset << ")" << std::dec << dendl;
  return 0;
}

int DaoStore::fiemap(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl)
{
  interval_set<uint64_t> m;
  int r = _fiemap(c_, oid, offset, length, m);
  if (r >= 0) {
    encode(m, bl);
  }
  return r;
}

int DaoStore::fiemap(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  map<uint64_t, uint64_t>& destmap)
{
  interval_set<uint64_t> m;
  int r = _fiemap(c_, oid, offset, length, m);
  if (r >= 0) {
    destmap = std::move(m).detach();
  }
  return r;
}
 
int DaoStore::readv(
  CollectionHandle &c_,
  const ghobject_t& oid,
  interval_set<uint64_t>& m,
  bufferlist& bl,
  uint32_t op_flags)
{
  auto start = mono_clock::now();
  Collection *c = static_cast<Collection *>(c_.get());
  const coll_t &cid = c->get_cid();
  dout(15) << __func__ << " " << cid << " " << oid
           << " fiemap " << m
           << dendl;
  if (!c->exists)
    return -ENOENT;

  bl.clear();
  int r;
  {
    std::shared_lock l(c->lock);
    auto start1 = mono_clock::now();
    OnodeRef o = c->get_onode(oid, false);
    log_latency("get_onode@read",
      l_bluestore_read_onode_meta_lat,
      mono_clock::now() - start1,
      cct->_conf->bluestore_log_op_age);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }

    if (m.empty()) {
      r = 0;
      goto out;
    }

    r = _do_readv(c, o, m, bl, op_flags);
    if (r == -EIO) {
      logger->inc(l_bluestore_read_eio);
    }
  }

 out:
  if (r >= 0 && _debug_data_eio(oid)) {
    r = -EIO;
    derr << __func__ << " " << c->cid << " " << oid << " INJECT EIO" << dendl;
  }
  dout(10) << __func__ << " " << cid << " " << oid
           << " fiemap " << m << std::dec
           << " = " << r << dendl;
  log_latency(__func__,
    l_bluestore_read_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age);
  return r;
}

int DaoStore::getattr(
  CollectionHandle &c_,
  const ghobject_t& oid,
  const char *name,
  bufferptr& value)
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->cid << " " << oid << " " << name << dendl;
  if (!c->exists)
    return -ENOENT;

  int r;
  {
    std::shared_lock l(c->lock);
    mempool::bluestore_cache_meta::string k(name);

    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }

    if (!o->onode.attrs.count(k)) {
      r = -ENODATA;
      goto out;
    }
    value = o->onode.attrs[k];
    r = 0;
  }
 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " " << name
	   << " = " << r << dendl;
  return r;
}

int DaoStore::getattrs(
  CollectionHandle &c_,
  const ghobject_t& oid,
  std::map<std::string, bufferptr, less<>>& aset)
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;

  int r;
  {
    std::shared_lock l(c->lock);

    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
      r = -ENOENT;
      goto out;
    }
    for (auto& i : o->onode.attrs) {
      aset.emplace(i.first.c_str(), i.second);
    }
    r = 0;
  }

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " = " << r << dendl;
  return r;
}*/

int DaoStore::list_collections(std::vector<coll_t>& ls)
{
  std::shared_lock l(coll_lock);
  ls.reserve(coll_set.size());
  for (auto p = coll_set.begin();
       p != coll_set.end();
       ++p)
    ls.push_back((*p)->get_cid());
  return 0;
}

bool DaoStore::collection_exists(const coll_t& cid)
{
  std::shared_lock l(coll_lock);
  return coll_set.count(cid);
}

int DaoStore::collection_empty(CollectionHandle& ch, bool *empty)
{
  dout(15) << __func__ << " " << ch->cid << dendl;
  std::vector<ghobject_t> ls;
  ghobject_t next;
  int r = collection_list(ch, ghobject_t(), ghobject_t::get_max(), 1,
			  &ls, &next);
  if (r < 0) {
    derr << __func__ << " collection_list returned: " << cpp_strerror(r)
         << dendl;
    return r;
  }
  *empty = ls.empty();
  dout(10) << __func__ << " " << ch->cid << " = " << (int)(*empty) << dendl;
  return 0;
}

int DaoStore::collection_bits(CollectionHandle& ch)
{
  dout(15) << __func__ << " " << ch->cid << dendl;
  Collection *c = static_cast<Collection*>(ch.get());
  auto cnode = c->get_cnode();
  dout(10) << __func__ << " " << ch->cid << " = " << cnode.bits << dendl;
  return c->get_cnode().bits;
}

int DaoStore::collection_list(
  CollectionHandle &ch, const ghobject_t& start, const ghobject_t& end, int max,
  std::vector<ghobject_t> *ls, ghobject_t *pnext)
{
  dout(15) << __func__ << " " << ch->cid
    << " start " << start << " end " << end << " max " << max << dendl;
  Collection *c = static_cast<Collection *>(ch.get());

  ghobject_t dummy_pnext;
  if (!pnext) {
    pnext = &dummy_pnext;
  }
  int r = 0; //end, max, ls, pnext
  int collected = 0;
  c->collection_list(daos_container, start, [&](const ghobject_t& e) {
    if (collected >= max) {
      return true;
    }
    ++collected;
    ls->push_back(e);
    //FIXME: (maybe use single-shot ls.rbegin?) *pnext = e;
    return collected < max;
  });

  dout(10) << __func__ << " " << ch->cid
    << " start " << start << " end " << end << " max " << max
    << " = " << r << ", ls.size() = " << ls->size()
    << ", next = " << *pnext  
    << " ret = " << cpp_strerror(r)
    << dendl;
  return r;
}

int DaoStore::collection_list_legacy(
  CollectionHandle &c_, const ghobject_t& start, const ghobject_t& end, int max,
  std::vector<ghobject_t> *ls, ghobject_t *pnext)
{
  return -ENOTSUP;
}

/*int DaoStore::omap_get(
  CollectionHandle &c_,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  return _omap_get(c, oid, header, out);
}

int DaoStore::omap_get_header(
  CollectionHandle &c_,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  bool allow_eio ///< [in] don't assert on eio
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  std::shared_lock l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap())
    goto out;
  o->flush();
  {
    string head;
    o->get_omap_header(&head);
    if (db->get(o->get_omap_prefix(), head, header) >= 0) {
      dout(30) << __func__ << "  got header" << dendl;
    } else {
      dout(30) << __func__ << "  no header" << dendl;
    }
  }
 out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
  return -1;
}

int DaoStore::omap_get_keys(
  CollectionHandle &c_,              ///< [in] Collection containing oid
  const ghobject_t &oid, ///< [in] Object containing omap
  set<string> *keys      ///< [out] Keys defined on oid
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  auto start1 = mono_clock::now();
  std::shared_lock l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap())
    goto out;
  o->flush();
  {
    const string& prefix = o->get_omap_prefix();
    KeyValueDB::Iterator it = db->get_iterator(prefix);
    string head, tail;
    o->get_omap_key(string(), &head);
    o->get_omap_tail(&tail);
    it->lower_bound(head);
    while (it->valid()) {
      if (it->key() >= tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      }
      string user_key;
      o->decode_omap_key(it->key(), &user_key);
      dout(20) << __func__ << "  got " << pretty_binary_string(it->key())
	       << " -> " << user_key << dendl;
      keys->insert(user_key);
      it->next();
    }
  }
 out:
  c->store->log_latency(
    __func__,
    l_bluestore_omap_get_keys_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age);

  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

int DaoStore::omap_get_values(
  CollectionHandle &c_,        ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const set<string> &keys,     ///< [in] Keys to get
  map<string, bufferlist> *out ///< [out] Returned keys and values
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  std::shared_lock l(c->lock);
  auto start1 = mono_clock::now();
  int r = 0;
  string final_key;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap()) {
    goto out;
  }
  o->flush();
  {
    const string& prefix = o->get_omap_prefix();
    o->get_omap_key(string(), &final_key);
    size_t base_key_len = final_key.size();
    for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
      final_key.resize(base_key_len); // keep prefix
      final_key += *p;
      bufferlist val;
      if (db->get(prefix, final_key, &val) >= 0) {
	dout(30) << __func__ << "  got " << pretty_binary_string(final_key)
		 << " -> " << *p << dendl;
	out->insert(make_pair(*p, val));
      }
    }
  }
 out:
  c->store->log_latency(
    __func__,
    l_bluestore_omap_get_values_lat,
    mono_clock::now() - start1,
    c->store->cct->_conf->bluestore_log_omap_iterator_age);

  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}
*/
/*#ifdef WITH_SEASTAR
int DaoStore::omap_get_values(
  CollectionHandle &c_,        ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const std::optional<string> &start_after,     ///< [in] Keys to get
  map<string, bufferlist> *output ///< [out] Returned keys and values
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  std::shared_lock l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap()) {
    goto out;
  }
  o->flush();
  {
    ObjectMap::ObjectMapIterator iter = get_omap_iterator(c_, oid);
    if (!iter) {
      r = -ENOENT;
      goto out;
    }
    iter->upper_bound(*start_after);
    for (; iter->valid(); iter->next()) {
      output->insert(make_pair(iter->key(), iter->value()));
    }
  }

out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
          << dendl;
  return r;
}
#endif

int DaoStore::omap_check_keys(
  CollectionHandle &c_,    ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const set<string> &keys, ///< [in] Keys to check
  set<string> *out         ///< [out] Subset of keys defined on oid
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(15) << __func__ << " " << c->get_cid() << " oid " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  std::shared_lock l(c->lock);
  int r = 0;
  string final_key;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.has_omap()) {
    goto out;
  }
  o->flush();
  {
    const string& prefix = o->get_omap_prefix();
    o->get_omap_key(string(), &final_key);
    size_t base_key_len = final_key.size();
    for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
      final_key.resize(base_key_len); // keep prefix
      final_key += *p;
      bufferlist val;
      if (db->get(prefix, final_key, &val) >= 0) {
	dout(30) << __func__ << "  have " << pretty_binary_string(final_key)
		 << " -> " << *p << dendl;
	out->insert(*p);
      } else {
	dout(30) << __func__ << "  miss " << pretty_binary_string(final_key)
		 << " -> " << *p << dendl;
      }
    }
  }
 out:
  dout(10) << __func__ << " " << c->get_cid() << " oid " << oid << " = " << r
	   << dendl;
  return r;
}

ObjectMap::ObjectMapIterator DaoStore::get_omap_iterator(
  CollectionHandle &c_,              ///< [in] collection
  const ghobject_t &oid  ///< [in] object
  )
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;
  if (!c->exists) {
    return ObjectMap::ObjectMapIterator();
  }
  std::shared_lock l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    dout(10) << __func__ << " " << oid << "doesn't exist" <<dendl;
    return ObjectMap::ObjectMapIterator();
  }
  o->flush();
  dout(10) << __func__ << " has_omap = " << (int)o->onode.has_omap() <<dendl;
  KeyValueDB::Iterator it = db->get_iterator(o->get_omap_prefix());
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(c, o, it));
}
*/
// -----------------

/*void DaoStore::_assign_nid(TransContext *txc, OnodeRef o)
{
  if (o->onode.nid) {
    ceph_assert(o->exists);
    return;
  }
  uint64_t nid = ++nid_last;
  dout(20) << __func__ << " " << nid << dendl;
  o->onode.nid = nid;
  txc->last_nid = nid;
  o->exists = true;
}*/

void DaoStore::get_db_statistics(Formatter *f)
{
  //db->get_statistics(f);
}

/*void DaoStore::_txc_update_store_statfs(TransContext *txc)
{
  if (txc->statfs_delta.is_empty())
    return;

  logger->inc(l_bluestore_allocated, txc->statfs_delta.allocated());
  logger->inc(l_bluestore_stored, txc->statfs_delta.stored());
  logger->inc(l_bluestore_compressed, txc->statfs_delta.compressed());
  logger->inc(l_bluestore_compressed_allocated, txc->statfs_delta.compressed_allocated());
  logger->inc(l_bluestore_compressed_original, txc->statfs_delta.compressed_original());

  bufferlist bl;
  txc->statfs_delta.encode(bl);
  if (per_pool_stat_collection) {
    string key;
    get_pool_stat_key(txc->osd_pool_id, &key);
    txc->t->merge(PREFIX_STAT, key, bl);

    std::lock_guard l(vstatfs_lock);
    auto& stats = osd_pools[txc->osd_pool_id];
    stats += txc->statfs_delta;
    
    vstatfs += txc->statfs_delta; //non-persistent in this mode

  } else {
    txc->t->merge(PREFIX_STAT, BLUESTORE_GLOBAL_STATFS_KEY, bl);

    std::lock_guard l(vstatfs_lock);
    vstatfs += txc->statfs_delta;
  } 
  txc->statfs_delta.reset();
}*/

void DaoStore::_osr_attach(CollectionRef c)
{
  // note: caller has coll_lock
  // or doesn't need it (e.g. during collection opening)
  auto q = coll_set.find(c->cid);
  if (q != coll_set.end()) {
    OpSequencerRef osr = (*q)->get_osr();
    c->assign_osr(osr);
    ldout(cct, 10) << __func__ << " " << c->cid
		   << " reusing osr " << osr << " from existing coll "
		   << *q << dendl;
  } else {
    std::lock_guard l(zombie_osr_lock);
    auto p = zombie_osr_set.find(c->cid);
    if (p == zombie_osr_set.end()) {
      OpSequencerRef osr(new OpSequencer(this, next_sequencer_id++, c->cid));
      c->assign_osr(osr);
      ldout(cct, 10) << __func__ << " " << c->cid
		     << " fresh osr " << osr << dendl;
    } else {
      c->assign_osr(*p);
      ldout(cct, 10) << __func__ << " " << c->cid
	<< " resurrecting zombie osr " << *p << dendl;
      (*p)->zombie = false;
      zombie_osr_set.erase(p);
    }
  }
}

void DaoStore::_osr_register_zombie(OpSequencerRef osr)
{
  std::lock_guard l(zombie_osr_lock);
  dout(10) << __func__ << " " << osr << " " << osr->get_cid() << dendl;
  osr->zombie = true;
  auto i = zombie_osr_set.emplace(osr);
  // this is either a new insertion or the same osr is already there
  ceph_assert(i.second || *(i.first) == osr);
}
/*
void DaoStore::_osr_drain_preceding(TransContext *txc)
{
  OpSequencer *osr = txc->osr.get();
  dout(10) << __func__ << " " << txc << " osr " << osr << dendl;
  ++deferred_aggressive; // FIXME: maybe osr-local aggressive flag?
  {
    // submit anything pending
    osr->deferred_lock.lock();
    if (osr->deferred_pending && !osr->deferred_running) {
      _deferred_submit_unlock(osr);
    } else {
      osr->deferred_lock.unlock();
    }
  }
  {
    // wake up any previously finished deferred events
    std::lock_guard l(kv_lock);
    if (!kv_sync_in_progress) {
      kv_sync_in_progress = true;
      kv_cond.notify_one();
    }
  }
  osr->drain_preceding(txc);
  --deferred_aggressive;
  dout(10) << __func__ << " " << osr << " done" << dendl;
}

void DaoStore::_osr_drain(OpSequencer *osr)
{
  dout(10) << __func__ << " " << osr << dendl;
  ++deferred_aggressive; // FIXME: maybe osr-local aggressive flag?
  {
    // submit anything pending
    osr->deferred_lock.lock();
    if (osr->deferred_pending && !osr->deferred_running) {
      _deferred_submit_unlock(osr);
    } else {
      osr->deferred_lock.unlock();
    }
  }
  {
    // wake up any previously finished deferred events
    std::lock_guard l(kv_lock);
    if (!kv_sync_in_progress) {
      kv_sync_in_progress = true;
      kv_cond.notify_one();
    }
  }
  osr->drain();
  --deferred_aggressive;
  dout(10) << __func__ << " " << osr << " done" << dendl;
}
*/
void DaoStore::_osr_drain_all()
{
  dout(10) << __func__ << dendl;

  std::set<OpSequencerRef> s;
  std::vector<OpSequencerRef> zombies;
  {
    std::shared_lock l(coll_lock);
    for (auto& i : coll_set) {
      s.insert(i->get_osr());
    }
  }
  {
    std::lock_guard l(zombie_osr_lock);
    for (auto& i : zombie_osr_set) {
      s.insert(i);
      zombies.push_back(i);
    }
  }
  dout(20) << __func__ << " osr_set " << s << dendl;

/*  {
    // wake up any previously finished deferred events
    std::lock_guard l(kv_lock);
    kv_cond.notify_one();
  }
  {
    std::lock_guard l(kv_finalize_lock);
    kv_finalize_cond.notify_one();
  }*/
  for (auto osr : s) {
    dout(20) << __func__ << " drain " << osr << dendl;
    osr->drain();
  }

  {
    std::lock_guard l(zombie_osr_lock);
    for (auto& osr : zombies) {
      if (zombie_osr_set.erase(osr)) {
	dout(10) << __func__ << " reaping empty zombie osr " << osr << dendl;
	ceph_assert(osr->q.empty());
      } else if (osr->zombie) {
	dout(10) << __func__ << " empty zombie osr " << osr
		 << " already reaped" << dendl;
	ceph_assert(osr->q.empty());
      } else {
	dout(10) << __func__ << " empty zombie osr " << osr
		 << " resurrected" << dendl;
      }
    }
  }

  dout(10) << __func__ << " done" << dendl;
}

/*void DaoStore::_kv_start()
{
  dout(10) << __func__ << dendl;

  finisher.start();
  kv_sync_thread.create("bstore_kv_sync");
  kv_finalize_thread.create("bstore_kv_final");
}

void DaoStore::_kv_stop()
{
  dout(10) << __func__ << dendl;
  {
    std::unique_lock l{kv_lock};
    while (!kv_sync_started) {
      kv_cond.wait(l);
    }
    kv_stop = true;
    kv_cond.notify_all();
  }
  {
    std::unique_lock l{kv_finalize_lock};
    while (!kv_finalize_started) {
      kv_finalize_cond.wait(l);
    }
    kv_finalize_stop = true;
    kv_finalize_cond.notify_all();
  }
  kv_sync_thread.join();
  kv_finalize_thread.join();
  ceph_assert(removed_collections.empty());
  {
    std::lock_guard l(kv_lock);
    kv_stop = false;
  }
  {
    std::lock_guard l(kv_finalize_lock);
    kv_finalize_stop = false;
  }
  dout(10) << __func__ << " stopping finishers" << dendl;
  finisher.wait_for_empty();
  finisher.stop();
  dout(10) << __func__ << " stopped" << dendl;
}*/

/*void DaoStore::_kv_sync_thread()
{
  dout(10) << __func__ << " start" << dendl;
  deque<DeferredBatch*> deferred_stable_queue; ///< deferred ios done + stable
  std::unique_lock l{kv_lock};
  ceph_assert(!kv_sync_started);
  kv_sync_started = true;
  kv_cond.notify_all();

  auto t0 = mono_clock::now();
  timespan twait = ceph::make_timespan(0);
  size_t kv_submitted = 0;

  while (true) {
    auto period = cct->_conf->bluestore_kv_sync_util_logging_s;
    auto observation_period =
      ceph::make_timespan(period);
    auto elapsed = mono_clock::now() - t0;
    if (period && elapsed >= observation_period) {
      dout(5) << __func__ << " utilization: idle "
	      << twait << " of " << elapsed
	      << ", submitted: " << kv_submitted
	      <<dendl;
      t0 = mono_clock::now();
      twait = ceph::make_timespan(0);
      kv_submitted = 0;
    }
    ceph_assert(kv_committing.empty());
    if (kv_queue.empty() &&
	((deferred_done_queue.empty() && deferred_stable_queue.empty()) ||
	 !deferred_aggressive)) {
      if (kv_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      auto t = mono_clock::now();
      kv_sync_in_progress = false;
      kv_cond.wait(l);
      twait += mono_clock::now() - t;

      dout(20) << __func__ << " wake" << dendl;
    } else {
      deque<TransContext*> kv_submitting;
      deque<DeferredBatch*> deferred_done, deferred_stable;
      uint64_t aios = 0, costs = 0;

      dout(20) << __func__ << " committing " << kv_queue.size()
	       << " submitting " << kv_queue_unsubmitted.size()
	       << " deferred done " << deferred_done_queue.size()
	       << " stable " << deferred_stable_queue.size()
	       << dendl;
      kv_committing.swap(kv_queue);
      kv_submitting.swap(kv_queue_unsubmitted);
      deferred_done.swap(deferred_done_queue);
      deferred_stable.swap(deferred_stable_queue);
      aios = kv_ios;
      costs = kv_throttle_costs;
      kv_ios = 0;
      kv_throttle_costs = 0;
      l.unlock();

      dout(30) << __func__ << " committing " << kv_committing << dendl;
      dout(30) << __func__ << " submitting " << kv_submitting << dendl;
      dout(30) << __func__ << " deferred_done " << deferred_done << dendl;
      dout(30) << __func__ << " deferred_stable " << deferred_stable << dendl;

      auto start = mono_clock::now();

      bool force_flush = false;
      // if bluefs is sharing the same device as data (only), then we
      // can rely on the bluefs commit to flush the device and make
      // deferred aios stable.  that means that if we do have done deferred
      // txcs AND we are not on a single device, we need to force a flush.
      if (bluefs && bluefs_layout.single_shared_device()) {
	if (aios) {
	  force_flush = true;
	} else if (kv_committing.empty() && deferred_stable.empty()) {
	  force_flush = true;  // there's nothing else to commit!
	} else if (deferred_aggressive) {
	  force_flush = true;
	}
      } else {
      	if (aios || !deferred_done.empty()) {
	  force_flush = true;
      	} else {
	  dout(20) << __func__ << " skipping flush (no aios, no deferred_done)" << dendl;
      	}
      }

      if (force_flush) {
	dout(20) << __func__ << " num_aios=" << aios
		 << " force_flush=" << (int)force_flush
		 << ", flushing, deferred done->stable" << dendl;
	// flush/barrier on block device
	bdev->flush();

	// if we flush then deferred done are now deferred stable
	deferred_stable.insert(deferred_stable.end(), deferred_done.begin(),
			       deferred_done.end());
	deferred_done.clear();
      }
      auto after_flush = mono_clock::now();

      // we will use one final transaction to force a sync
      KeyValueDB::Transaction synct = db->get_transaction();

      // increase {nid,blobid}_max?  note that this covers both the
      // case where we are approaching the max and the case we passed
      // it.  in either case, we increase the max in the earlier txn
      // we submit.
      uint64_t new_nid_max = 0, new_blobid_max = 0;
      if (nid_last + cct->_conf->bluestore_nid_prealloc/2 > nid_max) {
	KeyValueDB::Transaction t =
	  kv_submitting.empty() ? synct : kv_submitting.front()->t;
	new_nid_max = nid_last + cct->_conf->bluestore_nid_prealloc;
	bufferlist bl;
	encode(new_nid_max, bl);
	t->set(PREFIX_SUPER, "nid_max", bl);
	dout(10) << __func__ << " new_nid_max " << new_nid_max << dendl;
      }
      if (blobid_last + cct->_conf->bluestore_blobid_prealloc/2 > blobid_max) {
	KeyValueDB::Transaction t =
	  kv_submitting.empty() ? synct : kv_submitting.front()->t;
	new_blobid_max = blobid_last + cct->_conf->bluestore_blobid_prealloc;
	bufferlist bl;
	encode(new_blobid_max, bl);
	t->set(PREFIX_SUPER, "blobid_max", bl);
	dout(10) << __func__ << " new_blobid_max " << new_blobid_max << dendl;
      }

      for (auto txc : kv_committing) {
	throttle.log_state_latency(*txc, logger, l_bluestore_state_kv_queued_lat);
	if (txc->get_state() == TransContext::STATE_KV_QUEUED) {
	  ++kv_submitted;
	  _txc_apply_kv(txc, false);
	  --txc->osr->kv_committing_serially;
	} else {
	  ceph_assert(txc->get_state() == TransContext::STATE_KV_SUBMITTED);
	}
	if (txc->had_ios) {
	  --txc->osr->txc_with_unstable_io;
	}
      }

      // release throttle *before* we commit.  this allows new ops
      // to be prepared and enter pipeline while we are waiting on
      // the kv commit sync/flush.  then hopefully on the next
      // iteration there will already be ops awake.  otherwise, we
      // end up going to sleep, and then wake up when the very first
      // transaction is ready for commit.
      throttle.release_kv_throttle(costs);

      // cleanup sync deferred keys
      for (auto b : deferred_stable) {
	for (auto& txc : b->txcs) {
	  bluestore_deferred_transaction_t& wt = *txc.deferred_txn;
	  ceph_assert(wt.released.empty()); // only kraken did this
	  string key;
	  get_deferred_key(wt.seq, &key);
	  synct->rm_single_key(PREFIX_DEFERRED, key);
	}
      }

      // submit synct synchronously (block and wait for it to commit)
      int r = cct->_conf->bluestore_debug_omit_kv_commit ? 0 : db->submit_transaction_sync(synct);
      ceph_assert(r == 0);

      int committing_size = kv_committing.size();
      int deferred_size = deferred_stable.size();

      {
	std::unique_lock m{kv_finalize_lock};
	if (kv_committing_to_finalize.empty()) {
	  kv_committing_to_finalize.swap(kv_committing);
	} else {
	  kv_committing_to_finalize.insert(
	      kv_committing_to_finalize.end(),
	      kv_committing.begin(),
	      kv_committing.end());
	  kv_committing.clear();
	}
	if (deferred_stable_to_finalize.empty()) {
	  deferred_stable_to_finalize.swap(deferred_stable);
	} else {
	  deferred_stable_to_finalize.insert(
	      deferred_stable_to_finalize.end(),
	      deferred_stable.begin(),
	      deferred_stable.end());
	  deferred_stable.clear();
	}
	if (!kv_finalize_in_progress) {
	  kv_finalize_in_progress = true;
	  kv_finalize_cond.notify_one();
	}
      }

      if (new_nid_max) {
	nid_max = new_nid_max;
	dout(10) << __func__ << " nid_max now " << nid_max << dendl;
      }
      if (new_blobid_max) {
	blobid_max = new_blobid_max;
	dout(10) << __func__ << " blobid_max now " << blobid_max << dendl;
      }

      {
	auto finish = mono_clock::now();
	ceph::timespan dur_flush = after_flush - start;
	ceph::timespan dur_kv = finish - after_flush;
	ceph::timespan dur = finish - start;
	dout(20) << __func__ << " committed " << committing_size
	  << " cleaned " << deferred_size
	  << " in " << dur
	  << " (" << dur_flush << " flush + " << dur_kv << " kv commit)"
	  << dendl;
	log_latency("kv_flush",
	  l_bluestore_kv_flush_lat,
	  dur_flush,
	  cct->_conf->bluestore_log_op_age);
	log_latency("kv_commit",
	  l_bluestore_kv_commit_lat,
	  dur_kv,
	  cct->_conf->bluestore_log_op_age);
	log_latency("kv_sync",
	  l_bluestore_kv_sync_lat,
	  dur,
	  cct->_conf->bluestore_log_op_age);
      }

      l.lock();
      // previously deferred "done" are now "stable" by virtue of this
      // commit cycle.
      deferred_stable_queue.swap(deferred_done);
    }
  }
  dout(10) << __func__ << " finish" << dendl;
  kv_sync_started = false;
}
*/
/*void DaoStore::_kv_finalize_thread()
{
  deque<TransContext*> kv_committed;
  deque<DeferredBatch*> deferred_stable;
  dout(10) << __func__ << " start" << dendl;
  std::unique_lock l(kv_finalize_lock);
  ceph_assert(!kv_finalize_started);
  kv_finalize_started = true;
  kv_finalize_cond.notify_all();
  while (true) {
    ceph_assert(kv_committed.empty());
    ceph_assert(deferred_stable.empty());
    if (kv_committing_to_finalize.empty() &&
	deferred_stable_to_finalize.empty()) {
      if (kv_finalize_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      kv_finalize_in_progress = false;
      kv_finalize_cond.wait(l);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      kv_committed.swap(kv_committing_to_finalize);
      deferred_stable.swap(deferred_stable_to_finalize);
      l.unlock();
      dout(20) << __func__ << " kv_committed " << kv_committed << dendl;
      dout(20) << __func__ << " deferred_stable " << deferred_stable << dendl;

      auto start = mono_clock::now();

      while (!kv_committed.empty()) {
	TransContext *txc = kv_committed.front();
	ceph_assert(txc->get_state() == TransContext::STATE_KV_SUBMITTED);
	_txc_state_proc(txc);
	kv_committed.pop_front();
      }

      for (auto b : deferred_stable) {
	auto p = b->txcs.begin();
	while (p != b->txcs.end()) {
	  TransContext *txc = &*p;
	  p = b->txcs.erase(p); // unlink here because
	  _txc_state_proc(txc); // this may destroy txc
	}
	delete b;
      }
      deferred_stable.clear();

      if (!deferred_aggressive) {
	if (deferred_queue_size >= deferred_batch_ops.load() ||
	    throttle.should_submit_deferred()) {
	  deferred_try_submit();
	}
      }

      // this is as good a place as any ...
      _reap_collections();

      logger->set(l_bluestore_fragmentation,
	  (uint64_t)(shared_alloc.a->get_fragmentation() * 1000));

      log_latency("kv_final",
	l_bluestore_kv_final_lat,
	mono_clock::now() - start,
	cct->_conf->bluestore_log_op_age);

      l.lock();
    }
  }
  dout(10) << __func__ << " finish" << dendl;
  kv_finalize_started = false;
}*/

// ---------------------------
// transactions
DaoStore::TransContext* DaoStore::_txc_create(
  CollectionRef c,
  std::list<Context*>* on_commits)
{
  TransContext* txc = new TransContext(cct, c, on_commits);
  dout(20) << __func__ << " osr " << c->get_osr() << " = " << txc
    << " seq " << txc->get_seq() << dendl;
  return txc;
}

int DaoStore::queue_transactions(
  CollectionHandle& ch,
  std::vector<Transaction>& tls,
  TrackedOpRef op,
  ThreadPool::TPHandle *handle)
{
  auto start = mono_clock::now();

  std::list<Context *> on_applied, on_commit, on_applied_sync;
  ObjectStore::Transaction::collect_contexts(
    tls, &on_applied, &on_commit, &on_applied_sync);

  //hopefully we don't have that stuff
  ceph_assert(on_applied_sync.empty());
  ceph_assert(on_applied.empty());

  Collection *c = static_cast<Collection*>(ch.get());
  dout(10) << __func__ << " ch " << c << " " << c->cid << dendl;

  // prepare
  TransContext *txc = _txc_create(c, &on_commit);

  for (std::vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
    //txc->bytes += (*p).get_num_bytes();
    _txc_add_transaction(txc, &(*p));
  }
/*  _txc_calc_cost(txc);
  _txc_write_nodes(txc, txc->t);

  _txc_finalize_transaction(txc, txc->t);
  logger->inc(l_daostore_txc);
  */

  // execute (start)
  _txc_state_proc(txc);

  log_latency("submit_transact",
    l_daostore_submit_lat,
    mono_clock::now() - start,
    cct->_conf->bluestore_log_op_age); //FIXME - update config param name
  return 0;
}

void DaoStore::_txc_add_transaction(TransContext *txc, Transaction *t)
{
  Transaction::iterator i = t->begin();

  _dump_transaction<30>(cct, t);

  std::vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (std::vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
       ++p, ++j) {
    cvec[j] = _get_collection(*p);
  }
  
  for (int pos = 0; i.have_op(); ++pos) {
    Transaction::Op *op = i.decode_op();
    int r = 0;

    // no coll or obj
    if (op->op == Transaction::OP_NOP)
      continue;


    // collection operations
    CollectionRef &c = cvec[op->cid];

    // initialize osd_pool_id and do a smoke test that all collections belong
    // to the same pool
    spg_t pgid;
    if (!!c ? c->cid.is_pg(&pgid) : false) {
      ceph_assert(txc->osd_pool_id == META_POOL_ID ||
                  txc->osd_pool_id == pgid.pool());
      txc->osd_pool_id = pgid.pool();
    }

    switch (op->op) {
    case Transaction::OP_RMCOLL:
      {
        const coll_t &cid = i.get_cid(op->cid);
	r = _remove_collection(txc, cid, &c);
	if (!r)
	  continue;
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	ceph_assert(!c);
	const coll_t &cid = i.get_cid(op->cid);
	r = _create_collection(txc, cid, op->split_bits, &c);
	if (!r) {
	  ceph_assert(r == -EEXIST);
	  continue;
	}
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_SPLIT_COLLECTION2:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_MERGE_COLLECTION:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_HINT:
      {
        uint32_t type = op->hint;
        bufferlist hint;
        i.decode_bl(hint);
        auto hiter = hint.cbegin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ceph::decode(pg_num, hiter);
	  ceph::decode(num_objs, hiter);
          dout(10) << __func__ << " collection hint objects is a no-op, "
		   << " pg_num " << pg_num << " num_objects " << num_objs
		   << dendl;
        } else {
          // Ignore the hint
          dout(10) << __func__ << " unknown collection hint " << type << dendl;
        }
	continue;
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RENAME:
      ceph_abort_msg("not implemented");
      break;
    }
    if (r < 0) {
      derr << __func__ << " error " << cpp_strerror(r)
           << " not handled on operation " << op->op
           << " (op " << pos << ", counting from 0)" << dendl;
      _dump_transaction<0>(cct, t);
      ceph_abort_msg("unexpected error");
    }

    // these operations implicity create the object
    /*
    vector<OnodeRef> ovec(i.objects.size());

    bool create = false;
    if (op->op == Transaction::OP_TOUCH ||
	op->op == Transaction::OP_CREATE ||
	op->op == Transaction::OP_WRITE ||
	op->op == Transaction::OP_ZERO) {
      create = true;
    }

    // object operations
    std::unique_lock l(c->lock);
    OnodeRef &o = ovec[op->oid];
    if (!o) {
      ghobject_t oid = i.get_oid(op->oid);
      o = c->get_onode(oid, create, op->op == Transaction::OP_CREATE);
    }
    if (!create && (!o || !o->exists)) {
      dout(10) << __func__ << " op " << op->op << " got ENOENT on "
	       << i.get_oid(op->oid) << dendl;
      r = -ENOENT;
      goto endop;
    }

    switch (op->op) {
    case Transaction::OP_CREATE:
    case Transaction::OP_TOUCH:
      r = _touch(txc, c, o);
      break;

    case Transaction::OP_WRITE:
      {
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
	r = _write(txc, c, o, off, len, bl, fadvise_flags);
      }
      break;

    case Transaction::OP_ZERO:
      {
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _zero(txc, c, o, off, len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        uint64_t off = op->off;
	r = _truncate(txc, c, o, off);
      }
      break;

    case Transaction::OP_REMOVE:
      {
	r = _remove(txc, c, o);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        string name = i.decode_string();
        bufferptr bp;
        i.decode_bp(bp);
	r = _setattr(txc, c, o, name, bp);
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
	r = _setattrs(txc, c, o, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
	string name = i.decode_string();
	r = _rmattr(txc, c, o, name);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	r = _rmattrs(txc, c, o);
      }
      break;

    case Transaction::OP_CLONE:
      {
	OnodeRef& no = ovec[op->dest_oid];
	if (!no) {
          const ghobject_t& noid = i.get_oid(op->dest_oid);
	  no = c->get_onode(noid, true);
	}
	r = _clone(txc, c, o, no);
      }
      break;

    case Transaction::OP_CLONERANGE:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_CLONERANGE2:
      {
	OnodeRef& no = ovec[op->dest_oid];
	if (!no) {
	  const ghobject_t& noid = i.get_oid(op->dest_oid);
	  no = c->get_onode(noid, true);
	}
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
	r = _clone_range(txc, c, o, no, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_COLL_ADD:
      ceph_abort_msg("not implemented");
      break;

    case Transaction::OP_COLL_REMOVE:
      ceph_abort_msg("not implemented");
      break;

    case Transaction::OP_COLL_MOVE:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
    case Transaction::OP_TRY_RENAME:
      {
	ceph_assert(op->cid == op->dest_cid);
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef& no = ovec[op->dest_oid];
	if (!no) {
	  no = c->get_onode(noid, false);
	}
	r = _rename(txc, c, o, no, noid);
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
	r = _omap_clear(txc, c, o);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
	bufferlist aset_bl;
        i.decode_attrset_bl(&aset_bl);
	r = _omap_setkeys(txc, c, o, aset_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
	bufferlist keys_bl;
        i.decode_keyset_bl(&keys_bl);
	r = _omap_rmkeys(txc, c, o, keys_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
	r = _omap_rmkey_range(txc, c, o, first, last);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        bufferlist bl;
        i.decode_bl(bl);
	r = _omap_setheader(txc, c, o, bl);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
	r = _set_alloc_hint(txc, c, o,
			    op->expected_object_size,
			    op->expected_write_size,
			    op->hint);
      }
      break;

    default:
      derr << __func__ << " bad op " << op->op << dendl;
      ceph_abort();
    }

  endop:
    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD ||
			    op->op == Transaction::OP_SETATTR ||
			    op->op == Transaction::OP_SETATTRS ||
			    op->op == Transaction::OP_RMATTR ||
			    op->op == Transaction::OP_OMAP_SETKEYS ||
			    op->op == Transaction::OP_OMAP_RMKEYS ||
			    op->op == Transaction::OP_OMAP_RMKEYRANGE ||
			    op->op == Transaction::OP_OMAP_SETHEADER))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC from bluestore, misconfigured cluster";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	}

        derr << __func__ << " error " << cpp_strerror(r)
             << " not handled on operation " << op->op
             << " (op " << pos << ", counting from 0)"
             << dendl;
        derr << msg << dendl;
        _dump_transaction<0>(cct, t);
	ceph_abort_msg("unexpected error");
      }
    }*/
  }
}

void DaoStore::_txc_state_proc(TransContext* txc)
{
  dout(10) << __func__ << " txc " << txc
    << " " << txc->get_state_name() << dendl;
  int r;
  switch (txc->get_state()) {
  case TransContext::STATE_PREPARE:
    txc->set_state(TransContext::STATE_DAOS_SUBMITTED);
    r = daos_container.submit(txc->t);
    if (r < 0) {
	derr << __func__ << " failed to submit transaction:" << txc << " :" << cpp_strerror(r) << dendl;
	ceph_abort_msg("txc submission failed");
    }
    break;
  case TransContext::STATE_DAOS_SUBMITTED:
    _txc_finish(txc);
    break;
  default:
    derr << __func__ << " unexpected txc " << txc
      << " state " << txc->get_state_name() << dendl;
    ceph_abort_msg("unexpected txc state");
    break;
  }
}

void DaoStore::_txc_finish(TransContext* txc)
{
  dout(20) << __func__ << " " << txc << dendl;
  ceph_assert(txc->get_state() == TransContext::STATE_DAOS_SUBMITTED);

  while (!txc->removed_collections.empty()) {
    _queue_reap_collection(txc->removed_collections.front());
    txc->removed_collections.pop_front();
  }

  OpSequencerRef osr = txc->get_osr();
  bool empty = false;
  {
    std::lock_guard l(osr->qlock);
    txc->set_state(TransContext::STATE_DONE);
    
    //FIXME: do we really need that loop, isn't single txc handling sufficient? We don't reorder operations as bluestore does due to async/deferred I/O
    while (!osr->q.empty()) {
      TransContext* txc = &osr->q.front();
      dout(20) << __func__ << "  txc " << txc << " " << txc->get_state_name()
	<< dendl;
      if (txc->get_state() != TransContext::STATE_DONE) {
	break;
      }
      txc->notify_on_commit();
      log_latency_fn(
	__func__,
	l_daostore_commit_lat,
	mono_clock::now() - txc->get_start(),
	cct->_conf->bluestore_log_op_age, //FIXME
	[&](auto lat) {
	  return ", txc = " + stringify(txc);
	}
      );
      osr->q.pop_front();
      delete txc;
    }

    if (osr->q.empty()) {
      dout(20) << __func__ << " osr " << osr << " q now empty" << dendl;
      osr->qcond.notify_all(); // for drain?
      empty = true;
    }
  }

  if (empty && osr->zombie) {
    std::lock_guard l(zombie_osr_lock);
    if (zombie_osr_set.erase(osr)) {
      dout(10) << __func__ << " reaping empty zombie osr " << osr << dendl;
    } else {
      dout(10) << __func__ << " empty zombie osr " << osr << " already reaped"
	       << dendl;
    }
  }
}

// -----------------
// write operations

/*void DaoStore::_pad_zeros(
  bufferlist *bl, uint64_t *offset,
  uint64_t chunk_size)
{
  auto length = bl->length();
  dout(30) << __func__ << " 0x" << std::hex << *offset << "~" << length
	   << " chunk_size 0x" << chunk_size << std::dec << dendl;
  dout(40) << "before:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
  // front
  size_t front_pad = *offset % chunk_size;
  size_t back_pad = 0;
  size_t pad_count = 0;
  if (front_pad) {
    size_t front_copy = std::min<uint64_t>(chunk_size - front_pad, length);
    bufferptr z = ceph::buffer::create_small_page_aligned(chunk_size);
    z.zero(0, front_pad, false);
    pad_count += front_pad;
    bl->begin().copy(front_copy, z.c_str() + front_pad);
    if (front_copy + front_pad < chunk_size) {
      back_pad = chunk_size - (length + front_pad);
      z.zero(front_pad + length, back_pad, false);
      pad_count += back_pad;
    }
    bufferlist old, t;
    old.swap(*bl);
    t.substr_of(old, front_copy, length - front_copy);
    bl->append(z);
    bl->claim_append(t);
    *offset -= front_pad;
    length += pad_count;
  }

  // back
  uint64_t end = *offset + length;
  unsigned back_copy = end % chunk_size;
  if (back_copy) {
    ceph_assert(back_pad == 0);
    back_pad = chunk_size - back_copy;
    ceph_assert(back_copy <= length);
    bufferptr tail(chunk_size);
    bl->begin(length - back_copy).copy(back_copy, tail.c_str());
    tail.zero(back_copy, back_pad, false);
    bufferlist old;
    old.swap(*bl);
    bl->substr_of(old, 0, length - back_copy);
    bl->append(tail);
    length += back_pad;
    pad_count += back_pad;
  }
  dout(20) << __func__ << " pad 0x" << std::hex << front_pad << " + 0x"
	   << back_pad << " on front/back, now 0x" << *offset << "~"
	   << length << std::dec << dendl;
  dout(40) << "after:\n";
  bl->hexdump(*_dout);
  *_dout << dendl;
  if (pad_count)
    logger->inc(l_bluestore_write_pad_bytes, pad_count);
  ceph_assert(bl->length() == length);
}*/
/*
int DaoStore::_write(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& o,
		      uint64_t offset, size_t length,
		      bufferlist& bl,
		      uint32_t fadvise_flags)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << dendl;
  int r = 0;
  if (offset + length >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
  } else {
    _assign_nid(txc, o);
    r = _do_write(txc, c, o, offset, length, bl, fadvise_flags);
    txc->write_onode(o);
  }
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << " = " << r << dendl;
  return r;
}

int DaoStore::_zero(TransContext *txc,
		     CollectionRef& c,
		     OnodeRef& o,
		     uint64_t offset, size_t length)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << dendl;
  int r = 0;
  if (offset + length >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
  } else {
    _assign_nid(txc, o);
    r = _do_zero(txc, c, o, offset, length);
  }
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << " = " << r << dendl;
  return r;
}

int DaoStore::_do_zero(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o,
			uint64_t offset, size_t length)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << dendl;
  int r = 0;

  _dump_onode<30>(cct, *o);

  WriteContext wctx;
  o->extent_map.fault_range(db, offset, length);
  o->extent_map.punch_hole(c, offset, length, &wctx.old_extents);
  o->extent_map.dirty_range(offset, length);
  _wctx_finish(txc, c, o, &wctx);

  if (length > 0 && offset + length > o->onode.size) {
    o->onode.size = offset + length;
    dout(20) << __func__ << " extending size to " << offset + length
	     << dendl;
  }
  txc->write_onode(o);

  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << "~" << length << std::dec
	   << " = " << r << dendl;
  return r;
}

void DaoStore::_do_truncate(
  TransContext *txc, CollectionRef& c, OnodeRef o, uint64_t offset,
  set<SharedBlob*> *maybe_unshared_blobs)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << std::dec << dendl;

  _dump_onode<30>(cct, *o);

  if (offset == o->onode.size)
    return;

  WriteContext wctx;
  if (offset < o->onode.size) {
    uint64_t length = o->onode.size - offset;
    o->extent_map.fault_range(db, offset, length);
    o->extent_map.punch_hole(c, offset, length, &wctx.old_extents);
    o->extent_map.dirty_range(offset, length);

#ifdef HAVE_LIBZBD
    if (bdev->is_smr()) {
      // On zoned devices, we currently support only removing an object or
      // truncating it to zero size, both of which fall through this code path.
      ceph_assert(offset == 0 && !wctx.old_extents.empty());
      int64_t ondisk_offset = wctx.old_extents.begin()->r.begin()->offset;
      txc->zoned_note_truncated_object(o, ondisk_offset);
    }
#endif
    
    _wctx_finish(txc, c, o, &wctx, maybe_unshared_blobs);

    // if we have shards past EOF, ask for a reshard
    if (!o->onode.extent_map_shards.empty() &&
	o->onode.extent_map_shards.back().offset >= offset) {
      dout(10) << __func__ << "  request reshard past EOF" << dendl;
      if (offset) {
	o->extent_map.request_reshard(offset - 1, offset + length);
      } else {
	o->extent_map.request_reshard(0, length);
      }
    }
  }

  o->onode.size = offset;

  txc->write_onode(o);
}

int DaoStore::_truncate(TransContext *txc,
			 CollectionRef& c,
			 OnodeRef& o,
			 uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << std::dec
	   << dendl;
  int r = 0;
  if (offset >= OBJECT_MAX_SIZE) {
    r = -E2BIG;
  } else {
    _do_truncate(txc, c, o, offset);
  }
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " 0x" << std::hex << offset << std::dec
	   << " = " << r << dendl;
  return r;
}*/

/*int DaoStore::_remove(TransContext *txc,
		       CollectionRef& c,
		       OnodeRef &o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " onode " << o.get()
	   << " txc "<< txc << dendl;

  auto start_time = mono_clock::now();
  int r = _do_remove(txc, c, o);
  log_latency_fn(
    __func__,
    l_bluestore_remove_lat,
    mono_clock::now() - start_time,
    cct->_conf->bluestore_log_op_age,
    [&](const ceph::timespan& lat) {
      ostringstream ostr;
      ostr << ", lat = " << timespan_str(lat)
        << " cid =" << c->cid
        << " oid =" << o->oid;
      return ostr.str();
    }
  );

  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int DaoStore::_setattr(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o,
			const string& name,
			bufferptr& val)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << dendl;
  int r = 0;
  if (val.is_partial()) {
    auto& b = o->onode.attrs[name.c_str()] = bufferptr(val.c_str(),
						       val.length());
    b.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
  } else {
    auto& b = o->onode.attrs[name.c_str()] = val;
    b.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
  }
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << " = " << r << dendl;
  return r;
}

int DaoStore::_setattrs(TransContext *txc,
			 CollectionRef& c,
			 OnodeRef& o,
			 const map<string,bufferptr>& aset)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << aset.size() << " keys"
	   << dendl;
  int r = 0;
  for (map<string,bufferptr>::const_iterator p = aset.begin();
       p != aset.end(); ++p) {
    if (p->second.is_partial()) {
      auto& b = o->onode.attrs[p->first.c_str()] =
	bufferptr(p->second.c_str(), p->second.length());
      b.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
    } else {
      auto& b = o->onode.attrs[p->first.c_str()] = p->second;
      b.reassign_to_mempool(mempool::mempool_bluestore_cache_meta);
    }
  }
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << aset.size() << " keys"
	   << " = " << r << dendl;
  return r;
}


int DaoStore::_rmattr(TransContext *txc,
		       CollectionRef& c,
		       OnodeRef& o,
		       const string& name)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << dendl;
  int r = 0;
  auto it = o->onode.attrs.find(name.c_str());
  if (it == o->onode.attrs.end())
    goto out;

  o->onode.attrs.erase(it);
  txc->write_onode(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " " << name << " = " << r << dendl;
  return r;
}

int DaoStore::_rmattrs(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;

  if (o->onode.attrs.empty())
    goto out;

  o->onode.attrs.clear();
  txc->write_onode(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

void DaoStore::_do_omap_clear(TransContext *txc, OnodeRef& o)
{
  const string& omap_prefix = o->get_omap_prefix();
  string prefix, tail;
  o->get_omap_header(&prefix);
  o->get_omap_tail(&tail);
  txc->t->rm_range_keys(omap_prefix, prefix, tail);
  txc->t->rmkey(omap_prefix, tail);
  dout(20) << __func__ << " remove range start: "
           << pretty_binary_string(prefix) << " end: "
           << pretty_binary_string(tail) << dendl;
}

int DaoStore::_omap_clear(TransContext *txc,
			   CollectionRef& c,
			   OnodeRef& o)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;
  if (o->onode.has_omap()) {
    o->flush();
    _do_omap_clear(txc, o);
    o->onode.clear_omap_flag();
    txc->write_onode(o);
  }
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int DaoStore::_omap_setkeys(TransContext *txc,
			     CollectionRef& c,
			     OnodeRef& o,
			     bufferlist &bl)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r;
  auto p = bl.cbegin();
  __u32 num;
  if (!o->onode.has_omap()) {
    if (o->oid.is_pgmeta()) {
      o->onode.set_omap_flags_pgmeta();
    } else {
      o->onode.set_omap_flags();
    }
    txc->write_onode(o);

    const string& prefix = o->get_omap_prefix();
    string key_tail;
    bufferlist tail;
    o->get_omap_tail(&key_tail);
    txc->t->set(prefix, key_tail, tail);
  } else {
    txc->note_modified_object(o);
  }
  const string& prefix = o->get_omap_prefix();
  string final_key;
  o->get_omap_key(string(), &final_key);
  size_t base_key_len = final_key.size();
  decode(num, p);
  while (num--) {
    string key;
    bufferlist value;
    decode(key, p);
    decode(value, p);
    final_key.resize(base_key_len); // keep prefix
    final_key += key;
    dout(20) << __func__ << "  " << pretty_binary_string(final_key)
	     << " <- " << key << dendl;
    txc->t->set(prefix, final_key, value);
  }
  r = 0;
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int DaoStore::_omap_setheader(TransContext *txc,
			       CollectionRef& c,
			       OnodeRef &o,
			       bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r;
  string key;
  if (!o->onode.has_omap()) {
    if (o->oid.is_pgmeta()) {
      o->onode.set_omap_flags_pgmeta();
    } else {
      o->onode.set_omap_flags();
    }
    txc->write_onode(o);

    const string& prefix = o->get_omap_prefix();
    string key_tail;
    bufferlist tail;
    o->get_omap_tail(&key_tail);
    txc->t->set(prefix, key_tail, tail);
  } else {
    txc->note_modified_object(o);
  }
  const string& prefix = o->get_omap_prefix();
  o->get_omap_header(&key);
  txc->t->set(prefix, key, bl);
  r = 0;
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int DaoStore::_omap_rmkeys(TransContext *txc,
			    CollectionRef& c,
			    OnodeRef& o,
			    bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  int r = 0;
  auto p = bl.cbegin();
  __u32 num;
  string final_key;

  if (!o->onode.has_omap()) {
    goto out;
  }
  {
    const string& prefix = o->get_omap_prefix();
    o->get_omap_key(string(), &final_key);
    size_t base_key_len = final_key.size();
    decode(num, p);
    while (num--) {
      string key;
      decode(key, p);
      final_key.resize(base_key_len); // keep prefix
      final_key += key;
      dout(20) << __func__ << "  rm " << pretty_binary_string(final_key)
	       << " <- " << key << dendl;
      txc->t->rmkey(prefix, final_key);
    }
  }
  txc->note_modified_object(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int DaoStore::_omap_rmkey_range(TransContext *txc,
				 CollectionRef& c,
				 OnodeRef& o,
				 const string& first, const string& last)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
  string key_first, key_last;
  int r = 0;
  if (!o->onode.has_omap()) {
    goto out;
  }
  {
    const string& prefix = o->get_omap_prefix();
    o->flush();
    o->get_omap_key(first, &key_first);
    o->get_omap_key(last, &key_last);
    txc->t->rm_range_keys(prefix, key_first, key_last);
    dout(20) << __func__ << " remove range start: "
             << pretty_binary_string(key_first) << " end: "
             << pretty_binary_string(key_last) << dendl;
  }
  txc->note_modified_object(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r << dendl;
  return r;
}

int DaoStore::_set_alloc_hint(
  TransContext *txc,
  CollectionRef& c,
  OnodeRef& o,
  uint64_t expected_object_size,
  uint64_t expected_write_size,
  uint32_t flags)
{
  dout(15) << __func__ << " " << c->cid << " " << o->oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << " flags " << ceph_osd_alloc_hint_flag_string(flags)
	   << dendl;
  int r = 0;
  o->onode.expected_object_size = expected_object_size;
  o->onode.expected_write_size = expected_write_size;
  o->onode.alloc_hint_flags = flags;
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << o->oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << " flags " << ceph_osd_alloc_hint_flag_string(flags)
	   << " = " << r << dendl;
  return r;
}*/

/*int DaoStore::_rename(TransContext *txc,
		       CollectionRef& c,
		       OnodeRef& oldo,
		       OnodeRef& newo,
		       const ghobject_t& new_oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
	   << new_oid << dendl;
  int r;
  ghobject_t old_oid = oldo->oid;
  mempool::bluestore_cache_meta::string new_okey;

  if (newo) {
    if (newo->exists) {
      r = -EEXIST;
      goto out;
    }
    ceph_assert(txc->onodes.count(newo) == 0);
  }

  txc->t->rmkey(PREFIX_OBJ, oldo->key.c_str(), oldo->key.size());

  // rewrite shards
  {
    oldo->extent_map.fault_range(db, 0, oldo->onode.size);
    get_object_key(cct, new_oid, &new_okey);
    string key;
    for (auto &s : oldo->extent_map.shards) {
      generate_extent_shard_key_and_apply(oldo->key, s.shard_info->offset, &key,
        [&](const string& final_key) {
          txc->t->rmkey(PREFIX_OBJ, final_key);
        }
      );
      s.dirty = true;
    }
  }

  newo = oldo;
  txc->write_onode(newo);

  // this adjusts oldo->{oid,key}, and reset oldo to a fresh empty
  // Onode in the old slot
  c->onode_map.rename(oldo, old_oid, new_oid, new_okey);
  r = 0;

  // hold a ref to new Onode in old name position, to ensure we don't drop
  // it from the cache before this txc commits (or else someone may come along
  // and read newo's metadata via the old name).
  txc->note_modified_object(oldo);

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " = " << r << dendl;
  return r;
}
*/
// collections

int DaoStore::_create_collection(
  TransContext *txc,
  const coll_t &cid,
  unsigned bits,
  CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << " bits " << bits << dendl;
  int r;

  {
    if (*c) {
      r = -EEXIST;
      goto out;
    }
    std::unique_lock l(coll_lock);
    auto p = new_coll_set.find(cid);
    ceph_assert(p != new_coll_set.end());
    *c = *p;
    (*c)->set_bits(bits);
    coll_set.emplace(*c);
    new_coll_set.erase(p);

    bufferlist bl;
    daostore_cnode_t cnode = (*c)->get_cnode();
    encode(cnode, bl);
    txc->t.set_key(SUPER_DKEY,
      build_full_akey(fsid.to_string(), CNODES_SUFFIX),
      bl);
  }
  r = 0;
 
 out:
  dout(10) << __func__ << " " << cid << " bits " << bits << " = " << r << dendl;
  return r;
}

int DaoStore::_remove_collection(TransContext *txc, const coll_t &cid,
				  CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << dendl;
  int r = 0;

  (*c)->get_osr()->flush_all_but_last();
  {
    std::unique_lock l(coll_lock);
    if (!*c) {
      r = -ENOENT;
      goto out;
    }
    /*size_t nonexistent_count = 0;
    ceph_assert((*c)->exists);
    if ((*c)->onode_map.map_any([&](Onode* o) {
      if (o->exists) {
        dout(1) << __func__ << " " << o->oid << " " << o
	        << " exists in onode_map" << dendl;
          return true;
      }
      ++nonexistent_count;
      return false;
    })) {
      r = -ENOTEMPTY;
      goto out;
    }
    vector<ghobject_t> ls;
    ghobject_t next;
    // Enumerate onodes in db, up to nonexistent_count + 1
    // then check if all of them are marked as non-existent.
    // Bypass the check if (next != ghobject_t::get_max())
    r = _collection_list(c->get(), ghobject_t(), ghobject_t::get_max(),
                         nonexistent_count + 1, false, &ls, &next);*/
    if (r >= 0) {
      // If true mean collecton has more objects than nonexistent_count,
      // so bypass check.
      /*bool exists = (!next.is_max());
      for (auto it = ls.begin(); !exists && it < ls.end(); ++it) {
        dout(10) << __func__ << " oid " << *it << dendl;
        auto onode = (*c)->onode_map.lookup(*it);
        exists = !onode || onode->exists;
        if (exists) {
          dout(1) << __func__ << " " << *it
	  << " exists in db, "
	  << (!onode ? "not present in ram" : "present in ram")
	  << dendl;
        }
      }
      if (!exists)*/ {
        _do_remove_collection(txc, c);
        r = 0;
      } /*else {
        dout(10) << __func__ << " " << cid
                 << " is non-empty" << dendl;
	r = -ENOTEMPTY;
      }*/
    }
  }
out:
  dout(10) << __func__ << " " << cid << " = " << r << dendl;
  return r;
}

void DaoStore::_do_remove_collection(TransContext *txc,
				      CollectionRef *c)
{
  coll_set.erase(*c);
  txc->removed_collections.push_back(*c);
  (*c)->set_exists(false);
  _osr_register_zombie((*c)->get_osr());
  txc->t.rm_key(SUPER_DKEY,
    build_full_akey(fsid.to_string(), CNODES_SUFFIX));
  c->reset();
}


void DaoStore::log_latency(
  const char* name,
  int idx,
  const ceph::timespan& l,
  double lat_threshold,
  const char* info) const
{
  logger->tinc(idx, l);
  if (lat_threshold > 0.0 &&
      l >= make_timespan(lat_threshold)) {
    dout(0) << __func__ << " slow operation observed for " << name
      << ", latency = " << l
      << info
      << dendl;
  }
}

void DaoStore::log_latency_fn(
  const char* name,
  int idx,
  const ceph::timespan& l,
  double lat_threshold,
  std::function<std::string (const ceph::timespan& lat)> fn) const
{
  logger->tinc(idx, l);
  if (lat_threshold > 0.0 &&
      l >= make_timespan(lat_threshold)) {
    dout(0) << __func__ << " slow operation observed for " << name
      << ", latency = " << l
      << fn(l)
      << dendl;
  }
}

/*void DaoStore::_shutdown_cache()
{
  dout(10) << __func__ << dendl;
  for (auto i : buffer_cache_shards) {
    i->flush();
    ceph_assert(i->empty());
  }
  for (auto& p : coll_map) {
    p.second->onode_map.clear();
    if (!p.second->shared_blob_set.empty()) {
      derr << __func__ << " stray shared blobs on " << p.first << dendl;
      p.second->shared_blob_set.dump<0>(cct);
    }
    ceph_assert(p.second->onode_map.empty());
    ceph_assert(p.second->shared_blob_set.empty());
  }
  coll_map.clear();
  for (auto i : onode_cache_shards) {
    ceph_assert(i->empty());
  }
}

// For external caller.
// We use a best-effort policy instead, e.g.,
// we don't care if there are still some pinned onodes/data in the cache
// after this command is completed.
int DaoStore::flush_cache(ostream *os)
{
  dout(10) << __func__ << dendl;
  for (auto i : onode_cache_shards) {
    i->flush();
  }
  for (auto i : buffer_cache_shards) {
    i->flush();
  }

  return 0;
}*/

/*void DaoStore::_apply_padding(uint64_t head_pad,
			       uint64_t tail_pad,
			       bufferlist& padded)
{
  if (head_pad) {
    padded.prepend_zero(head_pad);
  }
  if (tail_pad) {
    padded.append_zero(tail_pad);
  }
  if (head_pad || tail_pad) {
    dout(20) << __func__ << "  can pad head 0x" << std::hex << head_pad
	      << " tail 0x" << tail_pad << std::dec << dendl;
    logger->inc(l_bluestore_write_pad_bytes, head_pad + tail_pad);
  }
}*/
/*void DaoStore::_log_alerts(osd_alert_list_t& alerts)
{
  std::lock_guard l(qlock);

  if (!spurious_read_errors_alert.empty() &&
      cct->_conf->bluestore_warn_on_spurious_read_errors) {
    alerts.emplace(
      "BLUESTORE_SPURIOUS_READ_ERRORS",
      spurious_read_errors_alert);
  }
}*/
