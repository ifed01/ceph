// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Igor Fedotov <ifedotov@mirantis.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <boost/variant.hpp>
#include <boost/optional/optional_io.hpp>
#include <iostream>
#include <sstream>

#include "ECUtil.h"
#include "ECBackend.h"
#include "CompressBackend.h"


#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix *_dout


typedef pair<boost::tuple<uint64_t, uint64_t, uint32_t>, pair<bufferlist*, Context*> > ReadRangeCallParam;

struct CompressBackendReadCallContext : public Context {

  CompressContextRef ccontext;
  const hobject_t hoid;
  ReadRangeCallParam to_read;
  bufferlist intermediate_buffer;

  CompressBackendReadCallContext(
    const CompressContextRef& ccontext,
    const hobject_t& hoid,
    const ReadRangeCallParam& to_read)
    : ccontext(ccontext), hoid(hoid), to_read(to_read) {
    assert(ccontext != NULL);
  }

  virtual void finish(int r) {
    if (r) {
      ccontext->try_decompress(
	hoid,
	to_read.first.get<0>(),
	to_read.first.get<1>(),
	intermediate_buffer,
	to_read.second.first
      );
      if (to_read.second.second) {
	to_read.second.second->complete(to_read.second.first->length());
	to_read.second.second = NULL;
      }
    }
  }

  ~CompressBackendReadCallContext() {
    delete to_read.second.second;
  }
};

struct CompressBackendTransactCommittedContext : public Context
{
  CompressBackendTransactCommittedContext(
    Context* _parent ) :
      parent(_parent)
  { 
    assert(parent);
  }
  CompressBackend::CompressInfoMap& get_compress_infos() 
  {
    return compress_infos;
  }

protected:
  Context* parent;
  CompressBackend::CompressInfoMap compress_infos;

  virtual void finish(int r)
  {
    compress_infos.clear();
    if( parent ) 
      parent->complete(r);
    parent = NULL;
  }
};

CompressBackend::CompressBackend(
  PGBackend::Listener* pg,
  coll_t coll,
  ObjectStore* store,
  PGBackend* _next_pgbackend,
  uint64_t _stripe_width,
  uint64_t _align_width ) :
    PGBackend(pg, store, coll ),
    next_pgbackend( _next_pgbackend ),
    stripe_width(_stripe_width),
    align_width(_align_width)
{
  assert(next_pgbackend);
  assert(stripe_width);
}

PGBackend::PGTransaction *CompressBackend::get_transaction()
{
    return new Transaction( next_pgbackend->get_transaction());
}

struct IsAppendVisitor : public ObjectModDesc::Visitor {
  enum { EMPTY, FOUND_APPEND, FOUND_CREATE_STASH } state;
  IsAppendVisitor() : state(EMPTY) {}
  void append(uint64_t) {
    if (state == EMPTY) {
      state = FOUND_APPEND;
    }
  }
  void rmobject(version_t) {
    if (state == EMPTY) {
      state = FOUND_CREATE_STASH;
    }
  }
  void create() {
    if (state == EMPTY) {
      state = FOUND_CREATE_STASH;
    }
  }
  bool operator()() const { return state == FOUND_APPEND; }
};

void CompressBackend::submit_transaction(
  const hobject_t &hoid,               ///< [in] object
  const eversion_t &at_version,        ///< [in] version
  PGTransaction *_t,                   ///< [in] trans to execute
  const eversion_t &trim_to,           ///< [in] trim log to here
  const eversion_t &trim_rollback_to,  ///< [in] trim rollback info to here
  const vector<pg_log_entry_t> &log_entries, ///< [in] log entries for t
  /// [in] hitset history (if updated with this transaction)
  boost::optional<pg_hit_set_history_t> &hset_history,
  Context *on_local_applied_sync,      ///< [in] called when applied locally
  Context *on_all_applied,             ///< [in] called when all acked
  Context *on_all_commit,              ///< [in] called when all commit
  ceph_tid_t tid,                      ///< [in] tid
  osd_reqid_t reqid,                   ///< [in] reqid
  OpRequestRef op                      ///< [in] op
  )
{
  Transaction* t = dynamic_cast<Transaction*>(_t);
  assert(t);

  set<hobject_t, hobject_t::BitwiseComparator> need_infos;

  t->get_append_objects(&need_infos);
  if (need_infos.empty()) {
    next_pgbackend->submit_transaction(
      hoid,
      at_version,
      t->get_associated_transaction(),
      trim_to,
      trim_rollback_to,
      log_entries,
      hset_history,
      on_local_applied_sync,
      on_all_applied,
      on_all_commit,
      tid,
      reqid,
      op);
  } else {

    CompressBackendTransactCommittedContext* on_all_commit_this = new CompressBackendTransactCommittedContext(on_all_commit);
    CompressInfoMap& compress_infos = on_all_commit_this->get_compress_infos();

    for (set<hobject_t, hobject_t::BitwiseComparator>::iterator i = need_infos.begin();
      i != need_infos.end();
      ++i) {

      CompressContextRef cinfo = get_compress_context_basic(*i);
      if (!cinfo) {
	derr << __func__ << ": get_compress_context_basic(" << *i << ")"
	  << " returned a null pointer and there is no "
	  << " way to recover from such an error in this "
	  << " context" << dendl;
	assert(0);
      }
      compress_infos.insert(make_pair(*i, cinfo));
    }

    vector<pg_log_entry_t> log_entries_local = log_entries;

    for (vector<pg_log_entry_t>::iterator i = log_entries_local.begin();
      i != log_entries_local.end();
      ++i) {
      IsAppendVisitor is_append;
      i->mod_desc.visit(&is_append);
      if (is_append()) {
        dout(10) << __func__ << ": stashing CompressInfo for "
        << i->soid << " for entry " << *i << dendl;
        assert(compress_infos.count(i->soid));
        ObjectModDesc desc;
        map<string, boost::optional<bufferlist> > old_attrs;

        compress_infos[i->soid]->flush_for_rollback(&old_attrs);

        desc.setattrs(old_attrs);
        i->mod_desc.swap(desc);
        i->mod_desc.claim_append(desc);
        assert(i->mod_desc.can_rollback());
      }
    }

    t->generate_compression_transactions(
      compress_infos,
      get_parent()->get_pool().compression_type.c_str(),
      align_width);

    next_pgbackend->submit_transaction(
      hoid,
      at_version,
      t->get_associated_transaction(),
      trim_to,
      trim_rollback_to,
      log_entries_local,
      hset_history,
      on_local_applied_sync,
      on_all_applied,
      on_all_commit_this,
      tid,
      reqid,
      op );
  }
}


void CompressBackend::objects_read_async(
  const hobject_t& hoid,
  const list<ReadRangeCallParam>& to_read,
  Context* on_complete,
  bool fast_read) {
  map<string, bufferlist> attrset;
  int r = load_attrs(hoid, &attrset);
  if (r != 0) {
    derr << __func__ << ": failed loading attributes: oid=" << hoid << ":"
	 << " null pointer returned and there is no "
	 << " way to recover from such an error in this "
	 << " context" << dendl;
    assert(0);
  }

  pair<uint64_t, uint64_t> tmp;

  list<ReadRangeCallParam> to_read_from_ec;
  for (list<ReadRangeCallParam>::const_iterator it = to_read.begin(); it != to_read.end(); it++) {
    CompressContextRef cinfo = get_compress_context_on_read(attrset, it->first.get<0>(), it->first.get<0>() + it->first.get<1>());
    if (!cinfo) {
      derr << __func__ << ": failed to obtain CompressionCcontext: oid=" << hoid
	   << " (" << it->first.get<0>() << "," << it->first.get<1>()
	   << "): null pointer returned and there is no "
	   << " way to recover from such an error in this "
	   << " context" << dendl;
      assert(0);
    }

    tmp = cinfo->offset_len_to_compressed_block(make_pair(it->first.get<0>(), it->first.get<1>()));

    CompressBackendReadCallContext* ctx = new CompressBackendReadCallContext(cinfo, hoid, *it);

    dout(CompressContext::DEBUG_LEVEL) << __func__
					  << " reading from EC pool: original(" << "( offs:" << it->first.get<0>() << ", len:" << it->first.get<1>() << ")"
					  << " target ( offs:" << tmp.first << ", len:" << tmp.second << ")" << dendl;

    to_read_from_ec.push_back(
      std::make_pair(
	boost::make_tuple(
	  tmp.first, //new offset
	  tmp.second, //new length
	  it->first.get<2>()), //flags
	std::make_pair(
	  &ctx->intermediate_buffer,
	  ctx))
    );
  }
  next_pgbackend->objects_read_async(hoid, to_read_from_ec, on_complete, fast_read);
}

int CompressBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist> *out)
{
  int r = next_pgbackend->objects_get_attrs( hoid, out );
  if (r >= 0) {
    for (map<string, bufferlist>::iterator i = out->begin();
      i != out->end();
      ) {
      if (CompressContext::is_internal_key_string(i->first))
	out->erase(i++);
      else
	++i;
    }
  }
  return r;
}

CompressContextRef CompressBackend::get_compress_context_on_read(
  map<string, bufferlist>& attrset, uint64_t offs, uint64_t offs_last) {
  CompressContextRef ref(new CompressContext(stripe_width, align_width));
  ref->setup_for_read(attrset, offs, offs_last);
  return ref;
}

CompressContextRef CompressBackend::get_compress_context_basic(const hobject_t &hoid)
{
  CompressContextRef ref = unstable_compressinfo_registry.lookup(hoid);
  if (!ref) {
    dout(10) << __func__ << ": not in cache " << hoid << dendl;
    map<string, bufferlist> attrset;
    CompressContext cctx(stripe_width, align_width);
    if (load_attrs(hoid, &attrset) >= 0) {
      cctx.setup_for_append_or_recovery(attrset);
    }
    ref = unstable_compressinfo_registry.lookup_or_create(hoid, cctx);
  }
  return ref;
}

int CompressBackend::load_attrs(const hobject_t &hoid, map<string, bufferlist>* attrset)
{
  assert(attrset);
  dout(10) << __func__ << ": Loading attrs on " << hoid << dendl;
  struct stat st;
  int r = store->stat(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    &st);
  if (r >= 0 && st.st_size > 0) {
    dout(10) << __func__ << ": found on disk, size " << st.st_size << dendl;
    bufferlist bl;

    r = store->getattrs(
      coll,
      ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      *attrset);
    if (r < 0){
      derr << __func__ << ": failed to load attrs" << dendl;
    }
    else{
      dout(10)<<__func__<<"retrieved "<<attrset->size()<<dendl;
    }
  }
  return r;
}


// PG_RecoveryInfoProvider interface implementation
uint64_t CompressBackend::get_object_size_for_recovery(const map<string, bufferlist>& attrs, uint64_t defval) const
{
  return CompressContext::get_compressed_size(attrs, defval);
}

struct CompressTransGenerator : public boost::static_visitor<void> {
  CompressBackend::CompressInfoMap &compress_infos;

  std::string compression_method;
  PGBackend::PGTransaction* next_pgtrans;
  uint64_t align_width;

  stringstream *out;
  CompressTransGenerator(
    CompressBackend::CompressInfoMap &_compress_infos,
    const char* _compression_method,
    PGBackend::PGTransaction* _next_pgtrans,
    uint64_t _align_width,
    stringstream *_out)
    : compress_infos(_compress_infos),
    compression_method(_compression_method), 
    next_pgtrans(_next_pgtrans),
    align_width(_align_width),
    out(_out) {
  }

  void operator()(const ECTransaction::TouchOp &op) 
  {
    next_pgtrans->touch(op.oid);
  }

  void operator()(const ECTransaction::AppendOp &op) {
    uint64_t offset = op.off;
    bufferlist bl;
    map<string, bufferlist> attrset;

    assert(op.bl.length());
    if (align_width) {
      assert(offset % align_width == 0);
    }
    assert(compress_infos.count(op.oid));
    CompressContextRef cinfo = compress_infos[op.oid];
    cinfo->try_compress(compression_method, op.oid, op.bl, &offset, &bl);
    cinfo->flush(&attrset);

    assert(bl.length());
    if (align_width) {
      assert(offset % align_width == 0); //offset has changed - check again
    }
    next_pgtrans->append(op.oid, offset, bl.length(), bl, op.fadvise_flags);
    next_pgtrans->setattrs(op.oid, attrset);

  }
  void operator()(const ECTransaction::CloneOp &op) {
    assert(compress_infos.count(op.source));
    assert(compress_infos.count(op.target));
    *(compress_infos[op.target]) = *(compress_infos[op.source]);
    next_pgtrans->clone(op.source, op.target);
  }
  void operator()(const ECTransaction::RenameOp &op) {
    assert(compress_infos.count(op.source));
    assert(compress_infos.count(op.destination));
    *(compress_infos[op.destination]) = *(compress_infos[op.source]);
    compress_infos[op.source]->clear();
    next_pgtrans->rename(op.source, op.destination);
  }
  void operator()(const ECTransaction::StashOp &op) {
    assert(compress_infos.count(op.oid));
    compress_infos[op.oid]->clear();
    next_pgtrans->stash(op.oid, op.version);
  }
  void operator()(const ECTransaction::RemoveOp &op) {
    assert(compress_infos.count(op.oid));
    compress_infos[op.oid]->clear();
    next_pgtrans->remove(op.oid);
  }
  void operator()(ECTransaction::SetAttrsOp &op)
  {
    next_pgtrans->setattrs(op.oid, op.attrs);
  }
  void operator()(const ECTransaction::RmAttrOp &op) 
  {
    next_pgtrans->rmattr(op.oid, op.key);
  }
  void operator()(const ECTransaction::AllocHintOp &op) 
  {
    next_pgtrans->set_alloc_hint(op.oid, op.expected_object_size, op.expected_write_size);
  }
  void operator()(const ECTransaction::NoOp &op) 
  {
    next_pgtrans->nop();
  }
};

void CompressBackend::Transaction::generate_compression_transactions(
  CompressBackend::CompressInfoMap &compress_infos,
  const char* compression_method,
  uint64_t align_width,
  stringstream *out)
{
  CompressTransGenerator gen(
    compress_infos,
    compression_method,
    next_pgtrans,
    align_width,
    out);
  visit_and_modify(gen);
}
