// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#ifndef COMPRESS_BACKEND_H
#define COMPRESS_BACKEND_H

#include "OSD.h"
#include "PGBackend.h"
#include "osd_types.h"
#include "ECMsgTypes.h"
#include "ECTransaction.h"
#include "CompressContext.h"
#include "common/sharedptr_registry.hpp"

#include <boost/optional/optional_io.hpp>
/*
CompressBackend is an intermediate layer that "injects" data compression support by prepending regular ( EC only at the moment ) PG backends. 
To do that it implements PGBackend interface and proxies regular backend access.

Thus following chain appears:
[ReplicatedPG] -> [CompressBackend] -> [ECBackend]

*/

class CompressBackend : public PG_RecoveryInfoProvider, public PGBackend {

 public:
   typedef map<hobject_t, CompressContextRef, hobject_t::BitwiseComparator> CompressInfoMap;

   CompressBackend(
     PGBackend::Listener* pg,
     coll_t coll,
     ObjectStore* store,
     PGBackend* _next_pgbackend,
     uint64_t stripe_width,
     uint64_t align_width
     );

   //
   //PGBackend interface implementation, relaying most of calls
   //
   virtual RecoveryHandle *open_recovery_op() 
   { 
     return next_pgbackend->open_recovery_op(); 
   }

   virtual void run_recovery_op(
     RecoveryHandle *h,     ///< [in] op to finish
     int priority           ///< [in] msg priority
     )
   {
     next_pgbackend->run_recovery_op(h, priority);
   }

   virtual void recover_object(
     const hobject_t &hoid, ///< [in] object to recover
     eversion_t v,          ///< [in] version to recover
     ObjectContextRef head,  ///< [in] context of the head/snapdir object
     ObjectContextRef obc,  ///< [in] context of the object
     PG_RecoveryInfoProvider* rinfo_provider, ///< [in] pointer to an interface to obtain actual object size for recovery
     RecoveryHandle *h      ///< [in,out] handle to attach recovery op to
     )
   {
     next_pgbackend->recover_object(hoid, v, head, obc, rinfo_provider, h);
   }

   virtual bool can_handle_while_inactive(OpRequestRef op)
   {
     return next_pgbackend->can_handle_while_inactive(op);
   }

   virtual bool handle_message(
     OpRequestRef op ///< [in] message received
     )
   {
     return next_pgbackend->handle_message(op);
   }

   virtual void check_recovery_sources(const OSDMapRef osdmap)
   {
     return next_pgbackend->check_recovery_sources(osdmap);
   }

   virtual void on_change()
   {
     next_pgbackend->on_change();
   }

   virtual void clear_recovery_state()
   {
     next_pgbackend->clear_recovery_state();
   }

   virtual void on_flushed()
   {
     next_pgbackend->on_flushed();
   }

   virtual IsPGRecoverablePredicate *get_is_recoverable_predicate()
   {
     return next_pgbackend->get_is_recoverable_predicate();
   }
   virtual IsPGReadablePredicate *get_is_readable_predicate()
   {
     return next_pgbackend->get_is_readable_predicate();
   }

   virtual void dump_recovery_info(Formatter *f) const
   {
     next_pgbackend->dump_recovery_info(f);
   }

   virtual PGTransaction *get_transaction();

   virtual void submit_transaction(
     const hobject_t &hoid,               ///< [in] object
     const eversion_t &at_version,        ///< [in] version
     PGTransaction *t,                    ///< [in] trans to execute
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
     );

   virtual void rollback_append(
     const hobject_t &hoid,
     uint64_t old_size,
     ObjectStore::Transaction *t)
   {
       next_pgbackend->rollback_append(hoid, old_size, t);
   }

   virtual int objects_get_attrs(
     const hobject_t &hoid,
     map<string, bufferlist> *out);

   virtual int objects_read_sync(
     const hobject_t &hoid,
     uint64_t off,
     uint64_t len,
     uint32_t op_flags,
     bufferlist *bl)
   {
     return next_pgbackend->objects_read_sync( hoid, off, len, op_flags, bl);
   }

  virtual void objects_read_async(
    const hobject_t& hoid,
    const list < pair < boost::tuple<uint64_t, uint64_t, uint32_t>,
    pair<bufferlist*, Context*> > >& to_read,
    Context* on_complete,
    bool fast_read);


  virtual bool scrub_supported()
  { 
    return next_pgbackend->scrub_supported(); 
  }
  virtual bool auto_repair_supported() const 
  { 
    return next_pgbackend->auto_repair_supported(); 
  }

  virtual uint64_t be_get_ondisk_size(
    uint64_t logical_size) 
  {
    return next_pgbackend->be_get_ondisk_size(logical_size); //FIXME: need handling?
  }
  virtual void be_deep_scrub(
    const hobject_t &poid,
    uint32_t seed,
    ScrubMap::object &o,
    ThreadPool::TPHandle &handle)
  {
    next_pgbackend->be_deep_scrub( poid, seed, o, handle);
  }

protected:
  
  int load_attrs(const hobject_t &hoid, map<string, bufferlist>* attrset);
  
  CompressContextRef get_compress_context_basic(const hobject_t &hoid);
  CompressContextRef get_compress_context_on_read(map<string, bufferlist>& attrset, uint64_t offs, uint64_t offs_last);

  // PG_RecoveryInfoProvider interface implementation
  virtual uint64_t get_object_size_for_recovery(
    const map<string, bufferlist>& attrs, uint64_t defval) const;

  class Transaction : public ECTransaction
  {
    PGBackend::PGTransaction* next_pgtrans;

  public:
    Transaction(PGBackend::PGTransaction* _next_pgtrans) : next_pgtrans(_next_pgtrans) {}
    virtual ~Transaction() { delete next_pgtrans;  }


    PGBackend::PGTransaction* get_associated_transaction() { return next_pgtrans; }
    void generate_compression_transactions(
      CompressBackend::CompressInfoMap &compress_infos,
      const char* compression_method,
      uint64_t align_width,
      stringstream *out = 0);

    using ECTransaction::append;
    void append(PGTransaction *_to_append) {
      Transaction *to_append = dynamic_cast<Transaction*>(_to_append);
      assert(to_append);
      ECTransaction::append(to_append->get_associated_transaction());
    }

  };

private:
  PGBackend* next_pgbackend;

  uint64_t stripe_width;          // Compression backend specific stripe width - this determines max compression block and blockset sized
  uint64_t align_width;           // Determines compressed block alignment if any. Required if compressed data to be subsequently processed by a backend ( e.g. EC one ) that requires aligned input data when writing.


  SharedPtrRegistry<hobject_t, CompressContext, hobject_t::BitwiseComparator> unstable_compressinfo_registry;

};

#endif
