// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
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
#include "ECBackend.h"
#include "osd_types.h"
#include <boost/optional/optional_io.hpp>
#include "ECTransaction.h"
#include "ECMsgTypes.h"
#include "ECUtil.h"
#include "CompressContext.h"


class CompressedECBackend : public ECBackend {


protected:
   CompressContextRef get_compress_context_on_read(map<string, bufferlist>& attrset, uint64_t offs, uint64_t offs_last);

public:
  CompressedECBackend(
    PGBackend::Listener *pg,
    coll_t coll,
    ObjectStore *store,
    CephContext *cct,
    ErasureCodeInterfaceRef ec_impl,
    uint64_t stripe_width);

  virtual void objects_read_async(
                  const hobject_t &hoid,
                  const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>, 
                    pair<bufferlist*, Context*> > > &to_read,
                  Context* on_complete,
                  bool fast_read );

  //virtual PGTransaction *get_transaction(); later we can override this method to create modified ECTransaction to handle compression

  void try_decompress(CompressContextRef cinfo, const bufferlist& cs_bl, bufferlist& res_bl);

};

#endif
