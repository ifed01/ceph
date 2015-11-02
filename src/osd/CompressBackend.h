// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COMPRESS_BACKEND_H
#define COMPRESS_BACKEND_H

#include "OSD.h"
#include "ECBackend.h"
#include "osd_types.h"
#include <boost/optional/optional_io.hpp>
//#include "ECTransaction.h"
#include "ECMsgTypes.h"
#include "ECUtil.h"
#include "CompressContext.h"

/*
ECBackend extenstion to provide object data decompression on read access
*/
class CompressedECBackend : public ECBackend {
protected:
  CompressContextRef get_compress_context_on_read(map<string, bufferlist>& attrset, uint64_t offs, uint64_t offs_last);

public:
  CompressedECBackend(
    PGBackend::Listener* pg,
    coll_t coll,
    ObjectStore* store,
    CephContext* cct,
    ErasureCodeInterfaceRef ec_impl,
    uint64_t stripe_width);

  virtual void objects_read_async(
    const hobject_t& hoid,
    const list < pair < boost::tuple<uint64_t, uint64_t, uint32_t>,
    pair<bufferlist*, Context*> > > &to_read,
    Context* on_complete,
    bool fast_read);
};

#endif
