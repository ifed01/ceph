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

//FIXME: check if we need decompress on Recovery!!!!

#include <boost/variant.hpp>
#include <boost/optional/optional_io.hpp>
#include <iostream>
#include <sstream>
//#include <typeinfo>

#include "ECUtil.h"
#include "ECBackend.h"
#include "CompressBackend.h"


#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix *_dout


typedef pair<boost::tuple<uint64_t, uint64_t, uint32_t>, pair<bufferlist*, Context*> > ReadRangeCallParam;

struct CompressBackendReadCallContext : public Context {

        CompressionInterfaceRef cs_impl;
        CompressContextRef ccontext;
        const hobject_t hoid;
        ReadRangeCallParam to_read;
        bufferlist intermediate_buffer;

        CompressBackendReadCallContext(
                CompressionInterfaceRef cs_impl,
                const CompressContextRef& ccontext,
                const hobject_t& hoid,
                const ReadRangeCallParam& to_read)
                : cs_impl(cs_impl), ccontext(ccontext), hoid(hoid), to_read(to_read)
        {
                //assert(cs_impl != NULL);
                assert(ccontext != NULL);
        }

        virtual void finish(int r)
        {
                if (r)
                {
                        bufferlist bl;
                        int res = ccontext->try_decompress(cs_impl,
                                        hoid,
                                        to_read.first.get<0>(),
                                        to_read.first.get<1>(),
                                        intermediate_buffer,
                                        *to_read.second.first
                                        );
                        //FIXME: how to handle an error!!!!
                        if (to_read.second.second) {
                                to_read.second.second->complete(to_read.second.first->length());
                                to_read.second.second=NULL;
                        }
                }
        }

        ~CompressBackendReadCallContext() {
                        delete to_read.second.second;
        }
};

CompressedECBackend::CompressedECBackend(
                        PGBackend::Listener *pg,
                        coll_t coll,
                        ObjectStore *store,
                        CephContext *cct,
                        ErasureCodeInterfaceRef ec_impl,
                        CompressionInterfaceRef cs_impl,
                        uint64_t stripe_width) : 
        ECBackend( pg, coll, store, cct, ec_impl, cs_impl, stripe_width),
        cs_impl(cs_impl)
{
}


void CompressedECBackend::objects_read_async(
        const hobject_t &hoid,
        const list<ReadRangeCallParam> &to_read,
        Context *on_complete)
{
        map<string, bufferlist> attrset;
        int r = load_attrs(hoid, attrset);
        if (r != 0)
        {
                derr << __func__ << ": load_attrs(" << hoid << ")"
                        << " returned a null pointer and there is no "
                        << " way to recover from such an error in this "
                        << " context" << dendl;
                assert(0);
        }

        pair<uint64_t, uint64_t> tmp;

        list<ReadRangeCallParam> to_read_from_ec;
        for( list<ReadRangeCallParam>::const_iterator it = to_read.begin(); it != to_read.end(); it++ ){
                CompressContextRef cinfo = get_compress_context_on_read(attrset, it->first.get<0>(), it->first.get<0>() + it->first.get<1>());
                if (!cinfo) {
                        derr << __func__ << ": get_compress_context_on_read(" << hoid << ")"
                                << " returned a null pointer and there is no "
                                << " way to recover from such an error in this "
                                << " context" << dendl;
                        assert(0);
                }

                tmp = cinfo->offset_len_to_compressed_block(make_pair(it->first.get<0>(), it->first.get<1>()));

                CompressBackendReadCallContext* ctx = new CompressBackendReadCallContext(cs_impl, cinfo, hoid, *it);

                dout(1)<<__func__<<" ifed: add to read from ec:"<< tmp.first<<","<<tmp.second <<dendl;

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
        ECBackend::objects_read_async(hoid, to_read_from_ec, on_complete);
}

CompressContextRef CompressedECBackend::get_compress_context_on_read(
        map<string, bufferlist>& attrset, uint64_t offs, uint64_t offs_last)
{
        //dout(1) << __func__ << ": Getting CompressContext [" << offs << ", " << offs_last << "]" << dendl;
        CompressContextRef ref(new CompressContext);
        ref->setup_for_read(attrset, offs, offs_last);
        return ref;
}
