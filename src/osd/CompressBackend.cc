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
//#define dout_prefix _prefix(_dout, this)
#define dout_prefix *_dout
// static ostream& _prefix(std::ostream *_dout, ECBackend *pgb) {
//   return *_dout << pgb->get_parent()->gen_dbg_prefix();
// }


pair<boost::tuple<uint64_t, uint64_t, uint32_t>, pair<bufferlist*, Context*> > ReadRangeCallParam;

struct CompressionBackendReadCallContext : public Context {

        CompressedECBackend* cec_backend;
        CompressContextRef ccontext;
        ReadRangeCallParam to_read;
        bufferlist* intermediate_buffer;

        CompressionBackendReadCallContext(
                CompressedECBackend* cec_backend,
                const CompressContextRef& ccontext,
                const ReadRangeCallParam& to_read,
                bufferlist* intermediate_buffer)
                : cec_backend(cec_backend), ccontext(ccontext), to_read(to_read), intermediate_buffer(intermediate_buffer)
        {
                assert(cec_backend != NULL);
                assert(ccontext != NULL);
                assert(intermediate_buffer != NULL);
        }

        virtual void finish(int r)
        {
                assert(next_finished_cctx != ccontexts.end());
                assert( next_finished_read != to_read.end();

                if (r)
                {
                        bufferlist bl;
                        int res = cec_backend->try_decompress(cec_backend,
                                        ccontext,
                                        *intermediate_buffer,
                                        to_read.first.get<0>,
                                        to_read.first.get<1>,
                                        to_read.second.first
                                        );
                        //FIXME: how to handle an error!!!!
                        if (to_read.second.second) {
                                to_read.second.second->complete(to_read.second.first->length());
                        }
                }
        }

        ~CompressionBackendReadCallContext() {
                for (ReadRequestParams::iterator i = to_read.begin();
                        i != to_read.end();
                        to_read.erase(i++)) {
                        delete i->second.second;
                }
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
        const ReadRequestParams &to_read,
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

        ReadRequestParams to_read_from_ec;
        for (ReadRequestParams::const_iterator i = to_read.begin(); i != to_read.end(); ++i) {
                CompressContextRef cinfo = get_compress_context_on_read(attrset, i->first.get<0>(), i->first.get<0>() + i->first.get<1>());
                if (!cinfo) {
                        derr << __func__ << ": get_compress_context_on_read(" << *i << ")"
                                << " returned a null pointer and there is no "
                                << " way to recover from such an error in this "
                                << " context" << dendl;
                        assert(0);
                }

                tmp = cinfo->offset_len_to_compressed_block(make_pair(i->first.get<0>(), i->first.get<1>()));

                intermediate_buffer = new bufferlist; //to be destroyed by ECBackend when read op is completed ( see CallClientContexts dtor )
                CompressionBackendReadCallContext* ctx = new CompressionBackendReadCallContext(this, cinfo, *i, intermediate_buffer);
                to_read_from_ec.push_back(
                        boost::make_pair(
                        boost::make_tuple(
                                tmp.first, //new offset
                                tmp.second, //new length
                                i->first.get<2>()), //flags
                        boost::make_pair(
                                intermediate_buffer,
                                ctx));
                        );

                ECBackend::objects_read_async(hoid, to_read_from_ec, on_complete);
        }
}

ECUtil::CompressContextRef CompressedECBackend::get_compress_context_on_read(
        map<string, const bufferlist>& attrset, uint64_t offs, uint64_t offs_last)
{
        dout(10) << __func__ << ": Getting CompressContext [" << offs << ", " << offs_last << "]" << dendl;
        ECUtil::CompressContextRef ref(new ECUtil::CompressContext);
        ref->setup_for_read(attrset, offs, offs_last);
        return ref;
}
