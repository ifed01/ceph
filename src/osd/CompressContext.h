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

#ifndef COMPRESSCONTEXT_H
#define COMPRESSCONTEXT_H

#include <map>

#include "include/memory.h"
#include "compression/CompressionInterface.h"
#include "include/buffer.h"
#include "include/assert.h"
#include "include/encoding.h"
#include "common/Formatter.h"
#include "common/hobject.h"

class CompressContext {
public:
        struct BlockInfo
        {
                string method;
                uint64_t target_offset;
                BlockInfo() : target_offset(0){}
                BlockInfo(const string& _method, uint64_t _target_offset)
                        : method(_method), target_offset(_target_offset){}

                void encode(bufferlist &bl) const;
                void decode(bufferlist::iterator &bl);
        };
private:
        uint64_t current_original_pos, current_compressed_pos;

        typedef std::map<uint64_t, BlockInfo> BlockMap;
        BlockMap blocks;
        static bool less_upper(  const uint64_t&, const BlockMap::value_type& );
        static bool less_lower(  const BlockMap::value_type&, const uint64_t& );

        int do_compress(CompressionInterfaceRef cs_impl, const ECUtil::stripe_info_t& sinfo, bufferlist& block2compress, string& res_method, bufferlist& result_bl) const;

public:
        CompressContext() : current_original_pos(0), current_compressed_pos(0) {}
        CompressContext(const CompressContext& from) : 
                current_original_pos(from.current_original_pos), 
                current_compressed_pos(from.current_compressed_pos), 
                blocks(from.blocks) {}
        void clear() {
                current_original_pos = current_compressed_pos = 0;
                blocks.clear();
        }
        uint64_t get_next_compressed_offset() const { return current_compressed_pos; }
        uint64_t get_next_original_offset() const { return current_original_pos; }


        void setup_for_append(bufferlist& bl);
        void setup_for_read(map<string, bufferlist>& attrset, uint64_t start_offset, uint64_t end_offset);
        void flush(map<string, bufferlist>& attrset);

        void swap(CompressContext& other) { 
                blocks.swap(other.blocks); 
                std::swap(current_original_pos, other.current_original_pos); 
                std::swap(current_compressed_pos, other.current_compressed_pos); 
        }

        void dump(Formatter *f) const;
        //static void generate_test_instances(list<HashInfo*>& o);

        void append_block(uint64_t original_offset, uint64_t original_size, const string& method, uint64_t new_block_size);
        bool can_compress(uint64_t offs) const;

        pair<uint64_t, uint64_t>  map_offset(uint64_t offs, bool next_block_flag) const; //returns <original block offset, compressed block_offset>

        pair<uint64_t, uint64_t> offset_len_to_compressed_block(const pair<uint64_t, uint64_t> offs_len_pair) const;

        int try_decompress(CompressionInterfaceRef cs_impl, const hobject_t& oid, uint64_t orig_offs, uint64_t len, bufferlist& cs_bl, bufferlist& res_bl) const;
        int try_compress(CompressionInterfaceRef cs_impl, const hobject_t& oid, uint64_t& off, const bufferlist& bl, const ECUtil::stripe_info_t& sinfo, bufferlist& res_bl);


};
typedef ceph::shared_ptr<CompressContext> CompressContextRef;

WRITE_CLASS_ENCODER(CompressContext::BlockInfo)
#endif
