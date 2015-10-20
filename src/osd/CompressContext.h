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
        struct BlockInfoRecord
        {
                uint8_t method_idx;
                uint32_t original_length, compressed_length;
                BlockInfoRecord() : method_idx(0), original_length(0), compressed_length(0){}
                BlockInfoRecord(const BlockInfoRecord& from) : method_idx(from.method_idx), original_length(from.original_length), compressed_length(from.compressed_length){}
                BlockInfoRecord(uint8_t idx, uint32_t olen, uint32_t clen) : method_idx(idx), original_length(olen), compressed_length(clen){}

                void encode(bufferlist &bl) const;
                void decode(bufferlist::iterator &bl);
        };
        struct BlockInfoRecordSetHeader
        {
                uint64_t start_offset, compressed_offset;
                BlockInfoRecordSetHeader(uint64_t offset = 0, uint64_t coffs = 0) : start_offset(offset), compressed_offset(coffs){}

                void encode(bufferlist &bl) const;
                void decode(bufferlist::iterator &bl);
        };

        struct MasterRecord
        {
                uint64_t current_original_pos, current_compressed_pos;
                uint32_t block_info_record_length, block_info_recordset_header_length;
                typedef vector<string>  MethodList;
                MethodList methods;

                MasterRecord() : current_original_pos(0), current_compressed_pos(0), block_info_record_length(0), block_info_recordset_header_length(0) {}
                MasterRecord( const MasterRecord& other) : 
                  current_original_pos(other.current_original_pos), 
                  current_compressed_pos(other.current_compressed_pos), 
                  block_info_record_length(other.block_info_record_length), 
                  block_info_recordset_header_length(other.block_info_recordset_header_length),
                  methods(other.methods) {}

                void clear() { current_original_pos = current_compressed_pos = 0; block_info_record_length = block_info_recordset_header_length = 0; }

                string get_method_name(uint8_t index) const;
                uint8_t add_get_method(const string& method);

                void encode(bufferlist &bl) const;
                void decode(bufferlist::iterator &bl);
                void swap( MasterRecord& other ){
                  std::swap( current_original_pos, other.current_original_pos);
                  std::swap( current_compressed_pos, other.current_compressed_pos);
                  std::swap( block_info_record_length, other.block_info_record_length);
                  std::swap( block_info_recordset_header_length, other.block_info_recordset_header_length);
                  methods.swap( other.methods);
                }
    };

private:
        enum {
                RECS_PER_RECORDSET = 32
        };
        struct BlockInfo
        {
                uint8_t method_idx;
                uint64_t target_offset;
                BlockInfo() : target_offset(0){}
                BlockInfo(uint8_t _method, uint64_t _target_offset)
                        : method_idx(_method), target_offset(_target_offset){}

        };

        MasterRecord masterRec;

        typedef std::map<uint64_t, BlockInfo> BlockMap;
        BlockMap blocks;
        
        MasterRecord prevMasterRec;
        bufferlist prev_blocks_encoded;


        static bool less_upper(  const uint64_t&, const BlockMap::value_type& );
        static bool less_lower(  const BlockMap::value_type&, const uint64_t& );

        int do_compress(CompressionInterfaceRef cs_impl, const ECUtil::stripe_info_t& sinfo, bufferlist& block2compress, string& res_method, bufferlist& result_bl) const;

public:
        CompressContext() {}
        CompressContext(const CompressContext& from) : 
                masterRec (from.masterRec),
                blocks(from.blocks),
                prevMasterRec(from.prevMasterRec),
                prev_blocks_encoded(from.prev_blocks_encoded) {}
        virtual ~CompressContext();
        void clear() {
                masterRec.clear();
                blocks.clear();
                prev_blocks_encoded.clear();
                prevMasterRec.clear();
        }

        void setup_for_append_or_recovery(map<string, bufferlist>& attrset);
        void setup_for_read(map<string, bufferlist>& attrset, uint64_t start_offset, uint64_t end_offset);
        void flush(map<string, bufferlist>& attrset);
        void flush_for_rollback(map<string, boost::optional<bufferlist> >& attrset) const;

        void swap(CompressContext& other) { 
                blocks.swap(other.blocks); 
                masterRec.swap(other.masterRec); 
                std::swap( prev_blocks_encoded, other.prev_blocks_encoded);
                prevMasterRec.swap(other.prevMasterRec);
        }

        void dump(Formatter *f) const;
        //static void generate_test_instances(list<HashInfo*>& o);

        void append_block(uint64_t original_offset, uint64_t original_size, const string& method, uint64_t new_block_size);
        bool can_compress(uint64_t offs) const;

        pair<uint64_t, uint64_t>  map_offset(uint64_t offs, bool next_block_flag) const; //returns <original block offset, compressed block_offset>

        pair<uint64_t, uint64_t> offset_len_to_compressed_block(const pair<uint64_t, uint64_t> offs_len_pair) const;

        int try_decompress(CompressionInterfaceRef cs_impl, const hobject_t& oid, uint64_t orig_offs, uint64_t len, bufferlist& cs_bl, bufferlist& res_bl) const;
        int try_compress(CompressionInterfaceRef cs_impl, const hobject_t& oid, uint64_t& off, const bufferlist& bl, const ECUtil::stripe_info_t& sinfo, bufferlist& res_bl);

        uint64_t get_compressed_size() const { return masterRec.current_compressed_pos; }

};
typedef ceph::shared_ptr<CompressContext> CompressContextRef;

WRITE_CLASS_ENCODER(CompressContext::BlockInfoRecord)
WRITE_CLASS_ENCODER(CompressContext::BlockInfoRecordSetHeader)
WRITE_CLASS_ENCODER(CompressContext::MasterRecord)
#endif
