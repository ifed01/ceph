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

#ifndef ECUTIL_H
#define ECUTIL_H

#include <map>
#include <set>

#include "include/memory.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "compression/CompressionInterface.h"
#include "include/buffer.h"
#include "include/assert.h"
#include "include/encoding.h"
#include "common/Formatter.h"

namespace ECUtil {

const uint64_t CHUNK_ALIGNMENT = 64;
const uint64_t CHUNK_INFO = 8;
const uint64_t CHUNK_PADDING = 8;
const uint64_t CHUNK_OVERHEAD = 16; // INFO + PADDING

class stripe_info_t {
  const uint64_t stripe_size;
  const uint64_t stripe_width;
  const uint64_t chunk_size;
public:
  stripe_info_t(uint64_t stripe_size, uint64_t stripe_width)
    : stripe_size(stripe_size), stripe_width(stripe_width),
      chunk_size(stripe_width / stripe_size) {
    assert(stripe_width % stripe_size == 0);
  }
  uint64_t get_stripe_width() const {
    return stripe_width;
  }
  uint64_t pad_to_stripe_width(uint64_t length) const {
    if (length % get_stripe_width())
        length += get_stripe_width() - (length % get_stripe_width());
    return length;
  }

  uint64_t get_chunk_size() const {
    return chunk_size;
  }
  uint64_t logical_to_prev_chunk_offset(uint64_t offset) const {
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t logical_to_next_chunk_offset(uint64_t offset) const {
    return ((offset + stripe_width - 1)/ stripe_width) * chunk_size;
  }
  uint64_t logical_to_prev_stripe_offset(uint64_t offset) const {
    return offset - (offset % stripe_width);
  }
  uint64_t logical_to_next_stripe_offset(uint64_t offset) const {
    return ((offset % stripe_width) ?
      (offset - (offset % stripe_width) + stripe_width) :
      offset);
  }
  uint64_t aligned_logical_offset_to_chunk_offset(uint64_t offset) const {
    assert(offset % stripe_width == 0);
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t aligned_chunk_offset_to_logical_offset(uint64_t offset) const {
    assert(offset % chunk_size == 0);
    return (offset / chunk_size) * stripe_width;
  }
  pair<uint64_t, uint64_t> aligned_offset_len_to_chunk(
    pair<uint64_t, uint64_t> in) const {
    return make_pair(
      aligned_logical_offset_to_chunk_offset(in.first),
      aligned_logical_offset_to_chunk_offset(in.second));
  }
  pair<uint64_t, uint64_t> offset_len_to_stripe_bounds(
    pair<uint64_t, uint64_t> in) const {
    uint64_t off = logical_to_prev_stripe_offset(in.first);
    uint64_t len = logical_to_next_stripe_offset(
      (in.first - off) + in.second);
    return make_pair(off, len);
  }
};

int decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  bufferlist *out);

int decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  map<int, bufferlist*> &out);

int decompress(
  CompressionInterfaceRef &cs_impl,
  int original_size,
  bufferlist &in,
  bufferlist *out);

int compress(
  CompressionInterfaceRef &cs_impl,
  bufferlist &in,
  bufferlist *out);

int encode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  bufferlist &in,
  const set<int> &want,
  map<int, bufferlist> *out);

class HashInfo {
  uint64_t total_chunk_size;
  vector<uint32_t> cumulative_shard_hashes;
public:
  HashInfo() : total_chunk_size(0) {}
  HashInfo(unsigned num_chunks)
  : total_chunk_size(0),
    cumulative_shard_hashes(num_chunks, -1) {}
  void append(uint64_t old_size, map<int, bufferlist> &to_append) {
    assert(to_append.size() == cumulative_shard_hashes.size());
    //assert(old_size == total_chunk_size);
    uint64_t size_to_append = to_append.begin()->second.length();
    for (map<int, bufferlist>::iterator i = to_append.begin();
	 i != to_append.end();
	 ++i) {
      assert(size_to_append == i->second.length());
      assert((unsigned)i->first < cumulative_shard_hashes.size());
      uint32_t new_hash = i->second.crc32c(cumulative_shard_hashes[i->first]);
      cumulative_shard_hashes[i->first] = new_hash;
    }
    total_chunk_size += size_to_append;
  }
  void clear() {
    total_chunk_size = 0;
    cumulative_shard_hashes = vector<uint32_t>(
      cumulative_shard_hashes.size(),
      -1);
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<HashInfo*>& o);
  uint32_t get_chunk_hash(int shard) const {
    assert((unsigned)shard < cumulative_shard_hashes.size());
    return cumulative_shard_hashes[shard];
  }
  uint64_t get_total_chunk_size() const {
    return total_chunk_size;
  }
};
typedef ceph::shared_ptr<HashInfo> HashInfoRef;

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

        int do_compress(CompressionInterfaceRef cs_impl, const ECUtil::stripe_info_t& sinfo, const bufferlist& block2compress, stdstring& res_method, bufferlist& result_bl) const;

public:
        CompressContext() : current_original_pos, current_compressed_pos(0) {}
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


        void setup_for_append(const bufferlist& bl);
        void setup_for_read(map<string, bufferlist>& attrset, uint64_t start_offset, uint64_t end_offset);
        void flush(map<string, boost::optional<bufferlist> >& attrset);

        void swap(CompressContext& other) { 
                blocks.swap(other.blocks); 
                std::swap(current_original_pos, other.current_original_pos); 
                std::swap(current_compressed_pos, other.current_compressed_pos); 
        }

        void dump(Formatter *f) const;
        //static void generate_test_instances(list<HashInfo*>& o);

        void append_block(uint64_t original_offset, uint64_t original_size, const string& method, uint64_t new_block_size);
        bool can_compress(uint64_t offs) { return (offs == 0 && current_compressed_pos == 0) || current_compressed_pos != 0; }

        pair<uint64_t, uint64_t>  map_offset(uint64_t offs, bool next_block_flag) const; //returns <original block offset, compressed block_offset>

        pair<uint64_t, uint64_t> offset_len_to_compressed_block(const pair<uint64_t, uint64_t> offs_len_pair) const;

        int try_decompress(CompressionInterfaceRef cs_impl, const bufferlist& cs_bl, uint64_t orig_offs, uint64_t len, bufferlist& res_bl) const;
        int try_compress(const hobject_t& oid, uint64_t off, const bufferlist& bl, CompressionInterfaceRef cs_impl, const ECUtil::stripe_info_t& sinfo, bufferlist& res_bl);


};
typedef ceph::shared_ptr<CompressContext> CompressContextRef;

bool is_internal_key_string(const string &key);
const string &get_hinfo_key();
const string &get_cinfo_key();

}
WRITE_CLASS_ENCODER(ECUtil::HashInfo)
//WRITE_CLASS_ENCODER(ECUtil::CompressContext)
WRITE_CLASS_ENCODER(ECUtil::CompressContext::BlockInfo)
#endif
