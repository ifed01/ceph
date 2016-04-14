// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Mirantis, Inc
 *
 * Author: Igor Fedotov <ifedotov@mirantis.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_EXTENT_MANAGER_H
#define CEPH_OSD_EXTENT_MANAGER_H

#include <list>
#include <map>

#include "include/buffer.h"
#include "bluestore_types.h"

class ExtentManager{

public:
  struct CheckSumInfo{
    uint8_t csum_type;               //  enum bluestore_blob_t::CSumType
    uint8_t csum_block_order;
  };
 
  struct CompressInfo{
    uint8_t compress_type;
  };

  struct BlockOpInterface
  {
    virtual ~BlockOpInterface() {}
    virtual uint64_t get_block_size() = 0;

    virtual int read_block(uint64_t offset, uint32_t length, void* opaque, bufferlist* result) = 0;
    virtual int write_block(uint64_t offset, uint32_t length, const bufferlist& data, void* opaque) = 0;
    virtual int zero_block(uint64_t offset, uint32_t length, void* opaque) = 0;

    //method to allocate pextents, depending on the store state can return single or multiple pextents if there is no contiguous extent available
    virtual int allocate_blocks(uint32_t length, void* opaque, bluestore_extent_vector_t* result) = 0;

    virtual int release_block(uint64_t offset, uint32_t length, void* opaque) = 0;

  };

  struct CompressorInterface
  {
    virtual ~CompressorInterface() {}
    virtual int compress(CompressInfo* cinfo, uint32_t source_offs, uint32_t length, const bufferlist& source, void* opaque, bufferlist* result) = 0;
    virtual int decompress(const bufferlist& source, void* opaque, bufferlist* result) = 0;
  };
  struct CheckSumVerifyInterface
  {
    virtual ~CheckSumVerifyInterface() {}

    virtual int calculate(bluestore_blob_t::CSumType, uint32_t csum_block_size, uint32_t source_offs, const bufferlist& source, void* opaque, vector<char>* csum_data) = 0;
    virtual int verify(bluestore_blob_t::CSumType, uint32_t csum_block_size, const bufferlist& source, void* opaque, const vector<char>& csum_data ) = 0;
  };


  ExtentManager(BlockOpInterface& blockop_inf, CompressorInterface& compressor, CheckSumVerifyInterface& csum_verifier)
    : m_blockop_inf(blockop_inf), m_compressor(compressor), m_csum_verifier(csum_verifier) {
  }

  int write(uint64_t offset, const bufferlist& bl, void* opaque, CheckSumInfo* check_info, CompressInfo* compress_info);
  int read(uint64_t offset, uint32_t length, void* opaque, bufferlist* result);

protected:

  bluestore_blob_map_t m_blobs;
  bluestore_lextent_map_t m_lextents;
  BlockOpInterface& m_blockop_inf;
  CompressorInterface& m_compressor;
  CheckSumVerifyInterface& m_csum_verifier;

  //intermediate data structures used while reading
  struct region_t {
    uint64_t logical_offset;
    uint64_t blob_xoffset,   //region offset within the blob
             ext_xoffset,    //region offset within the pextent
             length;

    region_t(uint64_t offset, uint64_t b_offs, uint64_t x_offs, uint32_t len)
      : logical_offset(offset), blob_xoffset(b_offs), ext_xoffset(x_offs), length(len) {
    }
    region_t(const region_t& from)
      : logical_offset(from.logical_offset), blob_xoffset(from.blob_xoffset), ext_xoffset(from.ext_xoffset), length(from.length) {
    }
  };
  typedef list<region_t> regions2read_t;
  typedef map<const bluestore_blob_t*, regions2read_t> blobs2read_t;
  typedef map<const bluestore_extent_t*, regions2read_t> extents2read_t;
  typedef map<uint64_t, bufferlist> ready_regions_t;


  bluestore_blob_t* get_blob(BlobRef blob_ref);
  bluestore_blob_map_t::iterator get_blob_iterator(BlobRef blob_ref);

  void ref_blob(BlobRef blob_ref, unsigned delta);
  void ref_blob(bluestore_blob_map_t::iterator blob_it);
  void deref_blob(BlobRef blob_ref);

  uint64_t get_read_block_size(const bluestore_blob_t*) const;
  uint64_t get_max_blob_size() const;
  uint64_t get_min_alloc_size() const;

  int read_whole_blob(const bluestore_blob_t*, void* opaque, bufferlist* result);
  int read_extent_sparse(const bluestore_blob_t*, const bluestore_extent_t* extent, regions2read_t::const_iterator begin, regions2read_t::const_iterator end, void* opaque, ready_regions_t* result);
  int blob2read_to_extents2read(const bluestore_blob_t* blob, regions2read_t::const_iterator begin, regions2read_t::const_iterator end, extents2read_t* result);

  int verify_csum(const bluestore_blob_t* blob, uint64_t x_offset, const bufferlist& bl, void* opaque) const;

  int allocate_raw_blob(uint32_t length, void* opaque, CheckSumInfo* check_info, BlobRef* blob, bluestore_blob_map_t::iterator* res_blob_it);
  int compress_and_allocate_blob(uint64_t input_offs, const bufferlist& raw_buffer, void* opaque, CheckSumInfo* check_info, CompressInfo* compress_info, bufferlist* compressed_buffer, BlobRef* blob, bluestore_blob_map_t::iterator* res_blob_it);

  int write_blob(const bluestore_blob_t& blob, uint64_t input_offs, const bufferlist& bl, void* opaque);

  int write_uncompressed(uint64_t offset, const bufferlist& bl, void* opaque, CheckSumInfo* check_info);
  int write_compressed(uint64_t offset, const bufferlist& bl, void* opaque, CheckSumInfo* check_info, CompressInfo* compress_info);

  //Temporary struct to represent lextent along with corresponding pointer to a blob.
  //Valid during single write request handling call.
  //Intended to reduce blobs lookup.
  struct live_lextent_t : public bluestore_lextent_t
  {
    bluestore_blob_map_t::iterator blob_iterator;
    
    live_lextent_t(bluestore_blob_map_t::iterator blob_it, BlobRef blob_ref, uint32_t o, uint32_t l, uint32_t f)
      : bluestore_lextent_t(blob_ref, o, l, f),
      blob_iterator(blob_it)
    {}
  };
  typedef map<uint64_t, live_lextent_t> live_lextent_map_t;

  int apply_lextents(
    live_lextent_map_t& new_lextents,
    const bufferlist& raw_buffer,
    std::vector<bufferlist>* compressed_buffers,
    void* opaque,
    CheckSumInfo* check_info);

  void update_lextents(bluestore_lextent_map_t::iterator start, bluestore_lextent_map_t::iterator end);
  void update_lextents(live_lextent_map_t::iterator cur, live_lextent_map_t::iterator end);

  void release_lextents(bluestore_lextent_map_t::iterator start, bluestore_lextent_map_t::iterator end, bool zero, bool remove_from_primary_map, void* opaque);

};

#endif
