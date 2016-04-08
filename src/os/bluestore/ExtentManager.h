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
#define CEPH_OSD_EXTENT_ANAGER_H

#include <list>
#include <map>

#include "include/buffer.h"
#include "bluestore_types.h"

class ExtentManager{
  
public:

  struct DeviceInterface
  {
    virtual ~DeviceInterface() {}
    virtual uint64_t get_block_size() = 0;

    virtual int read_block(uint64_t offset, uint32_t length, void* opaque, bufferlist* result) = 0;

  };
  struct CompressorInterface
  {
    virtual ~CompressorInterface() {}
    virtual int decompress(const bufferlist& source, void* opaque, bufferlist* result) = 0;
  };

  ExtentManager(DeviceInterface& device, CompressorInterface& compressor)
    : m_device(device), m_compressor(compressor) {
  }

  int write(uint64_t offset, uint32_t length, void* opaque, const bufferlist& bl);
  int read(uint64_t offset, uint32_t length, void* opaque, bufferlist* result);

protected:

  bluestore_blob_map_t m_blobs;
  bluestore_lextent_map_t m_lextents;
  DeviceInterface& m_device;
  CompressorInterface& m_compressor;

  //intermediate data structures used while reading
  struct region_t {
    uint64_t logical_offset;
    uint64_t x_offset, length;

    region_t(uint64_t offset, uint64_t x_offs, uint32_t len)
      : logical_offset(offset), x_offset(x_offs), length(len) {
    }
    region_t(const region_t& from)
      : logical_offset(from.logical_offset), x_offset(from.x_offset), length(from.length) {
    }
  };
  typedef list<region_t> regions2read_t;
  typedef map<const bluestore_blob_t*, regions2read_t> blobs2read_t;
  typedef map<const bluestore_extent_t*, regions2read_t> extents2read_t;
  typedef map<uint64_t, bufferlist> ready_regions_t;


  bluestore_blob_t* get_blob( BlobRef pextent);
  uint64_t get_read_block_size(const bluestore_blob_t*) const;
  
  int read_whole_blob(const bluestore_blob_t*, void* opaque, bufferlist* result);
  int read_extent_sparse(const bluestore_extent_t* extent, void* opaque, regions2read_t::const_iterator begin, regions2read_t::const_iterator end, ready_regions_t* result);
  int regions2read_to_extents2read(const bluestore_blob_t* blob, regions2read_t::const_iterator begin, regions2read_t::const_iterator end, extents2read_t* result);

  int verify_csum( const bluestore_blob_t* blob, uint64_t x_offset, const bufferlist& bl) const;
};

#endif
