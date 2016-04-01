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

#include "ExtentManager.h"

bluestore_pextent_t* ExtentManager::get_pextent(PExtentRef pextent)
{
  bluestore_pextent_t* res = nullptr;
  bluestore_pextent_map_t::iterator it = m_pextents.find(pextent);
  if (it != m_pextents.end())
    res = &it->second;
  return res;
}

int  ExtentManager::read(uint64_t offset, uint32_t length, void* opaque, bufferlist* result)
{
  result->clear();

  bluestore_lextent_map_t::iterator bp = m_lextents.upper_bound(offset);
  uint32_t l = length;
  uint64_t o = offset;
  if (bp == m_lextents.begin() && offset+length <= bp->first){
    result->append_zero(length);
    return 0;
  } else if(bp == m_lextents.begin() ) {
    o = bp->first;
    l -= bp->first - offset;
  } else
    --bp;

  //build extent list to read
  pextents2read_t ext2read;
  while (l > 0 && bp != m_lextents.end()) {
    bluestore_pextent_t* eptr = get_pextent(bp->second.pextent);
    assert(eptr != nullptr);
    unsigned l2read;
    if(o >= bp->first && o < bp->first + bp->second.length) {
      pextents2read_t::iterator it = ext2read.find(eptr);
      unsigned r_off = o - bp->first;
      l2read = MIN( l, bp->second.length - r_off );
      if (it != ext2read.end())
        it->second.push_back(region_t(o, r_off + bp->second.x_offset, l2read));
      else
        ext2read[eptr].push_back(region_t(o, r_off + bp->second.x_offset, l2read));
      ++bp;
    } else if(o >= bp->first + bp->second.length){
      //handling the case when the first lookup get into the previous block due to the hole
      l2read = 0;
      ++bp;
    } else {
      //hole found
      l2read = MIN( l, bp->first -o);
    }
    o += l2read;
    l -= l2read;
  }

  ready_regions_t ready_regions;

  //enumerate and read/decompress desired extents
  pextents2read_t::iterator e2r_it = ext2read.begin();
  while (e2r_it != ext2read.end()) {
    bluestore_pextent_t* eptr = e2r_it->first;
    regions2read_t r2r = e2r_it->second;
    regions2read_t::const_iterator r2r_it = r2r.cbegin();
    if (eptr->compression) {
      bufferlist compressed_bl, raw_bl;
      
      int r = read_extent_total(eptr, opaque, &compressed_bl);
      if (r < 0)
	return r;

      r = m_compressor.decompress(eptr->compression, compressed_bl, opaque, &raw_bl);
      if (r < 0)
	return r;

      while (r2r_it != r2r.end()) {
	ready_regions[r2r_it->logical_offset].substr_of(raw_bl, r2r_it->x_offset, r2r_it->length);
	++r2r_it;
      }

    } else {
      int r = read_extent_sparse(eptr, opaque, r2r_it, r2r.cend(), &ready_regions);
      if (r < 0)
	return r;
    }
    ++e2r_it;
  }

  //form a resulting buffer
  ready_regions_t::iterator rr_it = ready_regions.begin();
  o = offset;

  while ( rr_it != ready_regions.end()) {
    if (o < rr_it->first)
      result->append_zero(rr_it->first - o);
    o = rr_it->first + rr_it->second.length();
    assert(o <= offset + length);
    result->claim_append(rr_it->second);
    ++rr_it;
  }
  result->append_zero(offset + length - o);

  return 0;
}

int ExtentManager::read_extent_total(bluestore_pextent_t* pextent, void* opaque, bufferlist* result)
{
  result->clear();

  uint64_t r_len = ROUND_UP_TO(pextent->length, m_device.get_block_size());
  bufferlist bl;
//  dout(30) << __func__ << "  reading " << pextent->offset << "~" << r_len << dendl;
  int r = m_device.read_block(pextent->offset, r_len, opaque, &bl);
  if (r < 0) {
    return r;
  }
  result->substr_of(bl, 0, pextent->length);
  return 0;
}

int ExtentManager::read_extent_sparse(bluestore_pextent_t* pextent, void* opaque, ExtentManager::regions2read_t::const_iterator cur, ExtentManager::regions2read_t::const_iterator end, ExtentManager::ready_regions_t* result)
{
  uint64_t block_size = m_device.get_block_size();
  while (cur != end) {

    assert(cur->x_offset + cur->length <= pextent->length);
    uint64_t r_off = cur->x_offset;
    uint64_t front_extra = r_off % block_size;
    r_off -= front_extra;

    uint64_t x_len = cur->length;
    uint64_t r_len = ROUND_UP_TO(x_len + front_extra, block_size);

//    dout(30) << __func__ << "  reading " << r_off << "~" << r_len << dendl;
    bufferlist bl;
    int r = m_device.read_block(r_off + pextent->offset, r_len, opaque, &bl);
    if (r < 0) {
      return r;
    }
	
    bufferlist u;
    u.substr_of(bl, front_extra, x_len);
    (*result)[cur->logical_offset].claim_append(u);

    ++cur;
  }
  return 0;
}
