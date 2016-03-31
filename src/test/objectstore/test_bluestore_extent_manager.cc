// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#include "os/bluestore/ExtentManager.h"
#include "gtest/gtest.h"
#include <sstream>
#include <vector>

//#include "include/types.h"
//#include "include/stringify.h"

typedef pair<uint64_t, uint32_t> ReadTuple;
typedef vector<ReadTuple> ReadList;

class TestExtentManager
    : public ExtentManager::DeviceInterface,
      public ExtentManager::CompressorInterface, 
      public ExtentManager {

  enum {
    PEXTENT_BASE = 0x12345, //just to have pextent offsets different from lextent ones
    PEXTENT_ALLOC_UNIT = 0x10000
  };

public:
  TestExtentManager() 
    : ExtentManager::DeviceInterface(),
      ExtentManager::CompressorInterface(),
      ExtentManager( *this, *this) {
  }
  ReadList m_reads;

  void prepareTestSet4SimpleRead() {

    m_lextents[0] = bluestore_lextent_t(0, 0, 0x8000);
    m_pextents[0] = bluestore_pextent_t(PEXTENT_BASE + 0x00000, 1 * PEXTENT_ALLOC_UNIT);

    m_lextents[0x8000] = bluestore_lextent_t(0x8000, 0, 0x2000);
    m_pextents[1] = bluestore_pextent_t(PEXTENT_BASE + 0x10000, 1 * PEXTENT_ALLOC_UNIT);

    //hole at 0x10000~0x6000

    m_lextents[0x16000] = bluestore_lextent_t(2, 0, 0x3000);
    m_pextents[2] = bluestore_pextent_t(PEXTENT_BASE + 0x20000, 1 * PEXTENT_ALLOC_UNIT);

    m_lextents[0x19000] = bluestore_lextent_t(3, 0, 0x17610);
    m_pextents[3] = bluestore_pextent_t(PEXTENT_BASE + 0x40000, 2 * PEXTENT_ALLOC_UNIT);

    m_lextents[0x19000] = bluestore_lextent_t(3, 0, 0x17610);
    m_pextents[3] = bluestore_pextent_t(PEXTENT_BASE + 0x60000, 0x20000);

    //hole at 0x36610~0x39000

    m_lextents[0x39000] = bluestore_lextent_t(4, 0x0, 0x1900);
    m_pextents[4] = bluestore_pextent_t(PEXTENT_BASE + 0x80000, 0x10000);

    m_lextents[0x3a900] = bluestore_lextent_t(5, 0x400, 0x1515);
    m_pextents[5] = bluestore_pextent_t(PEXTENT_BASE + 0x90000, 0x30000);

    m_lextents[0x4c215] = bluestore_lextent_t(6, 0x0, 0x3deb);
    m_pextents[6] = bluestore_pextent_t(PEXTENT_BASE + 0xc0000, 0x10000);

    //hole at 0x50000~
  }

  void reset(bool total) {
    if (total){
      m_lextents.clear();
      m_pextents.clear();
    }
    m_reads.clear();
  }


protected:
  ////////////////DeviceInterface implementation////////////
  virtual uint64_t get_block_size() { return 4096; }

  virtual int read_block(uint64_t offset, uint32_t length, void* opaque, bufferlist* result)
  {
    uint64_t block_size = get_block_size();
    offset -= PEXTENT_BASE;
    assert(length > 0);
    assert((length % block_size) == 0);
    assert((offset % block_size) == 0);

    auto o0 = (offset >> 12) << 4; //pblock no * 16

    bufferptr buf(length);
    for (unsigned o = 0; o < length; o++){
      buf[o] = (o + o0) & 0xff;  //fill resulting buffer with some checksum pattern
    }
    result->append(buf);
    m_reads.push_back(ReadList::value_type(offset, length));
    return 0;
  }

  ////////////////CompressorInterface implementation////////
  virtual int decompress(uint32_t alg, const bufferlist& source, void* opaque, bufferlist* result) { 
    result->append(source); 
    return 0;
  }

};

TEST(bluestore_extent_manager, read)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SimpleRead();

  mgr.read(0, 128, NULL, &res);
  ASSERT_EQ(128u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0, 4096), mgr.m_reads[0]);
  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(1u, unsigned(res[1]));
  ASSERT_EQ(127u, unsigned(res[127]));

  mgr.reset(true);
  res.clear();

}
