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

//FIXME: replace index access to m_reads with std::find in 
//TEST(bluestore_extent_manager, read)

#include "os/bluestore/ExtentManager.h"
#include "gtest/gtest.h"
#include <sstream>
#include <vector>


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

  void prepareTestSet4SimpleRead( bool compress )

    m_lextents[0] = bluestore_lextent_t(0, 0, 0x8000);
    m_pextents[0] = bluestore_pextent_t(PEXTENT_BASE + 0x00000, 1 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    m_lextents[0x8000] = bluestore_lextent_t(1, 0, 0x2000);
    m_pextents[1] = bluestore_pextent_t(PEXTENT_BASE + 0x10000, 1 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    //hole at 0x0a000~0xc000

    m_lextents[0x16000] = bluestore_lextent_t(2, 0, 0x3000);
    m_pextents[2] = bluestore_pextent_t(PEXTENT_BASE + 0x20000, 1 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    m_lextents[0x19000] = bluestore_lextent_t(3, 0, 0x17610);
    m_pextents[3] = bluestore_pextent_t(PEXTENT_BASE + 0x40000, 2 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    //hole at 0x30610~0x29f0

    m_lextents[0x33000] = bluestore_lextent_t(4, 0x0, 0x1900);
    m_pextents[4] = bluestore_pextent_t(PEXTENT_BASE + 0x80000, 1 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    m_lextents[0x34900] = bluestore_lextent_t(5, 0x400, 0x1515);
    m_pextents[5] = bluestore_pextent_t(PEXTENT_BASE + 0x90000, 3 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    m_lextents[0x35e15] = bluestore_lextent_t(6, 0x0, 0xa1eb);
    m_pextents[6] = bluestore_pextent_t(PEXTENT_BASE + 0xc0000, 1 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    //hole at 0x40000~
  }

  void prepareTestSet4SplitPExtentRead(bool compress) {

    //hole at 0~100
    m_lextents[0x100] = bluestore_lextent_t(0, 0, 0x8000);
    m_pextents[0] = bluestore_pextent_t(PEXTENT_BASE + 0xa0000, 1 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    m_lextents[0x8100] = bluestore_lextent_t(1, 0, 0x200);
    m_pextents[1] = bluestore_pextent_t(PEXTENT_BASE + 0x10000, 1 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    m_lextents[0x8300] = bluestore_lextent_t(2, 0, 0x1100);
    m_pextents[2] = bluestore_pextent_t(PEXTENT_BASE + 0x20000, 2 * PEXTENT_ALLOC_UNIT, compress ? 1 : 0);

    //hole at 0x9400~0x100

    m_lextents[0x9500] = bluestore_lextent_t(0, 0x9400, 0x200);

    //hole at 0x9700~
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

  virtual int read_block(uint64_t offset0, uint32_t length, void* opaque, bufferlist* result)
  {
    uint64_t block_size = get_block_size();
    offset0 -= PEXTENT_BASE;
    assert(length > 0);
    assert((length % block_size) == 0);
    assert((offset0 % block_size) == 0);

    auto offset = offset0;

    bufferptr buf(length);
    for (unsigned o = 0; o < length; o++){
      auto o0 = (offset >> 12); //pblock no
      buf[o] = (o + o0) & 0xff;  //fill resulting buffer with some checksum pattern
      ++offset;
    }
    result->append(buf);
    m_reads.push_back(ReadList::value_type(offset0, length));
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
  mgr.prepareTestSet4SimpleRead(false);

  mgr.read(0, 128, NULL, &res);
  ASSERT_EQ(128u, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0, 4096), mgr.m_reads[0]);
  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(1u, unsigned(res[1]));
  ASSERT_EQ(127u, unsigned(res[127]));

  mgr.reset(false);
  res.clear();

  //read 0x7000~0x1000, 0x8000~0x2000, 0xa00~0x1000(unallocated)
  mgr.read(0x7000, 0x4000, NULL, &res);
  ASSERT_EQ(0x4000u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0x7000, 0x1000), mgr.m_reads[0]);
  ASSERT_EQ(ReadTuple(0x10000, 0x2000), mgr.m_reads[1]);

  ASSERT_EQ(unsigned((0x7000 >> 12) & 0xff), unsigned(res[0]));
  ASSERT_EQ(unsigned(((0x7001 >> 12) + 1) & 0xff), unsigned(res[1]));
  ASSERT_EQ(unsigned(((0x7fff >> 12) + 0xfff) & 0xff), unsigned(res[0x0fff]));

  ASSERT_EQ(unsigned((0x10000>>12) & 0xff), unsigned(res[0x1000]));
  ASSERT_EQ(unsigned(((0x10001 >> 12)+1) & 0xff), unsigned(res[0x1001]));
  ASSERT_EQ(unsigned(((0x10fff >> 12)+0x0fff) & 0xff), unsigned(res[0x1fff]));
  ASSERT_EQ(unsigned(((0x11000 >> 12)+0x1000) & 0xff), unsigned(res[0x2000]));
  ASSERT_EQ(unsigned(((0x11001 >> 12)+0x1001) & 0xff), unsigned(res[0x2001]));
  ASSERT_EQ(unsigned(((0x11fff >> 12) + 0x1fff) & 0xff), unsigned(res[0x2fff]));

  ASSERT_EQ(0u, unsigned(res[0x3000]));
  ASSERT_EQ(0u, unsigned(res[0x3001]));
  ASSERT_EQ(0u, unsigned(res[0x3fff]));

  mgr.reset(false);
  res.clear();

  //read 0x7800~0x0800, 0x8000~0x0200
  mgr.read(0x7800, 0x0a00, NULL, &res);
  ASSERT_EQ(0x0a00u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0x7000, 0x1000), mgr.m_reads[0]);
  ASSERT_EQ(ReadTuple(0x10000, 0x1000), mgr.m_reads[1]);

  ASSERT_EQ(unsigned((0x7800 >> 12) & 0xff), unsigned(res[0]));
  ASSERT_EQ(unsigned(((0x7801 >> 12) + 1) & 0xff), unsigned(res[1]));
  ASSERT_EQ(unsigned(((0x7fff >> 12) + 0x7ff) & 0xff), unsigned(res[0x07ff]));

  ASSERT_EQ(unsigned((0x10000 >> 12) & 0xff), unsigned(res[0x0800]));
  ASSERT_EQ(unsigned(((0x10801 >> 12) + 1) & 0xff), unsigned(res[0x0801]));
  ASSERT_EQ(unsigned(((0x109ff >> 12) + 0x01ff) & 0xff), unsigned(res[0x09ff]));

  mgr.reset(false);
  res.clear();

  //read 0x77f8~0x0808, 0x8000~0x0002
  mgr.read(0x77f8, 0x080a, NULL, &res);
  ASSERT_EQ(0x080au, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0x7000, 0x1000), mgr.m_reads[0]);
  ASSERT_EQ(ReadTuple(0x10000, 0x1000), mgr.m_reads[1]);

  ASSERT_EQ(unsigned(((0x77f8 >> 12) + 0x07f8) & 0xff), (unsigned char)res[0]);
  ASSERT_EQ(unsigned(((0x77f9 >> 12) + 0x07f9) & 0xff), (unsigned char)res[1]);
  ASSERT_EQ(unsigned(((0x7fff >> 12) + 0x0fff) & 0xff), (unsigned char)res[0x0807]);

  ASSERT_EQ(unsigned((0x10000 >> 12) & 0xff), unsigned(res[0x0808]));
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 1) & 0xff), unsigned(res[0x0809]));

  mgr.reset(false);
  res.clear();

  //read 0x108f8~0x1808 (unallocated)
  mgr.read(0x108f8, 0x1808, NULL, &res);
  ASSERT_EQ(0x1808u, res.length());
  ASSERT_EQ(0u, mgr.m_reads.size());

  ASSERT_EQ(0u, unsigned(res[0x0000]));
  ASSERT_EQ(0u, unsigned(res[0x0001]));
  ASSERT_EQ(0u, unsigned(res[0x1807]));

  mgr.reset(false);
  res.clear();

  //read 0x15ffe~2, 0x16000~0x3000, 0x19000~0x17610, 0x30610~2 
  mgr.read(0x15ffe, 0x1a614, NULL, &res);
  ASSERT_EQ(0x1a614u, res.length());
  ASSERT_EQ(2u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0x20000, 0x3000), mgr.m_reads[0]);
  ASSERT_EQ(ReadTuple(0x40000, 0x18000), mgr.m_reads[1]);

  ASSERT_EQ(0u, unsigned(res[0x0000]));
  ASSERT_EQ(0u, unsigned(res[0x0001]));
  ASSERT_EQ(unsigned((0x20000 >> 12) & 0xff), unsigned(res[0x0002]));
  ASSERT_EQ(unsigned(((0x22fff >> 12) + 0x2fff) & 0xff), unsigned(res[0x3001]));
  ASSERT_EQ(unsigned((0x40000 >> 12) & 0xff), unsigned(res[0x3002]));
  ASSERT_EQ(unsigned(((0x5760f >> 12) + 0x1760f) & 0xff), unsigned(res[0x1a611]));
  ASSERT_EQ(0u, unsigned(res[0x1a612]));
  ASSERT_EQ(0u, unsigned(res[0x1a613]));


  mgr.reset(false);
  res.clear();

  //read 0x15ffe~2, 0x16000~0x3000, 0x19000~0x17610, 0x30610~29f0, 0x33000~0x1001
  mgr.read(0x15ffe, 0x1e003, NULL, &res);
  ASSERT_EQ(0x1e003u, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0x20000, 0x3000), mgr.m_reads[0]);
  ASSERT_EQ(ReadTuple(0x40000, 0x18000), mgr.m_reads[1]);
  ASSERT_EQ(ReadTuple(0x80000, 0x2000), mgr.m_reads[2]);

  ASSERT_EQ(0u, unsigned(res[0x0000]));
  ASSERT_EQ(0u, unsigned(res[0x0001]));
  ASSERT_EQ(unsigned((0x20000 >> 12) & 0xff), unsigned(res[0x0002]));
  ASSERT_EQ(unsigned(((0x22fff >> 12) + 0x2fff) & 0xff), unsigned(res[0x3001]));
  ASSERT_EQ(unsigned((0x40000 >> 12) & 0xff), unsigned(res[0x3002]));
  ASSERT_EQ(unsigned(((0x5760f >> 12) + 0x1760f) & 0xff), unsigned(res[0x1a611]));
  ASSERT_EQ(0u, unsigned(res[0x1a612]));
  ASSERT_EQ(0u, unsigned(res[0x1a613]));
  ASSERT_EQ(0u, unsigned(res[0x1d001]));
  ASSERT_EQ(unsigned(((0x80000 >> 12) + 0) & 0xff), (unsigned char)res[0x1d002]);
  ASSERT_EQ(unsigned(((0x80001 >> 12) + 1) & 0xff), (unsigned char)res[0x1d003]);
  ASSERT_EQ(unsigned(((0x81000 >> 12) + 0x1000) & 0xff), (unsigned char)res[0x1e002]);

  mgr.reset(false);
  res.clear();

  //read 0x34902~2
  mgr.read(0x34902, 0x2, NULL, &res);
  ASSERT_EQ(0x2, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0x90000, 0x1000), mgr.m_reads[0]);

  ASSERT_EQ(unsigned(((0x90402 >> 12) + 2)& 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x90403 >> 12) + 3)& 0xff), (unsigned char)res[0x1]);

  mgr.reset(false);
  res.clear();

  //read 0x34902~0x1001
  mgr.read(0x34902, 0x1001, NULL, &res);
  ASSERT_EQ(0x1001, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0x90000, 0x2000), mgr.m_reads[0]);

  ASSERT_EQ(unsigned(((0x90402 >> 12) + 2) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x90402 >> 12) + 3) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0x91401 >> 12) + 0x1001) & 0xff), (unsigned char)res[0xfff]);
  ASSERT_EQ(unsigned(((0x91402 >> 12) + 0x1002) & 0xff), (unsigned char)res[0x1000]);

  mgr.reset(false);
  res.clear();

  //read 0x348fe~2, 0x34900~0x1515, 35e15~0x00ea
  mgr.read(0x348fe, 0x1601, NULL, &res);
  ASSERT_EQ(0x1601, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0x81000, 0x1000), mgr.m_reads[0]);
  ASSERT_EQ(ReadTuple(0x90000, 0x2000), mgr.m_reads[1]);
  ASSERT_EQ(ReadTuple(0xc0000, 0x1000), mgr.m_reads[2]);

  ASSERT_EQ(unsigned(((0x818fe >> 12) + 0x18fe) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0x818ff >> 12) + 0x18ff) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0x90400 >> 12) + 0x0) & 0xff), (unsigned char)res[0x2]);
  ASSERT_EQ(unsigned(((0x91914 >> 12) + 0x1514) & 0xff), (unsigned char)res[0x1516]);

  ASSERT_EQ(unsigned(((0xc0000 >> 12) + 0) & 0xff), (unsigned char)res[0x1517]);
  ASSERT_EQ(unsigned(((0xc0001 >> 12) + 1) & 0xff), (unsigned char)res[0x1518]);
  ASSERT_EQ(unsigned(((0xc00e9 >> 12) + 0xe9) & 0xff), (unsigned char)res[0x1600]);

  mgr.reset(false);
  res.clear();

  //read 0x3fff0~0x10, 0x40000~0x12000 (unallocated)
  mgr.read(0x3fff0, 0x12010, NULL, &res);
  ASSERT_EQ(0x12010, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0xca000, 0x1000), mgr.m_reads[0]);

  ASSERT_EQ(unsigned(((0xca000 >> 12) + 0xa1db) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xca00f >> 12) + 0xa1ea) & 0xff), (unsigned char)res[0xf]);
  ASSERT_EQ(0u, unsigned(res[0x10]));
  ASSERT_EQ(0u, unsigned(res[0x11]));
  ASSERT_EQ(0u, unsigned(res[0x1200f]));

  mgr.reset(false);
  res.clear();

  //read 0x40700~0x0a02 (unallocated)
  mgr.read(0x40700, 0xa02, NULL, &res);
  ASSERT_EQ(0xa02, res.length());
  ASSERT_EQ(0u, mgr.m_reads.size());

  ASSERT_EQ(0u, unsigned(res[0x0]));
  ASSERT_EQ(0u, unsigned(res[0x1]));
  ASSERT_EQ(0u, unsigned(res[0xa01]));

  mgr.reset(false);
  res.clear();
}

TEST(bluestore_extent_manager, read_splitted_extent)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SplitPExtentRead(false);

  //0x50~0xb0 (unalloc), 0x100~0x1200
  mgr.read(0x50, 0x12b0, NULL, &res);
  ASSERT_EQ(0x12b0, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0xa0000, 0x2000), mgr.m_reads[0]);

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(0u, unsigned(res[1]));
  ASSERT_EQ(0u, unsigned(res[0xaf]));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0xb0]);

  mgr.reset(false);
  res.clear();

  // 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x100
  mgr.read(0x100, 0x9500, NULL, &res);
  ASSERT_EQ(0x9500, res.length());
  ASSERT_EQ(4u, mgr.m_reads.size());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0xa0000, 0x8000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0xa9000, 0x1000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0x10000, 0x1000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0x20000, 0x2000)), mgr.m_reads.end());

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x81ff]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x92ff]);
  ASSERT_EQ(0u, unsigned(res[0x9300]));
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x93ff]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9400]);
  ASSERT_EQ(unsigned(((0xa90ff >> 12) + 0xff) & 0xff), (unsigned char)res[0x94ff]);

  mgr.reset(false);
  res.clear();

  // 0xff~1, 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x200, 0x9700~1
  mgr.read(0xff, 0x9602, NULL, &res);
  ASSERT_EQ(0x9602, res.length());
  ASSERT_EQ(4u, mgr.m_reads.size());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0xa0000, 0x8000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0xa9000, 0x1000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0x10000, 0x1000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0x20000, 0x2000)), mgr.m_reads.end());

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8001]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8201]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x9300]);
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x9302]));
  ASSERT_EQ(0u, unsigned(res[0x9400]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9401]);
  ASSERT_EQ(unsigned(((0xa91ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x9600]);
  ASSERT_EQ(0u, unsigned(res[0x9601]));

  mgr.reset(false);
  res.clear();
}

TEST(bluestore_extent_manager, read_splitted_extent_compressed)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SplitPExtentRead(true);

  //0x50~0xb0 (unalloc), 0x100~0x1200
  mgr.read(0x50, 0x12b0, NULL, &res);
  ASSERT_EQ(0x12b0, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0xa0000, 0x10000), mgr.m_reads[0]);

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(0u, unsigned(res[1]));
  ASSERT_EQ(0u, unsigned(res[0xaf]));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0xb0]);

  mgr.reset(false);
  res.clear();

  // 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x100
  mgr.read(0x100, 0x9500, NULL, &res);
  ASSERT_EQ(0x9500, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0xa0000, 0x10000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0x10000, 0x10000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0x20000, 0x20000)), mgr.m_reads.end());

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x81ff]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x92ff]);
  ASSERT_EQ(0u, unsigned(res[0x9300]));
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x93ff]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9400]);
  ASSERT_EQ(unsigned(((0xa90ff >> 12) + 0xff) & 0xff), (unsigned char)res[0x94ff]);

  mgr.reset(false);
  res.clear();

  // 0xff~1, 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x200, 0x9700~1
  mgr.read(0xff, 0x9602, NULL, &res);
  ASSERT_EQ(0x9602, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0xa0000, 0x10000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0x10000, 0x10000)), mgr.m_reads.end());
  ASSERT_NE( std::find(mgr.m_reads.begin(), mgr.m_reads.end(), ReadTuple(0x20000, 0x20000)), mgr.m_reads.end());

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8001]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8201]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x9300]);
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x9302]));
  ASSERT_EQ(0u, unsigned(res[0x9400]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9401]);
  ASSERT_EQ(unsigned(((0xa91ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x9600]);
  ASSERT_EQ(0u, unsigned(res[0x9601]));

  mgr.reset(false);
  res.clear();
}

TEST(bluestore_extent_manager, read_splitted_extent_compressed)
{
  TestExtentManager mgr;
  bufferlist res;
  mgr.reset(true);
  mgr.prepareTestSet4SplitPExtentRead(true);

  //0x50~0xb0 (unalloc), 0x100~0x1200
  mgr.read(0x50, 0x12b0, NULL, &res);
  ASSERT_EQ(0x12b0, res.length());
  ASSERT_EQ(1u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0xa0000, 0x10000), mgr.m_reads[0]);

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(0u, unsigned(res[1]));
  ASSERT_EQ(0u, unsigned(res[0xaf]));

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0xb0]);

  mgr.reset(false);
  res.clear();

  // 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x100
  mgr.read(0x100, 0x9500, NULL, &res);
  ASSERT_EQ(0x9500, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0xa0000, 0x10000), mgr.m_reads[0]);
  ASSERT_EQ(ReadTuple(0x10000, 0x10000), mgr.m_reads[2]);
  ASSERT_EQ(ReadTuple(0x20000, 0x20000), mgr.m_reads[3]);

  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x0]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x7fff]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x81ff]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x92ff]);
  ASSERT_EQ(0u, unsigned(res[0x9300]));
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x93ff]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9400]);
  ASSERT_EQ(unsigned(((0xa90ff >> 12) + 0xff) & 0xff), (unsigned char)res[0x94ff]);

  mgr.reset(false);
  res.clear();

  // 0xff~1, 0x100~0x8000, 0x8100~0x200, 0x8300~0x1100, 0x9400~100 (unalloc), 0x9500~0x200, 0x9700~1
  mgr.read(0xff, 0x9602, NULL, &res);
  ASSERT_EQ(0x9602, res.length());
  ASSERT_EQ(3u, mgr.m_reads.size());
  ASSERT_EQ(ReadTuple(0xa0000, 0x10000), mgr.m_reads[0]);
  ASSERT_EQ(ReadTuple(0x10000, 0x10000), mgr.m_reads[2]);
  ASSERT_EQ(ReadTuple(0x20000, 0x20000), mgr.m_reads[3]);

  ASSERT_EQ(0u, unsigned(res[0]));
  ASSERT_EQ(unsigned(((0xa0000 >> 12) + 0) & 0xff), (unsigned char)res[0x1]);
  ASSERT_EQ(unsigned(((0xa7fff >> 12) + 0x7fff) & 0xff), (unsigned char)res[0x8000]);
  ASSERT_EQ(unsigned(((0x10000 >> 12) + 0) & 0xff), (unsigned char)res[0x8001]);
  ASSERT_EQ(unsigned(((0x101ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x8200]);
  ASSERT_EQ(unsigned(((0x20000 >> 12) + 0) & 0xff), (unsigned char)res[0x8201]);
  ASSERT_EQ(unsigned(((0x210ff >> 12) + 0x10ff) & 0xff), (unsigned char)res[0x9300]);
  ASSERT_EQ(0u, unsigned(res[0x9301]));
  ASSERT_EQ(0u, unsigned(res[0x9302]));
  ASSERT_EQ(0u, unsigned(res[0x9400]));
  ASSERT_EQ(unsigned(((0xa9000 >> 12) + 0) & 0xff), (unsigned char)res[0x9401]);
  ASSERT_EQ(unsigned(((0xa91ff >> 12) + 0x1ff) & 0xff), (unsigned char)res[0x9600]);
  ASSERT_EQ(0u, unsigned(res[0x9601]));

  mgr.reset(false);
  res.clear();
}