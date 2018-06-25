// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * In memory space allocator test cases.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "include/Context.h"
#include "os/bluestore/Allocator.h"

#include <boost/random/uniform_int.hpp>
typedef boost::mt11213b gen_type;

#if GTEST_HAS_PARAM_TEST

class AllocTest : public ::testing::TestWithParam<const char*> {

public:
  boost::scoped_ptr<Allocator> alloc;
  AllocTest(): alloc(0) { }
  void init_alloc(int64_t size, uint64_t min_alloc_size) {
    std::cout << "Creating alloc type " << string(GetParam()) << " \n";
    alloc.reset(Allocator::create(g_ceph_context, string(GetParam()), size,
				  min_alloc_size));
  }

  void init_close() {
    alloc.reset(0);
  }
};

TEST_P(AllocTest, test_alloc_init)
{
  int64_t blocks = 64;
  init_alloc(blocks, 1);
  ASSERT_EQ(0U, alloc->get_free());
  alloc->shutdown(); 
  blocks = 1024 * 2 + 16;
  init_alloc(blocks, 1);
  ASSERT_EQ(0U, alloc->get_free());
  alloc->shutdown(); 
  blocks = 1024 * 2;
  init_alloc(blocks, 1);
  ASSERT_EQ(alloc->get_free(), (uint64_t) 0);
}

TEST_P(AllocTest, test_alloc_min_alloc)
{
  int64_t block_size = 1024;
  int64_t capacity = 4 * 1024 * block_size;

  {
    init_alloc(capacity, block_size);

    alloc->init_add_free(block_size, block_size);
    PExtentVector extents;
    EXPECT_EQ(block_size, alloc->allocate(block_size, block_size,
					  0, (int64_t) 0, &extents));
  }

  /*
   * Allocate extent and make sure all comes in single extent.
   */   
  {
    alloc->init_add_free(0, block_size * 4);
    PExtentVector extents;
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      0, (int64_t) 0, &extents));
    EXPECT_EQ(1u, extents.size());
    EXPECT_EQ(extents[0].length, 4 * block_size);
  }

  /*
   * Allocate extent and make sure we get two different extents.
   */
  {
    alloc->init_add_free(0, block_size * 2);
    alloc->init_add_free(3 * block_size, block_size * 2);
    PExtentVector extents;
  
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      0, (int64_t) 0, &extents));
    EXPECT_EQ(2u, extents.size());
    EXPECT_EQ(extents[0].length, 2 * block_size);
    EXPECT_EQ(extents[1].length, 2 * block_size);
  }
  alloc->shutdown();
}

TEST_P(AllocTest, test_alloc_min_max_alloc)
{
  int64_t block_size = 1024;

  int64_t capacity = 4 * 1024 * block_size;
  init_alloc(capacity, block_size);

  /*
   * Make sure we get all extents different when
   * min_alloc_size == max_alloc_size
   */
  {
    alloc->init_add_free(0, block_size * 4);
    PExtentVector extents;
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      block_size, (int64_t) 0, &extents));
    for (auto e : extents) {
      EXPECT_EQ(e.length, block_size);
    }
    EXPECT_EQ(4u, extents.size());
  }


  /*
   * Make sure we get extents of length max_alloc size
   * when max alloc size > min_alloc size
   */
  {
    alloc->init_add_free(0, block_size * 4);
    PExtentVector extents;
    EXPECT_EQ(4*block_size,
	      alloc->allocate(4 * (uint64_t)block_size, (uint64_t) block_size,
			      2 * block_size, (int64_t) 0, &extents));
    EXPECT_EQ(2u, extents.size());
    for (auto& e : extents) {
      EXPECT_EQ(e.length, block_size * 2);
    }
  }

  /*
   * Make sure allocations are of min_alloc_size when min_alloc_size > block_size.
   */
  {
    alloc->init_add_free(0, block_size * 1024);
    PExtentVector extents;
    EXPECT_EQ(1024 * block_size,
	      alloc->allocate(1024 * (uint64_t)block_size,
			      (uint64_t) block_size * 4,
			      block_size * 4, (int64_t) 0, &extents));
    for (auto& e : extents) {
      EXPECT_EQ(e.length, block_size * 4);
    }
    EXPECT_EQ(1024u/4, extents.size());
  }

  /*
   * Allocate and free.
   */
  {
    alloc->init_add_free(0, block_size * 16);
    PExtentVector extents;
    EXPECT_EQ(16 * block_size,
	      alloc->allocate(16 * (uint64_t)block_size, (uint64_t) block_size,
			      2 * block_size, (int64_t) 0, &extents));

    EXPECT_EQ(extents.size(), 8u);
    for (auto& e : extents) {
      EXPECT_EQ(e.length, 2 * block_size);
    }
  }
}

TEST_P(AllocTest, test_alloc_failure)
{
  int64_t block_size = 1024;
  int64_t capacity = 4 * 1024 * block_size;

  init_alloc(capacity, block_size);
  {
    alloc->init_add_free(0, block_size * 256);
    alloc->init_add_free(block_size * 512, block_size * 256);

    PExtentVector extents;
    EXPECT_EQ(512 * block_size,
	      alloc->allocate(512 * (uint64_t)block_size,
			      (uint64_t) block_size * 256,
			      block_size * 256, (int64_t) 0, &extents));
    alloc->init_add_free(0, block_size * 256);
    alloc->init_add_free(block_size * 512, block_size * 256);
    extents.clear();
    EXPECT_EQ(-ENOSPC,
	      alloc->allocate(512 * (uint64_t)block_size,
			      (uint64_t) block_size * 512,
			      block_size * 512, (int64_t) 0, &extents));
  }
}

TEST_P(AllocTest, test_alloc_big)
{
  int64_t block_size = 4096;
  int64_t blocks = 104857600;
  int64_t mas = 4096;
  init_alloc(blocks*block_size, block_size);
  alloc->init_add_free(2*block_size, (blocks-2)*block_size);
  for (int64_t big = mas; big < 1048576*128; big*=2) {
    cout << big << std::endl;
    PExtentVector extents;
    EXPECT_EQ(big,
	      alloc->allocate(big, mas, 0, &extents));
  }
}

TEST_P(AllocTest, test_alloc_non_aligned_len)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 100;
  int64_t want_size = 1 << 22;
  int64_t alloc_unit = 1 << 20;
  
  init_alloc(blocks*block_size, block_size);
  alloc->init_add_free(0, 2097152);
  alloc->init_add_free(2097152, 1064960);
  alloc->init_add_free(3670016, 2097152);

  PExtentVector extents;
  EXPECT_EQ(want_size, alloc->allocate(want_size, alloc_unit, 0, &extents));
}

TEST_P(AllocTest, test_alloc_fragmentation)
{
  uint64_t capacity = 4 * 1024 * 1024;
  uint64_t alloc_unit = 4096;
  uint64_t want_size = alloc_unit;
  PExtentVector allocated, tmp;
  
  init_alloc(capacity, alloc_unit);
  alloc->init_add_free(0, capacity);
  bool bitmap_alloc = GetParam() == std::string("bitmap");
  
  EXPECT_EQ(0.0, alloc->get_fragmentation(alloc_unit));

  for (size_t i = 0; i < capacity / alloc_unit; ++i)
  {
    tmp.clear();
    EXPECT_EQ(want_size, alloc->allocate(want_size, alloc_unit, 0, 0, &tmp));
    allocated.insert(allocated.end(), tmp.begin(), tmp.end());

    // bitmap fragmentation calculation doesn't provide such constant
    // estimate
    if (!bitmap_alloc) {
      EXPECT_EQ(0.0, alloc->get_fragmentation(alloc_unit));
    }
  }
  EXPECT_EQ(-ENOSPC, alloc->allocate(want_size, alloc_unit, 0, 0, &tmp));

  for (size_t i = 0; i < allocated.size(); i += 2)
  {
    interval_set<uint64_t> release_set;
    release_set.insert(allocated[i].offset, allocated[i].length);
    alloc->release(release_set);
  }
  EXPECT_EQ(1.0, alloc->get_fragmentation(alloc_unit));
  for (size_t i = 1; i < allocated.size() / 2; i += 2)
  {
    interval_set<uint64_t> release_set;
    release_set.insert(allocated[i].offset, allocated[i].length);
    alloc->release(release_set);
  }
  if (bitmap_alloc) {
    // fragmentation = one l1 slot is free + one l1 slot is partial
    EXPECT_EQ(50, uint64_t(alloc->get_fragmentation(alloc_unit) * 100));
  } else {
    // fragmentation approx = 257 intervals / 768 max intervals
    EXPECT_EQ(33, uint64_t(alloc->get_fragmentation(alloc_unit) * 100));
  }

  for (size_t i = allocated.size() / 2 + 1; i < allocated.size(); i += 2)
  {
    interval_set<uint64_t> release_set;
    release_set.insert(allocated[i].offset, allocated[i].length);
    alloc->release(release_set);
  }
  // doing some rounding trick as stupid allocator doesn't merge all the 
  // extents that causes some minor fragmentation (minor bug or by-design behavior?).
  // Hence leaving just two 
  // digits after decimal point due to this.
  EXPECT_EQ(0, uint64_t(alloc->get_fragmentation(alloc_unit) * 100));
}

TEST_P(AllocTest, test_alloc_bug_24598)
{
  uint64_t capacity = 0x2625a0000ull;
  uint64_t alloc_unit = 0x4000;
  uint64_t want_size = 0x200000;
  PExtentVector allocated, tmp;

  init_alloc(capacity, alloc_unit);

  alloc->init_add_free(0x4800000, 0x100000);
  alloc->init_add_free(0x4a00000, 0x100000);

  alloc->init_rm_free(0x4800000, 0x100000);
  alloc->init_rm_free(0x4a00000, 0x100000);

  alloc->init_add_free(0x3f00000, 0x500000);
  alloc->init_add_free(0x4500000, 0x100000);
  alloc->init_add_free(0x4700000, 0x100000);
  alloc->init_add_free(0x4900000, 0x100000);
  alloc->init_add_free(0x4b00000, 0x200000);
//  alloc->init_add_free(0x4d00000, 0x200000);

  EXPECT_EQ(want_size, alloc->allocate(want_size, 0x100000, 0, 0, &tmp));
  EXPECT_EQ(tmp[0].offset, 0x4b00000);
  EXPECT_EQ(tmp[0].length, 0x200000);
  EXPECT_EQ(tmp.size(), 1);
}

// some tool to replay allocator ops to reproduce its issues if any.
#include <stdio.h>
TEST_P(AllocTest, test_alloc_bug_24598_replay)
{
  uint64_t capacity = 0x2625a0000ull;
  uint64_t alloc_unit = 0x4000;
  uint64_t want_size = 0x200000;
  PExtentVector allocated;
  size_t apos = 0;
  interval_set<uint64_t> release_set;

  init_alloc(capacity, alloc_unit);
  FILE* f = fopen("./alloc.out", "r");
  ASSERT_NE(f, nullptr);
  char buf[4096];
  while(!feof(f)) {
    fgets(buf, sizeof(buf), f);
    //std::cout<<buf<<std::endl;
    strtok(buf, " ");
    strtok(nullptr, " ");
    strtok(nullptr, " ");
    strtok(nullptr, " ");
    strtok(nullptr, " ");
    strtok(nullptr, " ");
    char* tmp = strtok(nullptr, " ");
    string cmd = tmp ? tmp : "";
    tmp = strtok(nullptr, " ");
    string params = tmp ? tmp : "";
    //std::cout<<cmd.c_str()<<" "<<params.c_str()<<std::endl;
    if (cmd == "init_add_free") {
      if (params.find("done") != 0) {
	uint64_t offs = strtoull(params.c_str() + 2,  nullptr, 16);
	uint64_t len = strtoull(params.c_str() + params.find_first_of('~') + 1,  nullptr, 16);
	//std::cout<<cmd.c_str()<<" "<<offs<<"~~"<<len<<std::endl;
	alloc->init_add_free(offs, len);
      }

    } else if (cmd == "init_rm_free") {
      if (params.find("done") != 0) {
	uint64_t offs = strtoull(params.c_str() + 2,  nullptr, 16);
	uint64_t len = strtoull(params.c_str() + params.find_first_of('~') + 1,  nullptr, 16);
	//std::cout<<cmd.c_str()<<" "<<offs<<"~~"<<len<<std::endl;
	alloc->init_rm_free(offs, len);
      }
    } else if (cmd == "allocate") {
      if (params.find_first_of("~") == std::string::npos) {
	char* end = nullptr;
	uint64_t want = strtoull(params.c_str() + 2, &end, 16);
	ASSERT_EQ(*end, '/');
	++end;
	uint64_t au = strtoull(end, &end, 16);
	ASSERT_EQ(*end, ',');
	++end;
	uint64_t max_alloc = strtoull(end, &end, 16);
	ASSERT_EQ(*end, ',');
	++end;
	uint64_t hint = strtoull(end, nullptr, 16);

	ASSERT_EQ(allocated.size(), apos);
	allocated.clear();
	apos = 0;
	if (want == 0x200000) {
	  std::cout<<"allocated???" << std::endl;
	}
	auto res = alloc->allocate(want, au, max_alloc, hint, &allocated);
	ASSERT_EQ(res, want);
	if (want == 0x200000) {
	  std::cout<<"allocated:"<<std::hex<<res<<" "<<allocated.size()<<std::endl;
	  for (auto i = 0; i < allocated.size(); ++i) {
	    std::cout<<std::hex<<" "<<allocated[i].offset << "~"<<allocated[i].length<<std::endl;
	  }
	}
	//std::cout<<"allocated:"<<std::hex<<res<<" "<<allocated.size()<<std::endl;
      } else {
	char* end = nullptr;
	uint64_t offs = strtoull(params.c_str() + 2,  &end, 16);
	ASSERT_EQ(*end, '~');
	++end;
	uint64_t len = strtoull(end,  &end, 16);
	ASSERT_EQ(*end, '/');
	ASSERT_EQ(offs, allocated[apos].offset);
	ASSERT_EQ(len, allocated[apos].length);
	apos++;
      }
    } else if (cmd == "release") {
      if (params.find("done") == 0) {
	alloc->release(release_set);
	release_set.clear();
      } else {
	//std::cout<<params.c_str()<<std::endl;
	char* end = nullptr;
	uint64_t offs = strtoull(params.c_str() + 2,  &end, 16);
	ASSERT_EQ(*end, '~');
	++end;
	uint64_t len = strtoull(end, nullptr, 16);
	release_set.insert(offs, len);
      }
    }
  }
  ASSERT_EQ(allocated.size(), apos);
  ASSERT_EQ(release_set.size(), 0);
  std::cout<<std::hex << "Allocated = "<<capacity - alloc->get_free() << std::endl;
  fclose(f);
}

INSTANTIATE_TEST_CASE_P(
  Allocator,
  AllocTest,
  ::testing::Values("stupid", "bitmap"));

#else

TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}
#endif
