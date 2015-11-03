// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <string.h>
#include <gtest/gtest.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"
#include "osd/CompressContext.h"
/*
Introducing CompressContextTester class to access CompressContext protected members
*/
class CompressContextTester : public CompressContext
{
public:
  bool testMapping1()
  {
    clear();

    //putting three blocks into the map
    size_t offs = 0;
    append_block(0, 8 * 1024, "some_compression", 4 * 1024);
    append_block(8*1024, 7*1024, "", 7 * 1024);
    append_block( (8+7)*1024, 1932, "another_compression", 932);

    pair<uint64_t, uint64_t> block_offs_start = map_offset(0, false);
    EXPECT_EQ(block_offs_start.first, 0);
    EXPECT_EQ(block_offs_start.second, 0);

    pair<uint64_t, uint64_t> block_offs_end = map_offset(0, true);
    EXPECT_EQ(block_offs_end.first, 8*1024);
    EXPECT_EQ(block_offs_end.second, 4 * 1024);

    pair<uint64_t, uint64_t> compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>( 0, 1) );
    EXPECT_EQ(compressed_block.first, 0);
    EXPECT_EQ(compressed_block.second, 4 * 1024);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 132));
    EXPECT_EQ(compressed_block.first, 0);
    EXPECT_EQ(compressed_block.second, 4 * 1024);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 4*1024));
    EXPECT_EQ(compressed_block.first, 0);
    EXPECT_EQ(compressed_block.second, 4 * 1024);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 4 * 1024+1));
    EXPECT_EQ(compressed_block.first, 0);
    EXPECT_EQ(compressed_block.second, 4 * 1024);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 8 * 1024 - 1));
    EXPECT_EQ(compressed_block.first, 0);
    EXPECT_EQ(compressed_block.second, 4 * 1024);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 8 * 1024));
    EXPECT_EQ(compressed_block.first, 0);
    EXPECT_EQ(compressed_block.second, 4 * 1024);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 8 * 1024 + 1));
    EXPECT_EQ(compressed_block.first, 0);
    EXPECT_EQ(compressed_block.second, (4+7) * 1024);

  }
};

TEST(CompressContext, check_mapping)
{
  CompressContextTester ctx;
  ctx.testMapping1();
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  //g_conf->set_val("compressor_dir", ".libs", false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 && 
 *   make unittest_compression_zlib && 
 *   valgrind --tool=memcheck \
 *      ./unittest_compression_zlib \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
