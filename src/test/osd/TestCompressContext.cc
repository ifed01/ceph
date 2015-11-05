// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 *
 * Author: Igor Fedotov <ifed@mirantis.com>
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
#include "compressor/CompressionPlugin.h"

struct TestCompressor : public Compressor
{
  enum Mode { NO_COMPRESS, COMPRESS, COMPRESS_NO_BENEFIT} mode;
  unsigned compress_calls, decompress_calls;
  const uint32_t TEST_MAGIC=0xdeadbeef;

  TestCompressor() { clear(); } 
  void clear() { mode = COMPRESS; compress_calls=decompress_calls = 0; }
  void reset(Mode _mode = COMPRESS) { clear(); mode = _mode; }
  virtual int compress( bufferlist& in, bufferlist &out)
  {
    int res=0;
    ++compress_calls;
    if( mode != NO_COMPRESS )
    {
      uint32_t size = in.length();
      assert( size<= 1024*1024); //introducing input block size limit to be able to validate decompressed value
      ::encode( TEST_MAGIC, out );
      ::encode( size, out );
      if( mode == COMPRESS_NO_BENEFIT )
        out.append(in);
      res = 0;
    }
    else
     res = -1;
    return res;
  }
  virtual int decompress( bufferlist& in, bufferlist &out)
  {
    ++decompress_calls;
    uint32_t ui;
    bufferlist::iterator it = in.begin();
    ::decode( ui, it );
    EXPECT_EQ( ui,  TEST_MAGIC );
    ::decode( ui, it );
    EXPECT_NE( ui, 0u );
    EXPECT_LE( ui, 1024*1024u ); //see assert in compress method above
    std::string res(ui, 'a' );
    out.append(res);
    return 0;
  }
  virtual const char* get_method_name() { return method_name().c_str(); }
  static const std::string& method_name() { static std::string s="test_method"; return s; }
};

struct TestCompressionPlugin : public CompressionPlugin
{
  TestCompressor* compressor;
  CompressorRef compressorRef;

  TestCompressionPlugin() : compressor( new TestCompressor() ), compressorRef(compressor) {}
  virtual int factory( const std::string & dir, CompressorRef *cs, ostream *ss ) {
    *cs = compressorRef;
    return 0;
  }
};
/*
Introducing CompressContextTester class to access CompressContext protected members
*/
class CompressContextTester : public CompressContext
{
public:
  void testMapping1()
  {
    clear();

    //putting three blocks into the map
    append_block(0, 8 * 1024, "some_compression", 4 * 1024);
    append_block(8*1024, 7*1024, "", 7 * 1024);
    append_block( (8+7)*1024, 1932, "another_compression", 932);

    EXPECT_EQ(get_compressed_size(), (4+7)*1024+932u);

    pair<uint64_t, uint64_t> block_offs_start = map_offset(0, false);
    EXPECT_EQ(block_offs_start.first, 0u);
    EXPECT_EQ(block_offs_start.second, 0u);

    pair<uint64_t, uint64_t> block_offs_end = map_offset(0, true);
    EXPECT_EQ(block_offs_end.first, 8*1024u);
    EXPECT_EQ(block_offs_end.second, 4 * 1024u);

    block_offs_start = map_offset(1, false);
    EXPECT_EQ(block_offs_start.first, 0u);
    EXPECT_EQ(block_offs_start.second, 0u);

    block_offs_end = map_offset(1, true);
    EXPECT_EQ(block_offs_end.first, 8*1024u);
    EXPECT_EQ(block_offs_end.second, 4 * 1024u);

    block_offs_start = map_offset(8*1024, false);
    EXPECT_EQ(block_offs_start.first, 8*1024u);
    EXPECT_EQ(block_offs_start.second, 4*1024u);

    block_offs_end = map_offset(8*1024, true);
    EXPECT_EQ(block_offs_end.first, (8+7)*1024u);
    EXPECT_EQ(block_offs_end.second, (4+7) * 1024u);

    block_offs_start = map_offset(8*1024+4096, false);
    EXPECT_EQ(block_offs_start.first, 8*1024u);
    EXPECT_EQ(block_offs_start.second, 4*1024u);

    block_offs_end = map_offset(8*1024+4096, true);
    EXPECT_EQ(block_offs_end.first, (8+7)*1024u);
    EXPECT_EQ(block_offs_end.second, (4+7) * 1024u);

    block_offs_start = map_offset((8+7)*1024, false);
    EXPECT_EQ(block_offs_start.first, (8+7)*1024u);
    EXPECT_EQ(block_offs_start.second, (4+7)*1024u );

    block_offs_end = map_offset((8+7)*1024, true);
    EXPECT_EQ(block_offs_end.first, (8+7)*1024u+1932);
    EXPECT_EQ(block_offs_end.second, (4+7)*1024u+932);

    block_offs_start = map_offset((8+7)*1024+1930, false);
    EXPECT_EQ(block_offs_start.first, (8+7)*1024u);
    EXPECT_EQ(block_offs_start.second, (4+7)*1024u );

    block_offs_end = map_offset((8+7)*1024 + 1930, true);
    EXPECT_EQ(block_offs_end.first, (8+7)*1024u+1932);
    EXPECT_EQ(block_offs_end.second, (4+7)*1024u+932);

    pair<uint64_t, uint64_t> compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>( 0, 1) );
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 132));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(131, 132));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 4*1024));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(10, 4*1024));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 4 * 1024+1));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 8 * 1024 - 1));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 8 * 1024));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(2, 8 * 1024-2));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, 4 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(0, 8 * 1024 + 1));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, (4+7) * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(1230, 8 * 1024));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, (4+7) * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(2, (8+7) * 1024 + 1930));
    EXPECT_EQ(compressed_block.first, 0u);
    EXPECT_EQ(compressed_block.second, (4+7) * 1024u+932);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(8*1024, 1024));
    EXPECT_EQ(compressed_block.first, 4*1024u);
    EXPECT_EQ(compressed_block.second, 7 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(8*1024+1023, 1024));
    EXPECT_EQ(compressed_block.first, 4*1024u);
    EXPECT_EQ(compressed_block.second, 7 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(8*1024+1024, 6*1024));
    EXPECT_EQ(compressed_block.first, 4*1024u);
    EXPECT_EQ(compressed_block.second, 7 * 1024u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>(8*1024+1024, 6*1024+1));
    EXPECT_EQ(compressed_block.first, 4*1024u);
    EXPECT_EQ(compressed_block.second, 7 * 1024u + 932);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>((8+7)*1024+1, 1));
    EXPECT_EQ(compressed_block.first, (4+7)*1024u);
    EXPECT_EQ(compressed_block.second, 932u);

    compressed_block = offset_len_to_compressed_block(std::pair<uint64_t, uint64_t>((8+7)*1024+1931, 1));
    EXPECT_EQ(compressed_block.first, (4+7)*1024u);
    EXPECT_EQ(compressed_block.second, 932u);
  }

  void testAppendAndFlush()
  {
    clear();

    EXPECT_FALSE(need_flush());
    //putting three blocks into the map
    append_block(0, 8 * 1024, "some_compression", 4 * 1024);
    append_block(8*1024, 7*1024, "", 7 * 1024);
    append_block( (8+7)*1024, 1932, "another_compression", 932);
    EXPECT_TRUE(need_flush());
    EXPECT_EQ(get_compressed_size(), (4+7)*1024+932u);

    map<string, boost::optional<bufferlist> > rollback_attrs;
    map<string, bufferlist> attrs;
    flush_for_rollback(rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_key()], boost::optional<bufferlist>());
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], boost::optional<bufferlist>());

    flush(attrs);
    EXPECT_EQ(attrs.size(), 2u);
    EXPECT_GT(attrs[ECUtil::get_cinfo_key()].length(), 0u);
    EXPECT_GT(attrs[ECUtil::get_cinfo_master_key()].length(), 0u);
    EXPECT_FALSE(need_flush());
    EXPECT_EQ(get_compressed_size(), (4+7)*1024+932u);

    rollback_attrs.clear();
    flush_for_rollback(rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_key()], attrs[ECUtil::get_cinfo_key()]);
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], attrs[ECUtil::get_cinfo_master_key()]);

    clear();
    EXPECT_EQ(get_compressed_size(), 0u);
    rollback_attrs.clear();
    flush_for_rollback(rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_key()], boost::optional<bufferlist>());
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], boost::optional<bufferlist>());

    setup_for_append_or_recovery( attrs );
    EXPECT_EQ(get_compressed_size(), (4+7)*1024+932u);
    rollback_attrs.clear();
    flush_for_rollback(rollback_attrs);
    EXPECT_EQ(rollback_attrs.size(), 2u);
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_key()], attrs[ECUtil::get_cinfo_key()]);
    EXPECT_EQ(rollback_attrs[ECUtil::get_cinfo_master_key()], attrs[ECUtil::get_cinfo_master_key()]);

    //verifying context content encoded in attrs
    CompressContext::MasterRecord masterRec;
    CompressContext::BlockInfoRecordSetHeader recHdr;
    CompressContext::BlockInfoRecord rec;

    bufferlist::iterator it = attrs[ECUtil::get_cinfo_master_key()].begin();
    ::decode(masterRec, it );
    EXPECT_EQ(masterRec.current_original_pos, (8+7)*1024+1932u);
    EXPECT_EQ(masterRec.current_compressed_pos, (4+7)*1024+932u);
    EXPECT_NE(masterRec.block_info_record_length, 0u);
    EXPECT_NE(masterRec.block_info_recordset_header_length, 0u);
    EXPECT_EQ(masterRec.methods.size(), 2u);
    EXPECT_EQ(masterRec.methods[0], "some_compression");
    EXPECT_EQ(masterRec.methods[1], "another_compression");

    it = attrs[ECUtil::get_cinfo_key()].begin();
    EXPECT_EQ(it.get_remaining(), masterRec.block_info_record_length*3+masterRec.block_info_recordset_header_length);

    recHdr.start_offset = recHdr.compressed_offset = 1234; //just fill with non-zero value
    ::decode(recHdr, it );
    EXPECT_EQ(recHdr.start_offset, 0u);
    EXPECT_EQ(recHdr.compressed_offset, 0u);

    ::decode(rec, it );
    EXPECT_EQ(rec.method_idx, 1u);
    EXPECT_EQ(rec.original_length, 8*1024u);
    EXPECT_EQ(rec.compressed_length, 4*1024u);
    ::decode(rec, it );
    EXPECT_EQ(rec.method_idx, 0u);
    EXPECT_EQ(rec.original_length, 7*1024u);
    EXPECT_EQ(rec.compressed_length, 7*1024u);
    ::decode(rec, it );
    EXPECT_EQ(rec.method_idx, 2u);
    EXPECT_EQ(rec.original_length, 1932u);
    EXPECT_EQ(rec.compressed_length, 932u);

    //verifying context content encoded in attrs when >1 recordsets are present
    attrs.clear();
    setup_for_append_or_recovery( attrs );
    uint64_t offs=0, coffs=0;
    uint32_t block_len = 4096, cblock_len=4090;
    size_t recCount=CompressContext::RECS_PER_RECORDSET*2+1;
    for( size_t i = 0; i<recCount; i++)
    {
      append_block( offs, block_len, "", cblock_len);
      offs+=block_len;
    }
    flush(attrs);

    it = attrs[ECUtil::get_cinfo_master_key()].begin();
    masterRec.clear();
    ::decode(masterRec, it );
    EXPECT_EQ(masterRec.current_original_pos, recCount*block_len);
    EXPECT_EQ(masterRec.current_compressed_pos, recCount*cblock_len);
    EXPECT_NE(masterRec.block_info_record_length, 0u);
    EXPECT_NE(masterRec.block_info_recordset_header_length, 0u);
    EXPECT_EQ(masterRec.methods.size(), 0u);

    it = attrs[ECUtil::get_cinfo_key()].begin();
    EXPECT_EQ(it.get_remaining(),
      masterRec.block_info_record_length*recCount + 
        masterRec.block_info_recordset_header_length*(1 + recCount / CompressContext::RECS_PER_RECORDSET ));
    offs = coffs = 0;
    for( size_t i = 0; i<recCount; i++)
    {
      if( (i % CompressContext::RECS_PER_RECORDSET)==0)
      {
        ::decode(recHdr, it);
        EXPECT_EQ(recHdr.start_offset, offs);
        EXPECT_EQ(recHdr.compressed_offset, coffs);
      } 
      rec.method_idx=255;
      rec.original_length=rec.compressed_length=1234;
      ::decode(rec, it );
      EXPECT_EQ(rec.method_idx, 0u);
      EXPECT_EQ(rec.original_length, block_len);
      EXPECT_EQ(rec.compressed_length, cblock_len);
      offs+=block_len;
      coffs+=cblock_len;
    }
  }
  void testCompressSimple( TestCompressor* compressor)
  {
    hobject_t oid;
    ECUtil::stripe_info_t sinfo( 1024/*arbitrary selected, not used*/ ,4096);
    std::string s1(7*1024, 'a');
    bufferlist in, out, out_res;
    uint64_t offs=0;
    in.append(s1);
    clear();
    //single block compress
    compressor->reset( TestCompressor::COMPRESS );
    int r = try_compress(TestCompressor::method_name(), oid, in, sinfo, &offs, &out);
    EXPECT_EQ(r, 0);
    EXPECT_NE(in.length(), out.length());
    EXPECT_EQ(out.length(), sinfo.get_stripe_width());
    EXPECT_EQ(get_compressed_size(), sinfo.get_stripe_width());
    EXPECT_EQ(offs, 0u);
    EXPECT_EQ(compressor->compress_calls, 1u);

    map<string, bufferlist> attrs;
    flush(attrs);

    clear();
    offs = 0;
    setup_for_read(attrs, 0, s1.size());
    r = try_decompress( oid, offs, s1.size(), out, &out_res);
    EXPECT_EQ(r, 0);
    EXPECT_TRUE(s1 == std::string(out_res.c_str()));
    EXPECT_EQ(compressor->decompress_calls, 1u);

    //multiple blocks compress
    clear();
    out_res.clear();
    compressor->reset( TestCompressor::COMPRESS );
    size_t block_count=200;
    uint64_t offs4append=0;
    for( size_t i=0;i<block_count; i++ ) {
      offs = offs4append;
      out.clear();
      int r = try_compress(TestCompressor::method_name(), oid, in, sinfo, &offs, &out);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(offs, out_res.length());
      offs4append+=in.length();
      out_res.append(out);
    }
    EXPECT_EQ(out_res.length(), block_count*sinfo.get_stripe_width());
    EXPECT_EQ(get_compressed_size(), block_count*sinfo.get_stripe_width());
    EXPECT_EQ(offs, (block_count-1)*sinfo.get_stripe_width());
    EXPECT_EQ(compressor->compress_calls, block_count);

    out=out_res;
    out_res.clear();
    attrs.clear();
    flush(attrs);

    clear();
    offs = 0;
    uint64_t size = block_count*s1.size();
    setup_for_read(attrs, offs, size); //read as a single block
    r = try_decompress( oid, offs, size, out, &out_res);
    EXPECT_EQ(r, 0);
    for( size_t i =0; i<block_count; i++){
      std::string tmpstr(out_res.get_contiguous( i*s1.size(), s1.size()), s1.size());
      EXPECT_TRUE(s1 == tmpstr);
    }
    EXPECT_EQ(compressor->decompress_calls, block_count);

    //reading all the object but the first and last bytes
    clear();
    compressor->reset();
    out_res.clear();
    offs = 1;
    size = block_count*s1.size() - 1;
    setup_for_read(attrs, offs, size); //read as a single block
    r = try_decompress( oid, offs, size, out, &out_res);
    EXPECT_EQ(r, 0);
    EXPECT_EQ(size, out_res.length());
    EXPECT_EQ(compressor->decompress_calls, block_count);

    //reading Nth block ( totally and partially ) 
    for( size_t i = 0;i<block_count; i++){
      clear();
      compressor->reset();
      out_res.clear();
      offs = i*s1.size();
      size = s1.size();
      setup_for_read(attrs, offs, offs+size); //read as a single block
      r = try_decompress( oid, offs, size, out, &out_res);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(size, out_res.length());
      std::string tmpstr(out_res.c_str(), s1.size());
      EXPECT_EQ(size, tmpstr.length());
      EXPECT_EQ(s1, tmpstr);
      EXPECT_EQ(compressor->decompress_calls, 1u);

      out_res.clear();
      offs = i*s1.size() + 2;
      size = s1.size()-3;
      setup_for_read(attrs, offs, offs+size); //read as a single block
      r = try_decompress( oid, offs, size, out, &out_res);
      EXPECT_EQ(r, 0);
      EXPECT_EQ(size, out_res.length());
      EXPECT_EQ(compressor->decompress_calls, 2u);
    }
  }
};


TEST(CompressContext, check_mapping)
{
  CompressContextTester ctx;
  ctx.testMapping1();
}

TEST(CompressContext, check_append)
{
  CompressContextTester ctx;
  ctx.testAppendAndFlush();
}


TEST(CompressContext, check_compres_decompress)
{
  CompressionPluginRegistry& reg = CompressionPluginRegistry::instance();
  TestCompressionPlugin compressorPlugin;
  {
    reg.disable_dlclose=true;
    Mutex::Locker l(reg.lock);
    reg.add(std::string(TestCompressor::method_name()), &compressorPlugin );
  }
  stringstream ss;
  CompressorRef cs;
  reg.factory(std::string(TestCompressor::method_name()), std::string(), &cs, &ss );
  EXPECT_TRUE( cs != NULL );
  
  CompressContextTester ctx;
  ctx.testCompressSimple( compressorPlugin.compressor);
  
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

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
