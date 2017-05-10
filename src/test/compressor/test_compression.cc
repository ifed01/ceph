// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 Mirantis, Inc.
 *
 * Author: Alyona Kiseleva <akiselyova@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <unistd.h>

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <gtest/gtest.h>
#include "global/global_init.h"
#include "compressor/Compressor.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"
#include "compressor/CompressionPlugin.h"
#include "common/errno.h"
#include "include/utime.h"
#include "common/Clock.h"

#include <zlib.h>

class CompressorTest : public ::testing::Test,
			public ::testing::WithParamInterface<const char*> {
public:
  std::string plugin;
  CompressorRef compressor;
  bool old_zlib_isal;

  CompressorTest() {
    // note for later
    old_zlib_isal = g_conf->compressor_zlib_isal;

    plugin = GetParam();
    size_t pos = plugin.find('/');
    if (pos != std::string::npos) {
      string isal = plugin.substr(pos + 1);
      plugin = plugin.substr(0, pos);
      if (isal == "isal") {
	g_conf->set_val("compressor_zlib_isal", "true");
	g_ceph_context->_conf->apply_changes(NULL);
      } else if (isal == "noisal" || isal == "qat") {
	g_conf->set_val("compressor_zlib_isal", "false");
	g_ceph_context->_conf->apply_changes(NULL);
      } else {
	assert(0 == "bad option");
      }
    }
    cout << "[plugin " << plugin << " (" << GetParam() << ")]" << std::endl;
  }
  ~CompressorTest() {
    g_conf->set_val("compressor_zlib_isal", old_zlib_isal ? "true" : "false");
    g_ceph_context->_conf->apply_changes(NULL);
  }

  void SetUp() {
    compressor = Compressor::create(g_ceph_context, plugin);
    ASSERT_TRUE(compressor);
  }
  void TearDown() {
    compressor.reset();
    compressor = Compressor::create(g_ceph_context, plugin);
  }
};

TEST_P(CompressorTest, load_plugin)
{
}

TEST_P(CompressorTest, small_round_trip)
{
  bufferlist orig;
  orig.append("This is a short string.  There are many strings like it but this one is mine.");
  bufferlist compressed;
  int r = compressor->compress(orig, compressed);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}

TEST_P(CompressorTest, big_round_trip_repeated)
{
  unsigned len = 1048576 * 4;
  bufferlist orig;
  while (orig.length() < len) {
    orig.append("This is a short string.  There are many strings like it but this one is mine.");
  }
  bufferlist compressed;
  int r = compressor->compress(orig, compressed);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
  for(int i = 0; i<0;i++){
  bufferlist compressed;
  r = compressor->compress(orig, compressed);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed);
  ASSERT_EQ(0, r);
  }
}

TEST_P(CompressorTest, big_round_trip_randomish)
{
  unsigned len = 1048576 * 100;//269;
  bufferlist orig;
  const char *alphabet = "abcdefghijklmnopqrstuvwxyz";
  if (false) {
    while (orig.length() < len) {
      orig.append(alphabet[rand() % 10]);
    }
  } else {
    bufferptr bp(len);
    char *p = bp.c_str();
    for (unsigned i=0; i<len; ++i) {
      p[i] = alphabet[rand() % 10];
    }
    orig.append(bp);
  }
  bufferlist compressed;
  int r = compressor->compress(orig, compressed);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}

#if 0
TEST_P(CompressorTest, big_round_trip_file)
{
  bufferlist orig;
  int fd = ::open("bin/ceph-osd", O_RDONLY);
  struct stat st;
  ::fstat(fd, &st);
  orig.read_fd(fd, st.st_size);

  bufferlist compressed;
  int r = compressor->compress(orig, compressed);
  ASSERT_EQ(0, r);
  bufferlist decompressed;
  r = compressor->decompress(compressed, decompressed);
  ASSERT_EQ(0, r);
  ASSERT_EQ(decompressed.length(), orig.length());
  ASSERT_TRUE(decompressed.contents_equal(orig));
  cout << "orig " << orig.length() << " compressed " << compressed.length()
       << " with " << GetParam() << std::endl;
}
#endif


TEST_P(CompressorTest, compress_decompress)
{
  const char* test = "This is test text";
  int res;
  int len = strlen(test);
  bufferlist in, out;
  bufferlist after;
  bufferlist exp;
  in.append(test, len);
  res = compressor->compress(in, out);
  EXPECT_EQ(res, 0);
  res = compressor->decompress(out, after);
  EXPECT_EQ(res, 0);
  exp.append(test);
  EXPECT_TRUE(exp.contents_equal(after));
  after.clear();
  size_t compressed_len = out.length();
  out.append_zero(12);
  auto it = out.begin();
  res = compressor->decompress(it, compressed_len, after);
  EXPECT_EQ(res, 0);
  EXPECT_TRUE(exp.contents_equal(after));

  //large block and non-begin iterator for continuous block
  std::string data;
  data.resize(0x10000 * 1);
  for(size_t i = 0; i < data.size(); i++)
    data[i] = i / 256;
  in.clear();
  out.clear();
  in.append(data);
  exp = in;
  res = compressor->compress(in, out);
  EXPECT_EQ(res, 0);
  compressed_len = out.length();
  out.append_zero(0x10000 - out.length());
  after.clear();
  out.c_str();
  bufferlist prefix;
  prefix.append(string("some prefix"));
  size_t prefix_len = prefix.length();
  out.claim_prepend(prefix);
  it = out.begin();
  it.advance(prefix_len);
  res = compressor->decompress(it, compressed_len, after);
  EXPECT_EQ(res, 0);
  EXPECT_TRUE(exp.contents_equal(after));
}

TEST_P(CompressorTest, sharded_input_decompress)
{
  const size_t small_prefix_size=3;

  string test(114*1024,0);
  int len = test.size();
  bufferlist in, out;
  in.append(test.c_str(), len);
  int res = compressor->compress(in, out);
  EXPECT_EQ(res, 0);
  EXPECT_GT(out.length(), small_prefix_size);
  bufferlist out2, tmp;
  tmp.substr_of(out, 0, small_prefix_size );
  out2.append( tmp );
  size_t left = out.length()-small_prefix_size;
  size_t offs = small_prefix_size;
  while( left > 0 ){
    size_t shard_size = MIN( 2048, left );
    tmp.substr_of(out, offs, shard_size );
    out2.append( tmp );
    left -= shard_size;
    offs += shard_size;
  }

  bufferlist after;
  res = compressor->decompress(out2, after);
  EXPECT_EQ(res, 0);
}

void test_compress(CompressorRef compressor, size_t size)
{
  char* data = (char*) malloc(size);
  for (size_t t = 0; t < size; t++) {
    data[t] = (t & 0xff) | (t >> 8);
  }
  bool first = true;
  bufferlist in;
  in.append(data, size);
  for (size_t t = 0; t < 100000; t++) {
    bufferlist out;
    int res = compressor->compress(in, out);
    EXPECT_EQ(res, 0);
    if (first) {
      first = false;
      cout << "orig " << in.length() << " compressed " << out.length()
           << std::endl;
    }
  }
}

void test_decompress(CompressorRef compressor, size_t size)
{
  char* data = (char*) malloc(size);
  for (size_t t = 0; t < size; t++) {
    data[t] = (t & 0xff) | (t >> 8);
  }
  bufferlist in, out;
  in.append(data, size);
  int res = compressor->compress(in, out);
  EXPECT_EQ(res, 0);
  cout << "orig " << in.length() << " compressed " << out.length()
       << std::endl;
  for (size_t t = 0; t < 100000; t++) {
    bufferlist out_dec;
    int res = compressor->decompress(out, out_dec);
    EXPECT_EQ(res, 0);
  }
}

TEST_P(CompressorTest, compress_1024)
{
  test_compress(compressor, 1024);
}

TEST_P(CompressorTest, compress_2048)
{
  test_compress(compressor, 2048);
}

TEST_P(CompressorTest, compress_4096)
{
  test_compress(compressor, 4096);
}

TEST_P(CompressorTest, compress_8192)
{
  test_compress(compressor, 8192);
}

TEST_P(CompressorTest, compress_16384)
{
  test_compress(compressor, 16384);
}

TEST_P(CompressorTest, decompress_1024)
{
  test_decompress(compressor, 1024);
}

TEST_P(CompressorTest, decompress_2048)
{
  test_decompress(compressor, 2048);
}

TEST_P(CompressorTest, decompress_4096)
{
  test_decompress(compressor, 4096);
}

TEST_P(CompressorTest, decompress_8192)
{
  test_decompress(compressor, 8192);
}

TEST_P(CompressorTest, decompress_16384)
{
  test_decompress(compressor, 16384);
}

TEST_P(CompressorTest, compress_65536)
{
  test_compress(compressor, 65536);
}

TEST_P(CompressorTest, decompress_65536)
{
  test_decompress(compressor, 65536);
}


INSTANTIATE_TEST_CASE_P(
  Compressor,
  CompressorTest,
  ::testing::Values(
    "zlib/isal",
    "zlib/noisal",
    "snappy"));

TEST(ZlibCompressor, zlib_isal_compatibility)
{
  g_conf->set_val("compressor_zlib_isal", "true");
  g_ceph_context->_conf->apply_changes(NULL);
  CompressorRef isal = Compressor::create(g_ceph_context, "zlib");
  g_conf->set_val("compressor_zlib_isal", "false");
  g_ceph_context->_conf->apply_changes(NULL);
  CompressorRef zlib = Compressor::create(g_ceph_context, "zlib");
  char test[101];
  srand(time(0));
  for (int i=0; i<100; ++i)
    test[i] = 'a' + rand()%26;
  test[100] = '\0';
  int len = strlen(test);
  bufferlist in, out;
  in.append(test, len);
  // isal -> zlib
  int res = isal->compress(in, out);
  EXPECT_EQ(res, 0);
  bufferlist after;
  res = zlib->decompress(out, after);
  EXPECT_EQ(res, 0);
  bufferlist exp;
  exp.append(test);
  EXPECT_TRUE(exp.contents_equal(after));
  after.clear();
  out.clear();
  exp.clear();
  // zlib -> isal
  res = zlib->compress(in, out);
  EXPECT_EQ(res, 0);
  res = isal->decompress(out, after);
  EXPECT_EQ(res, 0);
  exp.append(test);
  EXPECT_TRUE(exp.contents_equal(after));
}

TEST(CompressionPlugin, all)
{
  const char* env = getenv("CEPH_LIB");
  std::string directory(env ? env : ".libs");
  CompressorRef compressor;
  PluginRegistry *reg = g_ceph_context->get_plugin_registry();
  EXPECT_TRUE(reg);
  CompressionPlugin *factory = dynamic_cast<CompressionPlugin*>(reg->get_with_load("compressor", "invalid"));
  EXPECT_FALSE(factory);
  factory = dynamic_cast<CompressionPlugin*>(reg->get_with_load("compressor", "example"));
  EXPECT_TRUE(factory);
  stringstream ss;
  EXPECT_EQ(0, factory->factory(&compressor, &ss));
  EXPECT_TRUE(compressor.get());
  {
    Mutex::Locker l(reg->lock);
    EXPECT_EQ(-ENOENT, reg->remove("compressor", "does not exist"));
    EXPECT_EQ(0, reg->remove("compressor", "example"));
    EXPECT_EQ(0, reg->load("compressor", "example"));
  }
}

TEST(ZlibCompressor, isal_compress_zlib_decompress_random)
{
  g_conf->set_val("compressor_zlib_isal", "true");
  g_ceph_context->_conf->apply_changes(NULL);
  CompressorRef isal = Compressor::create(g_ceph_context, "zlib");
  g_conf->set_val("compressor_zlib_isal", "false");
  g_ceph_context->_conf->apply_changes(NULL);
  CompressorRef zlib = Compressor::create(g_ceph_context, "zlib");

  for (int cnt=0; cnt<1000; cnt++)
  {
    srand(cnt + 1000);
    int log2 = (rand()%18) + 1;
    int size = (rand() % (1 << log2)) + 1;

    char test[size];
    for (int i=0; i<size; ++i)
      test[i] = rand()%256;
    bufferlist in, out;
    in.append(test, size);

    int res = isal->compress(in, out);
    EXPECT_EQ(res, 0);
    bufferlist after;
    res = zlib->decompress(out, after);
    EXPECT_EQ(res, 0);
    bufferlist exp;
    exp.append(test, size);
    EXPECT_TRUE(exp.contents_equal(after));
  }
}

TEST(ZlibCompressor, isal_compress_zlib_decompress_walk)
{
  g_conf->set_val("compressor_zlib_isal", "true");
  g_ceph_context->_conf->apply_changes(NULL);
  CompressorRef isal = Compressor::create(g_ceph_context, "zlib");
  g_conf->set_val("compressor_zlib_isal", "false");
  g_ceph_context->_conf->apply_changes(NULL);
  CompressorRef zlib = Compressor::create(g_ceph_context, "zlib");

  for (int cnt=0; cnt<1000; cnt++)
  {
    srand(cnt + 1000);
    int log2 = (rand()%18) + 1;
    int size = (rand() % (1 << log2)) + 1;

    int range = 1;

    char test[size];
    test[0] = rand()%256;
    for (int i=1; i<size; ++i)
      test[i] = test[i-1] + rand()%(range*2+1) - range;
    bufferlist in, out;
    in.append(test, size);

    int res = isal->compress(in, out);
    EXPECT_EQ(res, 0);
    bufferlist after;
    res = zlib->decompress(out, after);
    EXPECT_EQ(res, 0);
    bufferlist exp;
    exp.append(test, size);
    EXPECT_TRUE(exp.contents_equal(after));
  }
}

#include <sched.h>
#include <atomic>
#include "common/Cond.h"

Mutex _mutex("compress_perf_comp");
Cond ready_cond;
Cond start_cond;
Cond stop_cond;

std::atomic<uint64_t> ready_thread_count = {0};

typedef struct
{
  pthread_t th;
  int id;
  const char* filename;
  size_t blob_size;
  size_t step;
  uint64_t size, buf_size;
  CompressorRef c;
  uint64_t compressed;
}
ThreadInfo;

#define MAX_STAT 10
#define MAX_CORE 32

#define CPU_STATUS_LINE_LENGTH 1024
#define TAG_LENGTH 10
#define CPU_TIME_MULTIPLIER 10000
#define CPU_PERCENTAGE_MULTIPLIER 100

typedef union
{
    struct
    {
        int user;
        int nice;
        int sys;
        int idle;
        int io;
        int irq;
        int softirq;
        int context;
    };
    int d[MAX_STAT];
}
cpu_time_t;

static cpu_time_t cpu_time[MAX_CORE];
static cpu_time_t cpu_time_total;
static cpu_time_t cpu_context;


#define MAX_THREAD 1024

/******************************************************************************
* function:
*        cpu_time_add (cpu_time_t *t1, cpu_time_t *t2, int subtract)
*
* @param t1 [IN] - cpu time
* @param t2 [IN] - cpu time
* @param substract [IN] - subtract flag
*
* description:
*   CPU timing calculation functions.
******************************************************************************/
static void cpu_time_add (cpu_time_t *t1, cpu_time_t *t2, int subtract)
{
    int i;

    for (i = 0; i < MAX_STAT; i++)
    {
        if (subtract)
            t1->d[i] -= t2->d[i];
        else
            t1->d[i] += t2->d[i];
    }
}


/******************************************************************************
* function:
*       read_stat (int init)
*
* @param init [IN] - op flag
*
* description:
*  read in CPU status from proc/stat file
******************************************************************************/
static void read_stat (int init)
{
    char line[CPU_STATUS_LINE_LENGTH];
    char tag[TAG_LENGTH];
    FILE *fp;
    int index = 0;
    cpu_time_t tmp;

    fp = fopen ("/proc/stat", "r");
    if (NULL == fp)
    {
        fprintf (stderr, "Can't open proc stat\n");
        exit (1);
    }

    while (!feof (fp))
    {
        if (fgets (line, sizeof line - 1, fp) == NULL)
            break;

        if (!strncmp (line, "ctxt", 4))
        {
            if (sscanf (line, "%*s %d", &tmp.context) < 1)
                goto parse_fail;

            cpu_time_add (&cpu_context, &tmp, init);
            continue;
        }

        if (strncmp (line, "cpu", 3))
            continue;

        if (sscanf (line, "%s %d %d %d %d %d %d %d",
                tag,
                &tmp.user,
                &tmp.nice,
                &tmp.sys,
                &tmp.idle,
                &tmp.io,
                &tmp.irq,
                &tmp.softirq) < 8)
        {
            goto parse_fail;
        }

        if (!strcmp (tag, "cpu"))
            cpu_time_add (&cpu_time_total, &tmp, init);
        else if (!strncmp (tag, "cpu", 3))
        {
            index = atoi (&tag[3]);
            if ((0 <= index) && (MAX_CORE >= index))
                cpu_time_add (&cpu_time[index], &tmp, init);
        }
    }

    fclose (fp);
    return;

parse_fail:
    fprintf (stderr, "Failed to parse %s\n", line);
    exit (1);
}

void print_cpu_stat(uint64_t elapsed_usec, int core_count)
{
//    if (cpu_core_info)
    {
/*        printf ("      %10s %10s %10s %10s %10s %10s %10s\n",
                "user", "nice", "sys", "idle", "io", "irq", "sirq");
        for (int i = 0; i < MAX_CORE + 1; i++)
        {
            cpu_time_t *t;

            if (i == MAX_CORE)
            {
                printf ("total ");
                t = &cpu_time_total;
            }
            else
            {
                printf ("cpu%d  ", i);
                t = &cpu_time[i];
            }

            printf (" %10d %10d %10d %10d %10d %10d %10d\n",
                    t->user,
                    t->nice,
                    t->sys,
                    t->idle,
                    t->io,
                    t->irq,
                    t->softirq);
        }

        printf ("Context switches: %d\n", cpu_context.context);
*/
        unsigned long cpu_time = 0;
        unsigned long cpu_user = 0;
        unsigned long cpu_kernel = 0;

        cpu_time = (cpu_time_total.user +
                cpu_time_total.nice +
                cpu_time_total.sys +
                cpu_time_total.io +
                cpu_time_total.irq +
                cpu_time_total.softirq) * CPU_TIME_MULTIPLIER / core_count;
        cpu_user = cpu_time_total.user * CPU_TIME_MULTIPLIER / core_count;
        cpu_kernel = cpu_time_total.sys * CPU_TIME_MULTIPLIER / core_count;

        printf("ctx switches %d, use(%%) %lu, user(%%) %lu, kernel(%%) %lu\n",
           cpu_context.context,
           (unsigned long)cpu_time * CPU_PERCENTAGE_MULTIPLIER / elapsed_usec,
           (unsigned long)cpu_user * CPU_PERCENTAGE_MULTIPLIER / elapsed_usec,
           (unsigned long)cpu_kernel * CPU_PERCENTAGE_MULTIPLIER / elapsed_usec );

    }
}

static void *thread_worker(void *arg)
{
  ThreadInfo *info = (ThreadInfo *) arg;

  std::vector<uint8_t> inmem_buffer;
  inmem_buffer.resize(info->buf_size);

  std::unique_ptr<char> blob(new char[info->blob_size]);

  int f = open(info->filename, O_RDONLY);
  if (f < 0){
    cerr << "open(" << info->filename << ") " << cpp_strerror(errno) << std::endl;
    exit(-1);
  }
  uint64_t pos = 0;
  off_t off = info->blob_size * info->id;
  lseek(f, off, SEEK_SET);

    int r;
    do{
 
      r = read(f, inmem_buffer.data() + pos, info->blob_size);
      if (r < 0){
        cerr << "read(" << info->filename << ") " << cpp_strerror(errno) << std::endl;
        exit(-1);
      } else if (r) {
        off += info->step;
        pos += r;

        lseek64(f, off, SEEK_SET);
      }
    }while (r > 0 && pos < inmem_buffer.size());
    close(f);

    
    {
      Mutex::Locker l(_mutex);
      ++ready_thread_count;
      ready_cond.SignalAll();
      start_cond.Wait(_mutex);
    }

    pos = 0;
    do{
  
      r = std::min(inmem_buffer.size()-pos, info->blob_size);
      memcpy(blob.get(), inmem_buffer.data() + pos, r);
      pos += r;
        buffer::ptr p(blob.get(), r);
        bufferlist bl, out;
        bl.push_back(p);
        if (info->c){
          info->c->compress(bl,out);
          info->compressed += out.length();
        } else {
          out = bl; //simulate raw data copy
          info->compressed += bl.length();
        }
    }while (r > 0 && pos < inmem_buffer.size());
    
    {
      Mutex::Locker l(_mutex);
      --ready_thread_count;
      stop_cond.Signal();
    }

      close(f);
    return NULL;
}


static 
void do_compress_perf_compare( 
  CompressorRef c,
  const char* cname,
  const char* filename, 
  size_t blob_size,
  unsigned core_count,  
  size_t thread_count)
{
  int rc = 0;

  ThreadInfo tinfo[MAX_THREAD];
  cpu_set_t cpuset;

  struct stat buf;
  if (::stat(filename, &buf)) {
    cerr << "stat(" << filename << ") " << cpp_strerror(errno) << std::endl;
    return;
  }

  memset(&cpu_time, 0, sizeof(cpu_time));
  memset(&cpu_time_total, 0, sizeof(cpu_time_total));
  memset(&cpu_context, 0, sizeof(cpu_context));
  
  for (size_t i = 0; i < thread_count; i++)
  {
    ThreadInfo *info = &tinfo[i];

    uint64_t buf_size = buf.st_size / (thread_count * blob_size) * blob_size;

    uint64_t rest = buf.st_size % (thread_count * blob_size);;
    size_t max_full_size_blob = rest / blob_size;
    if (i < max_full_size_blob) {
      buf_size+=blob_size;
    } else if (i == max_full_size_blob) {
      buf_size += rest % blob_size;
    }
    info->id = i;
    info->filename = filename;
    info->blob_size = blob_size;
    info->filename = filename;
    info->step = blob_size * thread_count;
    info->size = buf.st_size;
    info->buf_size = buf_size;
    info->c = c;
    info->compressed = 0;

    rc = pthread_create(&info->th, NULL, thread_worker, (void *)info);
    if (rc != 0) {
      cerr << "Failure to create thread, status = " << rc << std::endl;
      return;
    }

    /* cpu affinity setup */
    if ( core_count > 0 && thread_count > 1) {
      CPU_ZERO(&cpuset);

      /* assigning thread to different cores */
      int coreID = (i % core_count);
      CPU_SET(coreID, &cpuset);

      rc = pthread_setaffinity_np(info->th, sizeof(cpu_set_t), &cpuset);
      if (rc != 0)
      {
        cerr << "Failure to set thread affinity, status = " << rc << std::endl;
        return;
      }
      rc = pthread_getaffinity_np(info->th, sizeof(cpu_set_t), &cpuset);
      if( rc < 0 || !CPU_ISSET(coreID, &cpuset)) {
        cerr << "Affinity wasn't set." << std::endl;
        return;
      }
    }
  }
  /* set all threads to ready condition */
  {
    Mutex::Locker l(_mutex);
    while (ready_thread_count.load() < thread_count) {
      ready_cond.Wait(_mutex);
    }
  }

  read_stat(1);
  utime_t start = ceph_clock_now(g_ceph_context);
  {
    Mutex::Locker l(_mutex);
    start_cond.SignalAll();
  }

  /* wait for other threads stop */
  {
    Mutex::Locker l(_mutex);
    while (ready_thread_count.load() > 0) {
      stop_cond.Wait(_mutex);
    }
  }
  utime_t elapsed = ceph_clock_now(g_ceph_context) - start;
  read_stat(0);

  uint64_t compressed = 0;
  for (size_t i = 0; i < thread_count; i++)
  {
    pthread_join(tinfo[i].th, NULL);
    compressed += tinfo[i].compressed;
  }
  cout << "Summary: orig " << buf.st_size /1024 / 1024 << "mb -> " << compressed / 1024 / 1024
       << "mb ratio="<< float(compressed) / buf.st_size << " via " << cname <<"/" << blob_size
       << " in " << elapsed.to_msec() << " msec. " 
       << "cores/threads = " << core_count << "/" << thread_count
       << std::endl;

  print_cpu_stat( elapsed.to_msec() * 1000, core_count);
}

TEST(PerfTest, perf_compare_dummy)
{
  CompressorRef dummy;
  do_compress_perf_compare(dummy, "none",
    g_conf->bluestore_block_path.c_str(), //corpus file location
    g_conf->bluestore_compression_min_blob_size,
    g_conf->bluestore_max_ops, //used as core count
    g_conf->async_compressor_threads );
}

TEST(PerfTest, perf_compare_snappy)
{
  CompressorRef snappy = Compressor::create(g_ceph_context, "snappy");
  do_compress_perf_compare(snappy, "snappy",
    g_conf->bluestore_block_path.c_str(), //corpus file location
    g_conf->bluestore_compression_min_blob_size,
    g_conf->bluestore_max_ops, //used as core count
    g_conf->async_compressor_threads );
}

TEST(PerfTest, perf_compare_isal)
{
  //zlib + isal
  g_conf->set_val("compressor_zlib_isal", "true");
  g_ceph_context->_conf->apply_changes(NULL);
  CompressorRef isal = Compressor::create(g_ceph_context, "zlib");
  do_compress_perf_compare(isal, "zlib+isal", 
    g_conf->bluestore_block_path.c_str(), //corpus file location
    g_conf->bluestore_compression_min_blob_size,
    g_conf->bluestore_max_ops, //used as core count
    g_conf->async_compressor_threads );
}

TEST(PerfTest, perf_compare_zlib) //should be prior to _qat test case to avoid issues caused by zlibShutdownEngine
{
  //original zlib
  g_conf->set_val("compressor_zlib_isal", "false");
  g_conf->set_val("compressor_zlib_qat", "false");
  g_ceph_context->_conf->apply_changes(NULL);
  CompressorRef zlib = Compressor::create(g_ceph_context, "zlib");
  do_compress_perf_compare(zlib,  "zlib", 
    g_conf->bluestore_block_path.c_str(), //corpus file location
    g_conf->bluestore_compression_min_blob_size,
    g_conf->bluestore_max_ops, //used as core count
    g_conf->async_compressor_threads );
}

TEST(PerfTest, perf_compare_qat)
{
  //workaround to bypasss zlibSetupEngine/zlibShutdownEngine calls if no performance comparison is expected
  if (g_conf->bluestore_block_path.empty()) {
    ASSERT_TRUE(false);
    return;
  }

  g_conf->set_val("compressor_zlib_isal", "false");
  g_conf->set_val("compressor_zlib_qat", "true");
  //zlib + QAT  
  CompressorRef zlib2 = Compressor::create(g_ceph_context, "zlib");
  do_compress_perf_compare(zlib2,  "zlib+qat",
    g_conf->bluestore_block_path.c_str(), //corpus file location
    g_conf->bluestore_compression_min_blob_size,
    g_conf->bluestore_max_ops, //used as core count
    g_conf->async_compressor_threads );

}


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  if (env)
    g_conf->set_val("plugin_dir", env, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  int r = RUN_ALL_TESTS();
  return r;
}
