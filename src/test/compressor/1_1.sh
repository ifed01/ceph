#!/bin/sh

rm out

#"8K/1/1"
CEPH_ARGS="--bluestore_max_ops 1 --async_compressor_threads 1 --bluestore_compression_min_blob_size 8192 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

#"16K/1/1"
CEPH_ARGS="--bluestore_max_ops 1 --async_compressor_threads 1 --bluestore_compression_min_blob_size 16384 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

#"64K/1/1"
CEPH_ARGS="--bluestore_max_ops 1 --async_compressor_threads 1 --bluestore_compression_min_blob_size 65536 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

#"256K/1/1"
CEPH_ARGS="--bluestore_max_ops 1 --async_compressor_threads 1 --bluestore_compression_min_blob_size 262144 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

#"4M/1/1"
CEPH_ARGS="--bluestore_max_ops 1 --async_compressor_threads 1 --bluestore_compression_min_blob_size 4194304 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

#"8K/4/64"
#CEPH_ARGS="--bluestore_max_ops 4 --async_compressor_threads 64 --bluestore_compression_min_blob_size 8192 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*-*perf_compare_qat
#CEPH_ARGS="--bluestore_max_ops 4 --async_compressor_threads 64 --bluestore_compression_min_blob_size 16384 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest.perf_compare_qat
#"16K/4/64"
#CEPH_ARGS="--bluestore_max_ops 4 --async_compressor_threads 64 --bluestore_compression_min_blob_size 16384 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*-*perf_compare_qat
#CEPH_ARGS="--bluestore_max_ops 4 --async_compressor_threads 64 --bluestore_compression_min_blob_size 16384 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest.perf_compare_qat
