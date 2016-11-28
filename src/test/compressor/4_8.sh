#!/bin/sh

#"8K/4/8"
CEPH_ARGS="--bluestore_max_ops 4 --async_compressor_threads 8 --bluestore_compression_min_blob_size 8192 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

#"16K/4/8"
CEPH_ARGS="--bluestore_max_ops 4 --async_compressor_threads 8 --bluestore_compression_min_blob_size 16384 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

#"64K/4/8"
CEPH_ARGS="--bluestore_max_ops 4 --async_compressor_threads 8 --bluestore_compression_min_blob_size 65536 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

#"256K/4/8"
CEPH_ARGS="--bluestore_max_ops 4 --async_compressor_threads 8 --bluestore_compression_min_blob_size 262144 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

#"4M/4/8"
CEPH_ARGS="--bluestore_max_ops 4 --async_compressor_threads 8 --bluestore_compression_min_blob_size 4194304 --conf ./ceph.conf" bin/unittest_compression --gtest_filter=PerfTest*

