// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MXLCOMPRESSOR_H
#define CEPH_MXLCOMPRESSOR_H
extern "C" {
#include <dre_api.h>
}
#include <memory>
#include <boost/lockfree/stack.hpp>
#include "include/buffer.h"
#include "include/encoding.h"
#include "compressor/Compressor.h"

using std::byte;

class MxlCompressorSession {
public:
  MxlCompressorSession(CephContext *cct);
  virtual ~MxlCompressorSession();

  int open(std::ostream* ss, bool _compress);
  void close();
  
  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> &compressor_message);
  int decompress(ceph::buffer::list::const_iterator &p,
		 size_t compressed_len,
		 ceph::buffer::list &dst,
		 std::optional<int32_t> compressor_message);

private:
  bool opened = false;
  bool compress_mode = false;
  size_t dst_buf_size = 0;
  std::unique_ptr<byte[]> dst_buf;

  size_t num_src_desc = 0;
  std::unique_ptr<DRE_dataDesc[]> src_desc;
  size_t num_dst_desc = 0;
  std::unique_ptr<DRE_dataDesc[]> dst_desc;

  DRE_rawSessHandle handle;
  DRE_compAlgoParam compAlgoParam;
  DRE_rawSessCompParam comp;
  CephContext *const cct = nullptr;
};

class MxlCompressor : public Compressor
{
public:
  MxlCompressor(CephContext *cct);
  virtual ~MxlCompressor();

  int open(std::ostream* ss);
  void close();

  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> &compressor_message) override;

  int decompress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> compressor_message) override {
    auto i = std::cbegin(src);
    return decompress(i, src.length(), dst, compressor_message);
  }

  int decompress(ceph::buffer::list::const_iterator &p,
		 size_t compressed_len,
		 ceph::buffer::list &dst,
		 std::optional<int32_t> compressor_message) override;

private:
  static const size_t MAX_SESSIONS = 16;
  using SessPtr = std::unique_ptr<MxlCompressorSession>;
  using SessionStack = boost::lockfree::stack<MxlCompressorSession*, boost::lockfree::fixed_sized<true>>;

  bool opened = false;
  CephContext *const cct = nullptr;

  SessPtr compress_sess[MAX_SESSIONS];
  SessionStack* compress_sess_avail = nullptr;

  SessPtr decompress_sess[MAX_SESSIONS];
  SessionStack* decompress_sess_avail = nullptr;
};

#endif
