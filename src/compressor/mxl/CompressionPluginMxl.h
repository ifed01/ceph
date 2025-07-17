/*
 * Ceph - scalable distributed file system
 *
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_COMPRESSION_PLUGIN_MXL_H
#define CEPH_COMPRESSION_PLUGIN_MXL_H

// -----------------------------------------------------------------------------
#include "ceph_ver.h"
#include "compressor/CompressionPlugin.h"
#include "MxlCompressor.h"
// -----------------------------------------------------------------------------

class CompressionPluginMxl : public ceph::CompressionPlugin {
  bool initialized = false;
public:

  explicit CompressionPluginMxl(CephContext* cct) : CompressionPlugin(cct)
  {}

  virtual ~CompressionPluginMxl() {
    deinit();
  }

  int init();
  void deinit();
  int factory(CompressorRef *cs,
              std::ostream *ss) override
  {
    MxlCompressor* c = new MxlCompressor(cct);
    if (c->open(ss) == 0) {
      *cs = CompressorRef(c);
      return 0;
    }
    return -1;
  }
};

#endif
