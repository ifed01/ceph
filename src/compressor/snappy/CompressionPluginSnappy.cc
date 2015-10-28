/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */


// -----------------------------------------------------------------------------
#include "ceph_ver.h"
#include "compressor/CompressionPlugin.h"
#include "SnappyCompressor.h"
// -----------------------------------------------------------------------------

class CompressionPluginSnappy : public CompressionPlugin {
public:

  virtual int factory(const std::string &directory,
                      CompressorRef *cs,
                      ostream *ss)
  {
    if (compressor == 0) {
      SnappyCompressor *interface = new SnappyCompressor();
      *cs = CompressorRef(interface);
    } else 
      cs = &compressor;
    return 0;
  }
};

// -----------------------------------------------------------------------------

const char *__compression_version()
{
  return CEPH_GIT_NICE_VER;
}

// -----------------------------------------------------------------------------

int __compression_init(char *plugin_name, char *directory)
{
  CompressionPluginRegistry &instance = CompressionPluginRegistry::instance();

  return instance.add(plugin_name, new CompressionPluginSnappy());
}
