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
#include "common/debug.h"
#include "compressor/CompressionPlugin.h"
#include "SnappyCompressor.h"
// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "CompressionPluginSnappy: ";
}

class CompressionPluginSnappy : public CompressionPlugin {
public:

  virtual int factory(const std::string &directory,
                      CompressorRef *cs,
                      ostream *ss)
  {
    SnappyCompressor *interface = new SnappyCompressor();
    *cs = CompressorRef(interface);
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
