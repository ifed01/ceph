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
#include "compression/CompressionPlugin.h"
#include "CompressionZlib.h"
// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "CompressionPluginZlib: ";
}

class CompressionPluginZlib : public CompressionPlugin {
public:

  virtual int factory(const std::string &directory,
		                  CompressionProfile &profile,
                      CompressionInterfaceRef *cs,
                      ostream *ss)
  {
    dout(10) << "i'm here" << dendl;
    CompressionZlib *interface;
    std::string t;
    interface = new CompressionZlib();

    int r = interface->init(profile, ss);
    if (r) {
      delete interface;
      return r;
    }
    *cs = CompressionInterfaceRef(interface);
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

  return instance.add(plugin_name, new CompressionPluginZlib());
}
