// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

#include "ceph_ver.h"
#include "common/debug.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "ErasureCodeLrc.h"

// re-include our assert
#include "include/assert.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

class ErasureCodePluginLrc : public ErasureCodePlugin {
public:

  ErasureCodePluginLrc(CephContext* cct) : ErasureCodePlugin(cct)
  {}

  virtual int factory(ErasureCodeProfile &profile,
		      ErasureCodeInterfaceRef *erasure_code,
		      ostream *ss) {
    ErasureCodeLrc *interface;
    interface = new ErasureCodeLrc();
    int r = interface->init(profile, ss);
    if (r) {
      delete interface;
      return r;
    }
    *erasure_code = ErasureCodeInterfaceRef(interface);
    return 0;
  }
};

const char *__ceph_plugin_version() { return CEPH_GIT_NICE_VER; }

int __ceph_plugin_init(CephContext *cct,
                       const std::string& type,
                       const std::string& name)
{
  PluginRegistry *instance = cct->get_plugin_registry();
  return instance->add(type, name, new ErasureCodePluginLrc(cct));
}
