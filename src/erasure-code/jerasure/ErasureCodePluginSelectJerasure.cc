// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
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
#include "arch/probe.h"
#include "arch/intel.h"
#include "arch/arm.h"
#include "erasure-code/ErasureCodePlugin.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginSelectJerasure: ";
}

static string get_variant() {
  ceph_arch_probe();
    
  if (ceph_arch_intel_pclmul &&
      ceph_arch_intel_sse42 &&
      ceph_arch_intel_sse41 &&
      ceph_arch_intel_ssse3 &&
      ceph_arch_intel_sse3 &&
      ceph_arch_intel_sse2) {
    return "sse4";
  } else if (ceph_arch_intel_ssse3 &&
	     ceph_arch_intel_sse3 &&
	     ceph_arch_intel_sse2) {
    return "sse3";
  } else if (ceph_arch_neon) {
    return "neon";
  } else {
    return "generic";
  }
}

class ErasureCodePluginSelectJerasure : public ErasureCodePlugin {
public:

  ErasureCodePluginSelectJerasure(CephContext* cct) : ErasureCodePlugin(cct)
  {}

  virtual int factory(ErasureCodeProfile &profile,
		      ErasureCodeInterfaceRef *erasure_code,
		      ostream *ss) {
    PluginRegistry *reg = cct->get_plugin_registry();
    int ret;
    string name = "jerasure";
    string variant;
    if (profile.count("jerasure-name"))
      name = profile.find("jerasure-name")->second;
    if (profile.count("jerasure-variant")) {
      dout(10) << "jerasure-variant " 
	       << profile.find("jerasure-variant")->second << dendl;
      variant = name + "_" + profile.find("jerasure-variant")->second;
      // ret = instance.factory(name + "_" + profile.find("jerasure-variant")->second,
			   //   directory,
			   //   profile, erasure_code, ss);
    } else {
      variant = name + "_" + get_variant();
      dout(10) << variant << " plugin" << dendl;
      // ret = instance.factory(name + "_" + variant, directory,
			   //   profile, erasure_code, ss);
    }
    ErasureCodePlugin* ecp = dynamic_cast<ErasureCodePlugin*>(reg->get_with_load("erasure-code", variant));
    if (!ecp) {
      derr << "Failed to load plugin " << variant << dendl;
      return -EIO;
    }
    ret = ecp->factory(profile, erasure_code, ss);
    return ret;
  }
};

const char *__ceph_plugin_version() { return CEPH_GIT_NICE_VER; }

int __ceph_plugin_init(CephContext *cct,
                       const std::string& type,
                       const std::string& name)
{
  PluginRegistry *instance = cct->get_plugin_registry();
  string variant = get_variant();
  stringstream ss;
  int r = instance->load("erasure-code", name + string("_") + variant);
  if (r) {
    derr << ss.str() << dendl;
    return r;
  }
  dout(10) << ss.str() << dendl;
  return instance->add(type, name, new ErasureCodePluginSelectJerasure(cct));
}
