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

extern "C" {
#include <dre_api.h>
}

#include "acconfig.h"
#include "ceph_ver.h"
#include "common/ceph_context.h"
#include "common/debug.h"
#include "CompressionPluginMxl.h"

#define dout_context cct
#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix *_dout << "mxl plugin:"

// -----------------------------------------------------------------------------

const char *__ceph_plugin_version()
{
  return CEPH_GIT_NICE_VER;
}

// -----------------------------------------------------------------------------

int __ceph_plugin_init(CephContext *cct,
                       const std::string& type,
                       const std::string& name)
{
  auto mxl_plugin = new CompressionPluginMxl(cct);
  ceph_assert(mxl_plugin);
  int r = mxl_plugin->init();
  if (r >= 0) {
    auto instance = cct->get_plugin_registry();
    return instance->add(type, name, mxl_plugin);
  }
  return r;
}

int CompressionPluginMxl::init()
{
  DRE_status ret = DRE_OK;
  DRE_u32b numCards = 0;
  if (initialized) {
    return 0;
  }

  ret = DRE_apiSysInit();
  if(DRE_IS_RESULT_ERR(ret))
  {
    lderr(cct) << "DRE_apiSysInit() failed:" << ret << dendl;
    return -1;
  }
  ret = DRE_cardInfoGet(&numCards, NULL);
  if(DRE_IS_RESULT_ERR(ret))
  {
    lderr(cct) << "DRE_cardInfoGet() failed:" << ret << dendl;
    return -1;
  }
  ldout(cct, 0) << " " << numCards << " MXL cards found" << dendl;
  initialized = true;
  return 0;
}

void CompressionPluginMxl::deinit()
{
  if( initialized) {
    DRE_apiSysExit();
    initialized = false;
  }
}
