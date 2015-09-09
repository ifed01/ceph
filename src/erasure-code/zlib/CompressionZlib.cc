/*
 * Ceph - scalable distributed file system
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

// -----------------------------------------------------------------------------
#include "common/debug.h"
#include "CompressionZlib.h"
#include "osd/osd_types.h"
// -----------------------------------------------------------------------------
extern "C" {
#include "zlib-1.2.8/zlib.h"
}
// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

static ostream&
_prefix(std::ostream* _dout)
{
  return *_dout << "CompressionZlib: ";
}
// -----------------------------------------------------------------------------

int CompressionZlib::encode(const bufferlist &in,
                            bufferlist *encoded)
{
  *encoded = in;
  encoded->append("123");
  _profile["plugin_name"] = "CompressionFake";
  return 0;
}

int CompressionZlib::decode(const bufferlist &in,
                            bufferlist *decoded)
{
  *decoded = in;
  return 0;
}
