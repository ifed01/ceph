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

//#include "common/debug.h"
#include "CompressionFake.h"

// #define dout_subsys ceph_subsys_osd
// #undef dout_prefix
// #define dout_prefix _prefix(_dout)

// static ostream& _prefix(std::ostream* _dout)
// {
//   return *_dout << "ErasureCodeJerasure: ";
// }

int CompressionFake::encode(const bufferlist &in,
                            bufferlist *encoded)
{
  *encoded = in;
  encoded->append("123");
  _profile["plugin_name"] = "CompressionFake";
  return 0;
}

int CompressionFake::decode(const bufferlist &in,
                            bufferlist *decoded)
{
  *decoded = in;
  return 0;
}

