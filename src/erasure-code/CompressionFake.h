// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_COMPRESSION_FAKE_H
#define CEPH_COMPRESSION_FAKE_H


#include "Compression.h"

namespace ceph {

  class CompressionFake : public Compression {
  public:

    virtual ~CompressionFake() {}
    
    virtual int encode(const bufferlist &in,
                       bufferlist *encoded);
    virtual int decode(const bufferlist &in,
                       bufferlist *decoded);
  };
}

#endif
