/*
 * Ceph - scalable distributed file system
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */


#ifndef CEPH_COMPRESSION_ZLIB_H
#define CEPH_COMPRESSION_ZLIB_H

// -----------------------------------------------------------------------------
#include "common/Mutex.h"
#include "erasure-code/Compression.h"
// -----------------------------------------------------------------------------
#include <list>
// -----------------------------------------------------------------------------

class CompressionZlib : public Compression {
public:

  CompressionZlib()
  {
  }

  virtual
  ~CompressionZlib()
  {
  }

  virtual int encode(const bufferlist &in,
                     bufferlist *encoded);
  virtual int decode(const bufferlist &in,
                     bufferlist *decoded);

 };


#endif
