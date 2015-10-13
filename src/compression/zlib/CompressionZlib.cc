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
  dout(10) << "enter CompressionZlib!" << dendl;

  bufferlist tmp(in);
	unsigned char* c_in = (unsigned char*)tmp.c_str();
  long unsigned int len = in.length();
  long unsigned int new_len = compressBound(len);
  dout(10) << c_in << ">>" << len << "->" << new_len << dendl;

  unsigned char *c_out = new unsigned char [new_len];

  int ret = compress(c_out, &new_len, c_in, len);
  dout(10) << "ret " << ret << " new_len " << new_len << dendl;

  assert(ret == Z_OK);

  encoded->append((char*)c_out, new_len);
  dout(10) << "encoded len " << encoded->length() << dendl;
  return 0;
}

int CompressionZlib::decode(const bufferlist &in,
                            long unsigned int original_size,
                            bufferlist *decoded)
{
  dout(10) << "enter CompressionZlib decompress!" << dendl;

  bufferlist tmp(in);
  unsigned char* c_in = (unsigned char*)tmp.c_str();
  long unsigned int len = in.length();
  unsigned char *c_out = new unsigned char [original_size];

  int ret = uncompress(c_out, &original_size, c_in, len);
  dout(1) << "ret " << ret << " new_len " << original_size<<" "<<in.length() << dendl;

  assert(ret == Z_OK);

  decoded->append((char*)c_out, original_size);
  dout(10) << "decoded len " << decoded->length() << dendl;
  
  return 0;
}
