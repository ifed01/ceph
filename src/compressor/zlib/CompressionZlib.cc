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

#include <zlib.h>

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

const char* CompressionZlib::get_method_name()
{
	return "zlib";
}

int CompressionZlib::compress(bufferlist &in, bufferlist &out)
{
  unsigned char* c_in;
  // compute max len to avoid reallocation
  long unsigned int new_len = compressBound(in.length());
  long unsigned int future_len, len;

  unsigned char *c_out = new unsigned char [new_len];
  // here not good to use ptr, because real len will be
  // less than new_len value
  //buffer::ptr c_out(new_len);

  for (std::list<buffer::ptr>::const_iterator i = in.buffers().begin();
          i != in.buffers().end(); ++i) {

    future_len = new_len;
    c_in = (unsigned char*)(*i).c_str();
    len = (*i).length();
    int ret = ::compress(c_out,
                         &future_len,
                         c_in,
                         len);
    if (ret != Z_OK) {
      dout(10) << "Compression error: compress return "
           << ret << "instead of Z_OK" << dendl;
      return 1;
    }

    buffer::ptr pc_out(new_len + sizeof(len) + 1);
    pc_out.set_length(0);
    // we need save original data len
    char* blen = (char*)&len;
    pc_out.append(version);
    pc_out.append(blen, sizeof(len));

    pc_out.append((char*)c_out, new_len);
    out.append(pc_out);
  }

  delete [] c_out;
  return 0;
}

int CompressionZlib::decompress(bufferlist &in, bufferlist &out)
{
  long unsigned int original_size = 0, len = 0;
  unsigned char* c_in;

  for (std::list<buffer::ptr>::const_iterator i = in.buffers().begin();
          i != in.buffers().end(); ++i) {

    c_in = (unsigned char*)(*i).c_str();
    len = (*i).length();

    //get original data len from prefix
    char saved_version = c_in[0];
    if (saved_version != version) {
      dout(10) << "Decompression error: other compressor version used" << dendl;
      return 1;
    }

    original_size = *((long unsigned int*)(c_in+1));
    c_in = c_in + sizeof(long unsigned int) + 1;

    buffer::ptr c_out(original_size);

    int ret = uncompress((unsigned char *)c_out.c_str(),
                          &original_size,
                          c_in,
                          len);
     if (ret != Z_OK) {
      dout(10) << "Decompression error: uncompress return " << ret << "instead of Z_OK" << dendl;
      return 1;
    }

    out.append(c_out);
  }

  return 0;
}
