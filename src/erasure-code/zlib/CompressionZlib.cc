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
  dout(10) << "ret " << ret << dendl;

  assert(ret == Z_OK);

  encoded->append((char*)c_out, new_len);
  return 0;
}

int CompressionZlib::decode(const bufferlist &in,
                            bufferlist *decoded)
{
  int level = Z_DEFAULT_COMPRESSION;
  bufferlist tmp(in);
	char* data = tmp.c_str();
	int ret, flush;
	unsigned have;
	z_stream strm;
	int CHUNK = sizeof(data);
	unsigned char *c_in = (unsigned char*)data;//new unsigned char [CHUNK];
	unsigned char *c_out = new unsigned char [CHUNK];
	strm.avail_in = CHUNK;
    strm.next_in = c_in;

    do {
        strm.avail_out = CHUNK;
        strm.next_out = c_out;
        ret = inflate(&strm, Z_NO_FLUSH);
        assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
        switch (ret) {
        case Z_NEED_DICT:
            ret = Z_DATA_ERROR;     /* and fall through */
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            (void)inflateEnd(&strm);
            return ret;
        }
        have = CHUNK - strm.avail_out;
        decoded->append(ret);
    } while (strm.avail_out == 0);

	(void)inflateEnd(&strm);
	return ret == Z_STREAM_END ? 0 : Z_DATA_ERROR;
 //return 0;
}
