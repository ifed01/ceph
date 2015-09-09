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
	char* data = in.c_str();
    int ret, flush;
    unsigned have;
    z_stream strm;
    int CHUNK = sizeof(data);
    unsigned char *in = new unsigned char [CHUNK];
    unsigned char *out = new unsigned char [CHUNK];

    /* allocate deflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, level);
    if (ret != Z_OK)
        return ret;

    strm.avail_in = data;
    flush = Z_FINISH;
    strm.next_in = in;

    do {
        strm.avail_out = CHUNK;
        strm.next_out = out;
        ret = deflate(&strm, flush);    /* no bad return value */
        assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
        encoded->append(ret);
        have = CHUNK - strm.avail_out;
    } while (strm.avail_out == 0);
    assert(strm.avail_in == 0);     /* all input will be used */

    assert(ret == Z_STREAM_END);        /* stream will be complete */

    /* clean up and return */
    (void)deflateEnd(&strm);
    return 0;
}

int CompressionZlib::decode(const bufferlist &in,
                            bufferlist *decoded)
{
	char* data = in.c_str();
	int ret, flush;
	unsigned have;
	z_stream strm;
	int CHUNK = sizeof(data);
	unsigned char *in = new unsigned char [CHUNK];
	unsigned char *out = new unsigned char [CHUNK];
	strm.avail_in = data;
    strm.next_in = in;

    do {
        strm.avail_out = CHUNK;
        strm.next_out = out;
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
