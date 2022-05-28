// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_DAOSTORE_NAMING_H
#define CEPH_OSD_DAOSTORE_NAMING_H

#include <string>
#include "common/hobject.h"
#include "include/byteorder.h"

namespace daostore {

/*
Ceph        -> DAOS mapping:
Cluster     -> Container
OSD super   -> objid = 0, dkey = osd_fsid (or id???), akey = "superblock"[0]
                      ****               , akey = "_collections"[*] - collection (= tuple<coll_t, daostore_cnode_t>)

Pool        -> Set of Pool Shards (PS), each shard is an object with PSid = (pool_id << 12) + shard_id. shard_id are low 12 bits of oid hash
PG          -> Subset of Pool Shards, i.e. objects matching the following mask PSid = (pool_id << 12) | *: (12-cbits) | ( pg_id & ((1 << cbits) - 1))
               So for pool 1, cbits = 3, pgid =1 relevant PSid-s would be: 0x1001, 0x1009, 0x1011, 0x1019, 0x1021 etc...

Onodes list -> objid = PSid, dkey="list", akey = oid
Onode       -> objid = PSid, dkey = oid in the relevant Pool Shard object
Onode meta  -> objid = PSid, dkey=oid, akey = "meta"
Onode attr  -> objid = PSid, dkey=oid, akey = A_<attr_name> // will need to enumerate attrs when enumerating omaps
Onode omap  -> objid = PSid, dkey=oid, akey = O_<omap_name>

*/

/////////////////// superblock ///////////////////////
const uint64_t SUPER_DKEY = 0x8000000000000000ull;
const std::string SUPERBLOCK_LABEL_SUFFIX = "_slabel";
const std::string CNODES_SUFFIX = "_cnodes";

/*
SUPER_DKEY (= 1 << 63)
  osd_fsid + "_slabel"[0] - OSD super block
  osd_fsid + "_collections"[*] - per-osd collections (= tuple<coll_t, daostore_cnode_t>)

*/


std::string build_full_akey(std::string_view s1, std::string_view s2) {
  std::string res;
  res.reserve(s1.size() + s2.size());
  res = s1;
  res += s2;
  return res;
}

/////////////////// some key encoding helpers ///////////////////////
template<typename T>
inline static void _key_encode_u32(uint32_t u, T *key) {
  uint32_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 4);
}

template<typename T>
inline static void _key_encode_u32(uint32_t u, size_t pos, T *key) {
  uint32_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab(u);
#else
# error wtf
#endif
  key->replace(pos, sizeof(bu), (char*)&bu, sizeof(bu));
}

inline static const char *_key_decode_u32(const char *key, uint32_t *pu) {
  uint32_t bu;
  memcpy(&bu, key, 4);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab(bu);
#else
# error wtf
#endif
  return key + 4;
}

template<typename T>
inline static void _key_encode_u64(uint64_t u, T *key) {
  uint64_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 8);
}

inline static const char *_key_decode_u64(const char *key, uint64_t *pu) {
  uint64_t bu;
  memcpy(&bu, key, 8);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab(bu);
#else
# error wtf
#endif
  return key + 8;
}

/////////////////// Onode naming stuff ///////////////////////
 /*
  * object name key structure
  *
  * 'o'
  * encoded u8: shard + 2^7 (so that it sorts properly)
  * encoded u64: poolid + 2^63 (so that it sorts properly)
  * encoded u32: hash (bit reversed)
  *
  * escaped string: namespace
  *
  * escaped string: key or object name
  * 1 char: '<', '=', or '>'.  if =, then object key == object name, and
  *         we are done.  otherwise, we are followed by the object name.
  * escaped string: object name (unless '=' above)
  *
  * encoded u64: snap
  * encoded u64: generation
  * 'o'
  */
#define ONODE_KEY_PREFIX 'o'
#define ONODE_KEY_SUFFIX 'o'
#define ENCODED_KEY_PREFIX_LEN (1 + 8 + 4)

  /*
   * string encoding in the key
   *
   * The key string needs to lexicographically sort the same way that
   * ghobject_t does.  We do this by escaping anything <= to '#' with #
   * plus a 2 digit hex string, and anything >= '~' with ~ plus the two
   * hex digits.
   *
   * We use ! as a terminator for strings; this works because it is < #
   * and will get escaped if it is present in the string.
   *
   */
template<typename S>
static void append_escaped(const std::string& in, S* out)
{
  char hexbyte[in.length() * 3 + 1];
  char* ptr = &hexbyte[0];
  for (std::string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (unsigned(*i) <= '#') {
      *ptr++ = '#';
      *ptr++ = "0123456789abcdef"[(*i >> 4) & 0x0f];
      *ptr++ = "0123456789abcdef"[*i & 0x0f];
    }
    else if (unsigned(*i) >= '~') {
      *ptr++ = '~';
      *ptr++ = "0123456789abcdef"[(*i >> 4) & 0x0f];
      *ptr++ = "0123456789abcdef"[*i & 0x0f];
    }
    else {
      *ptr++ = *i;
    }
  }
  *ptr++ = '!';
  out->append(hexbyte, ptr - &hexbyte[0]);
}

inline unsigned h2i(char c);
int decode_escaped(const char* p, std::string* out);

template<typename S>
static void _key_encode_prefix(const ghobject_t& oid, S* key)
{
  key->push_back((char)((uint8_t)oid.shard_id + (uint8_t)0x80));
  _key_encode_u64(oid.hobj.pool + 0x8000000000000000ull, key);
  _key_encode_u32(oid.hobj.get_bitwise_key_u32(), key);
}

template<typename S>
static void get_object_key(CephContext* cct, const ghobject_t& oid, S* key)
{
  key->clear();

  size_t max_len = 
    1 + // onode prefix
    ENCODED_KEY_PREFIX_LEN +
    (oid.hobj.nspace.length() * 3 + 1) +
    (oid.hobj.get_key().length() * 3 + 1) +
    1 +  // for '<', '=', or '>'
    (oid.hobj.oid.name.length() * 3 + 1) +
    8 +  // snap
    8 +  // generation
    1; // onode suffix
  key->reserve(max_len);

  key->push_back(ONODE_KEY_PREFIX);

  _key_encode_prefix(oid, key);

  append_escaped(oid.hobj.nspace, key);

  if (oid.hobj.get_key().length()) {
    // is a key... could be < = or >.
    append_escaped(oid.hobj.get_key(), key);
    // (ASCII chars < = and > sort in that order, yay)
    int r = oid.hobj.get_key().compare(oid.hobj.oid.name);
    if (r) {
      key->append(r > 0 ? ">" : "<");
      append_escaped(oid.hobj.oid.name, key);
    } else {
      // same as no key
      key->append("=");
    }
  } else {
    // no key
    append_escaped(oid.hobj.oid.name, key);
    key->append("=");
  }

  _key_encode_u64(oid.hobj.snap, key);
  _key_encode_u64(oid.generation, key);

  key->push_back(ONODE_KEY_SUFFIX);

  // sanity check
/*  if (true) {
    ghobject_t t;
    int r = get_key_object(*key, &t);
    if (r || t != oid) {
      derr << "  r " << r << dendl;
      derr << "key " << pretty_binary_string(*key) << dendl;
      derr << "oid " << oid << dendl;
      derr << "  t " << t << dendl;
      ceph_assert(r == 0 && t == oid);
    }
  }*/
}
} // namespace

#endif
