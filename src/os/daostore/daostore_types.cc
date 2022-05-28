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

#include "daostore_types.h"
#include "common/Formatter.h"
//#include "common/Checksummer.h"
#include "include/stringify.h"

using std::list;
using std::map;
using std::make_pair;
using std::ostream;
using std::string;

using ceph::bufferlist;
//using ceph::bufferptr;
using ceph::Formatter;

namespace daostore {
// daostore_bdev_label_t

void daostore_bdev_label_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(osd_uuid, bl);
  encode(size, bl);
  encode(btime, bl);
  encode(description, bl);
  encode(meta, bl);
  ENCODE_FINISH(bl);
}

void daostore_bdev_label_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(osd_uuid, p);
  decode(size, p);
  decode(btime, p);
  decode(description, p);
  decode(meta, p);
  DECODE_FINISH(p);
}

void daostore_bdev_label_t::dump(Formatter *f) const
{
  f->dump_stream("osd_uuid") << osd_uuid;
  f->dump_unsigned("size", size);
  f->dump_stream("btime") << btime;
  f->dump_string("description", description);
  for (auto& i : meta) {
    f->dump_string(i.first.c_str(), i.second);
  }
}

void daostore_bdev_label_t::generate_test_instances(
  list<daostore_bdev_label_t*>& o)
{
  o.push_back(new daostore_bdev_label_t);
  o.push_back(new daostore_bdev_label_t);
  o.back()->size = 123;
  o.back()->btime = utime_t(4, 5);
  o.back()->description = "fakey";
  o.back()->meta["foo"] = "bar";
}

ostream& operator<<(ostream& out, const daostore_bdev_label_t& l)
{
  return out << "daos_bdev(osd_uuid " << l.osd_uuid
	     << ", size 0x" << std::hex << l.size << std::dec
	     << ", btime " << l.btime
	     << ", desc " << l.description
	     << ", " << l.meta.size() << " meta"
	     << ")";
}

// cnode_t

void daostore_cnode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(cid, bl);
  encode(bits, bl);
  ENCODE_FINISH(bl);
}

void daostore_cnode_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(cid, p);
  decode(bits, p);
  DECODE_FINISH(p);
}

void daostore_cnode_t::dump(Formatter* f) const
{
  f->dump_unsigned("bits", bits);
}

void daostore_cnode_t::generate_test_instances(list<daostore_cnode_t*>& o)
{
  o.push_back(new daostore_cnode_t());
  o.push_back(new daostore_cnode_t(coll_t(), 3));
  o.push_back(new daostore_cnode_t(coll_t(spg_t(pg_t(0, 1), shard_id_t(1))), 123));
}

ostream& operator<<(ostream& out, const daostore_cnode_t& l)
{
  return out << "cnode(cid " << l.cid << " bits " << l.bits << ")";
}

} // namespace