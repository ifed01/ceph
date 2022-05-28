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

#ifndef CEPH_OSD_DAOSTORE_TYPES_H
#define CEPH_OSD_DAOSTORE_TYPES_H

#include <ostream>
#include <string>
#include <map>
//#include <bitset>
#include <type_traits>
//#include "include/mempool.h"
#include "include/types.h"
//#include "include/interval_set.h"
#include "include/utime.h"
#include "include/buffer.h"
#include "include/denc.h"
#include "common/hobject.h"
#include "osd/osd_types.h"
//#include "compressor/Compressor.h"
//#include "common/Checksummer.h"

namespace ceph {
  class Formatter;
}

namespace daostore {
/// label for block device
struct daostore_bdev_label_t {
  uuid_d osd_uuid;     ///< osd uuid
  uint64_t size = 0;   ///< device size
  utime_t btime;       ///< birth time
  std::string description;  ///< device description

  std::map<std::string,std::string> meta; ///< {read,write}_meta() content from ObjectStore

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<daostore_bdev_label_t*>& o);
};
WRITE_CLASS_ENCODER(daostore_bdev_label_t)

std::ostream& operator<<(std::ostream& out, const daostore_bdev_label_t& l);

/// collection metadata
struct daostore_cnode_t {
  coll_t cid;
  uint32_t bits;   ///< how many bits of coll pgid are significant

  explicit daostore_cnode_t(coll_t _cid = coll_t(), uint32_t b = 0) :
    cid(_cid), bits(b) {}

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<daostore_cnode_t*>& o);
};
WRITE_CLASS_ENCODER(daostore_cnode_t)

std::ostream& operator<<(std::ostream& out, const daostore_cnode_t& l);

} // namespace
#endif
