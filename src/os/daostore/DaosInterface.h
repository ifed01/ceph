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

#ifndef CEPH_OSD_DAOS_INTERFACE_H
#define CEPH_OSD_DAOS_INTERFACE_H

#include <string>
#include <functional>
#include "include/buffer.h"

namespace daostore {

class DaosTransaction {
public:
  void set_key(uint64_t dkey, std::string_view akey, const ceph::bufferlist& val) {}
  void rm_key(uint64_t dkey, std::string_view akey) {}
};

class DaosContainer {
  bool _ready = false;

public:
  int get_key(uint64_t dkey, std::string_view akey, ceph::bufferlist* out);
  int set_key(uint64_t dkey, std::string_view akey, const ceph::bufferlist& val);

  int get_every_key(uint64_t dkey, std::string_view akey, 
    std::function<bool (const ceph::bufferlist&)> cb);
  int submit(DaosTransaction& t);

  int get_key_raw(uint64_t objid, dkey, std::string_view akey, ceph::bufferlist* out);
  bool ready() const {
    return _ready;
  }
};

} // namespace
#endif
