// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_MPROBE_H
#define CEPH_MPROBE_H

#include "msg/Message.h"

class MProbe final : public Message {
  std::string userdata;
  utime_t birth_time;
public:
  MProbe() : Message{ MSG_PROBE } {}
  MProbe(const std::string& _userdata) : Message{MSG_PROBE}, userdata{_userdata} {
    birth_time = ceph_clock_now();
  }
private:
  ~MProbe() final {}

public:
  const std::string get_userdata() const {
    return userdata;
  }
  utime_t get_birth_time() const {
    return birth_time;
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(birth_time, p);
    decode(userdata, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(birth_time, payload);
    encode(userdata, payload);
  }
  std::string_view get_type_name() const override { return "probe"; }
};

class MProbeAck final : public Message {
  std::string userdata;  // keeps decoded data only
  utime_t birth_time;    // birth probe timestamp
  MConstRef<MProbe> m;
public:
  MProbeAck() : Message{ MSG_PROBEACK } {}
  MProbeAck(const MConstRef<MProbe> _m) : Message{MSG_PROBEACK}, m(_m) {
  }
private:
  ~MProbeAck() final {}

public:
  const std::string get_userdata() const {
    return userdata;
  }
  utime_t get_birth_time() const {
    return birth_time;
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(birth_time, p);
    decode(userdata, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(m->get_birth_time(), payload);
    encode(m->get_userdata(), payload);
  }
  std::string_view get_type_name() const override { return "probe_ack"; }
};

#endif
