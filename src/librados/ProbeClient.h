
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef PROBE_CLIENT_H_
#define PROBE_CLIENT_H_

#include "msg/Connection.h"
#include "msg/Dispatcher.h"
#include "auth/AuthClient.h"
#include "auth/AuthRegistry.h"
#include "auth/KeyRing.h"
#include "auth/RotatingKeyRing.h"
#include "mon/MonClient.h"

#include <map>
class ProbeClient : public Dispatcher,
		    public AuthClient
{
protected:
  CephContext *cct;
  EntityName entity_name;
  AuthRegistry& auth_reg;
  std::unique_ptr<Messenger> msgr;
  KeyRing keyring;
  RotatingKeyRing rkeyring;

  std::unique_ptr<MonConnection> mc;

  struct ProbeState {
    int id = -1;
    std::string daemon_name;

    size_t count_out = 0;
    size_t count_in = 0;
    double max_rt = 0;
    double sum_rt = 0;
    uint64_t bytes_out = 0;
    uint64_t bytes_in = 0;

    ConnectionRef con;
    ProbeState(int _id, const std::string& _daemon_name, ConnectionRef _con) :
      id(_id), daemon_name(_daemon_name), con(_con) {
    }
    void reset_counters() {
      count_out = 0;
      count_in = 0;
      max_rt = 0.0;
      sum_rt = 0.0;
      bytes_out = 0;
      bytes_in = 0;
    }
    void dump(Formatter* f) const;
  };
  int next_id = 0;
  bool initialized = false;

  std::map<int, ProbeState*> probes_index;
  std::map<Connection*, ProbeState> probes;

  ceph::mutex lock = ceph::make_mutex("ProbeClient::lock");
  ceph::condition_variable shutdown_cond;

protected:
  void _release_connection(Connection* con);

  // Dispatcher overrides
  bool ms_dispatch2(const ceph::ref_t<Message>& m) override;
  bool ms_handle_reset(Connection* con) override;
  void ms_handle_remote_reset(Connection* con) override;
  bool ms_handle_refused(Connection* con) override;

  // AuthClient overrides
  int get_auth_request(
    Connection* con,
    AuthConnectionMeta* auth_meta,
    uint32_t* auth_method,
    std::vector<uint32_t>* preferred_modes,
    ceph::buffer::list* bl) override {
    return mc->get_auth_request(auth_method, preferred_modes, bl,
      cct->_conf->name, 0, &rkeyring);
  }
  int handle_auth_reply_more(
    Connection* con,
    AuthConnectionMeta* auth_meta,
    const ceph::buffer::list& bl,
    ceph::buffer::list* reply) override {
    return mc->handle_auth_reply_more(auth_meta, bl, reply);
  }
  int handle_auth_done(
    Connection* con,
    AuthConnectionMeta* auth_meta,
    uint64_t global_id,
    uint32_t con_mode,
    const ceph::buffer::list& bl,
    CryptoKey* session_key,
    std::string* connection_secret) override {
    return mc->handle_auth_done(auth_meta, global_id, bl,
      session_key, connection_secret);
  }
  int handle_auth_bad_method(
    Connection* con,
    AuthConnectionMeta* auth_meta,
    uint32_t old_auth_method,
    int result,
    const std::vector<uint32_t>& allowed_methods,
    const std::vector<uint32_t>& allowed_modes) override {
    return mc->handle_auth_bad_method(old_auth_method, result,
      allowed_methods, allowed_modes);
  }

public:
  ProbeClient(CephContext *_cct, const EntityName& entity_name, AuthRegistry& _auth_reg);

  void init(const MonMap& monmap);
  void shutdown();

  bool is_initialized() const { return initialized; }

  int probe_connect(int target_type, const std::string& name,
    const entity_addrvec_t& dest);
  int probe_shutdown(int connect_id);

  int probe_send(int connect_id, const std::string& userdata);
  int probe_query(int connect_id, Formatter* fmt, bool reset);
  int probe_query_all(Formatter* fmt, bool reset);
};

#endif
