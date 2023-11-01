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


#include "ProbeClient.h"

#include "msg/Messenger.h"
#include "messages/MProbe.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados:probe: "

void ProbeClient::ProbeState::dump(Formatter* f) const
{
  f->open_object_section("probe");
  f->dump_int("id", id);
  f->dump_string("daemon_name", daemon_name);
  f->dump_int("count_out", count_out);
  f->dump_int("count_in", count_in);
  f->dump_int("bytes_out", bytes_out);
  f->dump_int("bytes_in", bytes_in);
  f->dump_float("max_roundtrip", max_rt);
  f->dump_float("avg_roundtrip", count_in ? sum_rt / count_in : 0);
  f->close_section();
}

ProbeClient::ProbeClient(CephContext *_cct, const EntityName& _ename, AuthRegistry& _auth_reg)
  : Dispatcher(_cct),
    cct(_cct),
    entity_name(_ename),
    auth_reg(_auth_reg),
    rkeyring(cct, cct->get_module_type(), &keyring)
{
  ceph_assert(cct != nullptr);
}

void ProbeClient::init(const MonMap& monmap)
{
  ceph_assert(!initialized);

  auth_reg.refresh_config();
  keyring.from_ceph_context(cct);

  msgr.reset(Messenger::create_client_messenger(cct, "probe_client"));
  msgr->add_dispatcher_head(this);
  msgr->set_auth_client(this);
  msgr->start();

  ConnectionRef con = msgr->connect_to_mon(monmap.get_addrs(0)); //FIXME: use all ranks?
  ldout(cct, 0) << __func__ << " connect mon"
    << " " << con->get_peer_addr() << dendl;

  mc.reset(new MonConnection(cct, con, 0, &auth_reg));
  mc->start(monmap.get_epoch(), entity_name);

  initialized = true;
}

void ProbeClient::shutdown()
{
  ldout(cct, 10) << __func__ << dendl;
  std::unique_lock l(lock);
  if (!initialized) {
    return;
  }
  initialized = false;
  for (auto& p : probes) {
    p.first->mark_down();
  }
  probes_index.clear();
  probes.clear();

  mc.reset();

  if (msgr) {
    msgr->shutdown();
    msgr->wait();
    msgr.reset();
  }
}

int ProbeClient::probe_connect(int target_type,
  const std::string& name, const entity_addrvec_t& dest)
{
  ldout(cct, 10) << __func__ << " type " << target_type << " name " << name << dendl;
  int r = -EINVAL;
  switch (target_type) {
    case CEPH_ENTITY_TYPE_OSD:
    case CEPH_ENTITY_TYPE_MON:
    case CEPH_ENTITY_TYPE_MDS:
    case CEPH_ENTITY_TYPE_MGR:
    {
      std::unique_lock l(lock);
      ConnectionRef con = msgr->connect_to(target_type, dest, true);
      auto it = probes.lower_bound(con.get());
      if (it == probes.end() || it->first != con.get()) {
        r = ++next_id;
        it = probes.emplace_hint(it, con.get(), ProbeState(r, name, con));
        ProbeState* ps = &(it->second);
        probes_index.emplace(std::make_pair(r, ps));
      } else {
        r = it->second.id;
      }
      break;
    }
  }
  ldout(cct, 10) << __func__ << " ret " << r << dendl;
  return r;
}

int ProbeClient::probe_shutdown(int connect_id)
{
  ldout(cct, 0) << __func__ << " id " << connect_id << dendl;
  int r = 0; // silently ignore non-existent ids
  if (connect_id >= 0) {
    std::unique_lock l(lock);
    auto it = probes_index.find(connect_id);
    if (it != probes_index.end()) {
      it->second->con->mark_down();
      probes.erase(it->second->con.get());
      probes_index.erase(it);
    }
  }
  return r;
}

int ProbeClient::probe_send(int connect_id,  const std::string& userdata)
{
  ldout(cct, 10) << __func__ << " id " << connect_id << dendl;
  int r = -EINVAL;
  if (connect_id >= 0) {
    std::unique_lock l(lock);
    auto it = probes_index.find(connect_id);
    if (it != probes_index.end()) {
      ProbeState* ps = it->second;
      ceph_assert(ps);
      auto* mm = new MProbe(userdata);
      r = ps->con->send_message(mm);
      ++ps->count_out;
      ps->bytes_out += userdata.size();
    }
  }
  ldout(cct, 0) << __func__ << " probe_send = " << r << dendl;
  return r;
}

int ProbeClient::probe_query(int connect_id, 
  Formatter* f, bool reset)
{
  ldout(cct, 10) << __func__ << " id " << connect_id << dendl;
  int r = -EINVAL;
  if (connect_id >= 0 && f) {
    std::unique_lock l(lock);
    auto it = probes_index.find(connect_id);
    if (it != probes_index.end()) {
      ProbeState* ps = it->second;
      ldout(cct, 0) << __func__ << " " << ps->count_out << dendl;
      ceph_assert(ps);
      ps->dump(f);
      if (reset) {
        ps->reset_counters();
      }
      r = 0;
    }
  }
  return r;
}

int ProbeClient::probe_query_all(Formatter* f, bool reset)
{
  ldout(cct, 10) << __func__ << dendl;
  if (!f) {
    return -EINVAL;
  }
  std::unique_lock l(lock);
  f->open_array_section("probe");
  for (auto& p : probes_index) {
    ProbeState* ps = p.second;
    ldout(cct, 0) << __func__ << " " << ps->count_out << dendl;
    ceph_assert(ps);
    ps->dump(f);
    if (reset) {
      ps->reset_counters();
    }
  }
  f->close_section();
  return 0;
}

bool ProbeClient::ms_dispatch2(const ref_t<Message>& m)
{
  std::lock_guard l(lock);
  ldout(cct, 5) << __func__ << "  " << m << dendl;
  switch(m->get_type()) {
  case MSG_PROBEACK:
  {
    auto probe_ack = ref_cast<MProbeAck>(m);
    double rt = ceph_clock_now() - probe_ack->get_birth_time();
    double avg_rt = rt;
    auto probe_it = probes.find(m->get_connection().get());
    int id = probe_it != probes.end() ? probe_it->second.id : -1;
    if (id >= 0) {
      ProbeState& ps = probe_it->second;
      size_t cnt_in = ++(ps.count_in);
      ps.bytes_in += probe_ack->get_userdata().size();
      ps.max_rt = std::max(ps.max_rt, rt);
      ps.sum_rt += rt;
      avg_rt = ps.sum_rt / cnt_in;
    }
    ldout(cct, 0) << __func__ << " probe acked, seq " << m->get_seq()
                  << ", id " << id
                  //<< ", tid " << m->get_tid()
                  << ", payload " << probe_ack->get_userdata().size()
                  << ", rt " << rt
                  << ", avg_rt " << avg_rt
                  << dendl;
    break;
  }
  default:
    ldout(cct, 30) << "Not handling " << *m << dendl; 
    return false;
  }
  return true;
}

void ProbeClient::_release_connection(Connection* con)
{
  ldout(cct, 0) << __func__ << dendl;

  ceph_assert(con);
  auto probe_it = probes.find(con);
  if (probe_it != probes.end()) {
    probes_index.erase(probe_it->second.id);
    probes.erase(probe_it);
  }
}

bool ProbeClient::ms_handle_reset(Connection *con)
{
  std::lock_guard l(lock);
  _release_connection(con);
  return false;
}

void ProbeClient::ms_handle_remote_reset(Connection* con)
{
  std::lock_guard l(lock);
  _release_connection(con);
}

bool ProbeClient::ms_handle_refused(Connection *con)
{
  std::lock_guard l(lock);
  _release_connection(con);
  return false;
}
