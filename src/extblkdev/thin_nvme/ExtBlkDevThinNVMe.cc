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


#include <errno.h>
#include <string>

#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>

#include "ExtBlkDevThinNVMe.h"
#include "common/blkdev.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/debug.h"


#ifndef NVME_ADMIN_IDENTIFY
#define NVME_ADMIN_IDENTIFY 0x06
#endif

  struct nvme_lbaf {
           __le16 ms;
           __u8 ds;
           __u8 rp;
  };

  struct nvme_id_ns {
           __le64 nsze;
           __le64 ncap;
           __le64 nuse;
           __u8 nsfeat;
           __u8 nlbaf;
           __u8 flbas;
           __u8 mc;
           __u8 dpc;
           __u8 dps;
           __u8 nmic;
           __u8 rescap;
           __u8 fpi;
           __u8 dlfeat;
           __le16 nawun;
           __le16 nawupf;
           __le16 nacwu;
           __le16 nabsn;
           __le16 nabo;
           __le16 nabspf;
           __le16 noiob;
           __u8 nvmcap[16];
           __le16 npwg;
           __le16 npwa;
           __le16 npdg;
           __le16 npda;
           __le16 nows;
           __le16 mssrl;
           __le32 mcl;
           __u8 msrc;
           __u8 rsvd81;
           __u8 nulbaf;
           __u8 rsvd83[9];
           __le32 anagrpid;
           __u8 rsvd96[3];
           __u8 nsattr;
           __le16 nvmsetid;
           __le16 endgid;
           __u8 nguid[16];
           __u8 eui64[8];
           struct nvme_lbaf        lbaf[64];
           __le64 lbstm;
           __u8 vs[3704];
  };

#define dout_subsys ceph_subsys_bdev
#define dout_context cct
#undef dout_prefix
#define dout_prefix *_dout << "ThinNVMe(" << this << ") "

int ExtBlkDevThinNVMe::init(const std::string& alogdevname)
{
  dout(0) << __func__ << " devname:" << alogdevname << dendl;
  logdevname = alogdevname;
  int _dev = -1;
  int _ns = -1;
  int n = -1;
  int r = sscanf(alogdevname.c_str(), "nvme%dn%d%n", &_dev, &_ns, &n);
  if (r == 2 && n == int(alogdevname.length())) {
    // FIXME: make additional checking for vendor/model or something
    ceph_assert(_dev >= 0);
    ceph_assert(_ns >= 0);
    std::string dev_name("/dev/nvme");
    dev_name += stringify(_dev);
    int _fd = open(dev_name.c_str(), O_RDONLY);
    if (_fd < 0) {
      r = -errno;
      derr << __func__ << " failed to open " << dev_name.c_str() << ": " << cpp_strerror(-r) << dendl;
      return r;
    }

    r = 0;
    dev = _dev;
    ns = _ns;
    fd = _fd;
    dout(0) << __func__ << " use dev:" << dev << " ns:" << ns << " as ScaleFlux device" << dendl;
  } else {
    r = -EINVAL;
  }
  return r;
}

int ExtBlkDevThinNVMe::get_statfs(store_statfs_t& buf)
{
  if (fd < 0) {
    return -EBADF;
  }
  struct nvme_admin_cmd cmd = {};
  struct nvme_id_ns ns_data = {};

  cmd.opcode = NVME_ADMIN_IDENTIFY;
  cmd.nsid = ns;
  cmd.addr = (__u64)&ns_data;
  cmd.data_len = sizeof(ns_data);
  cmd.cdw10 = 0; // CNS value for Identify Namespace (0x00)


  int r = ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " ioctl failed: " << r << " " << cpp_strerror(-r) << dendl;
  } else {
    uint64_t lba_size = 1ULL << ns_data.lbaf[ns_data.flbas & 0x0F].ds;

    uint64_t nsze = ns_data.nsze * lba_size;
    uint64_t ncap = ns_data.ncap * lba_size;
    uint64_t nuse = ns_data.nuse * lba_size;

    buf.total = ns_data.nsze * lba_size;
    buf.available = (ns_data.ncap - ns_data.nuse) * lba_size;
    buf.raw_use = ns_data.nuse * lba_size;
    dout(0) << __func__ << " stats (nsze/ncap/nuse):"
                        << nsze << "/"
                        << ncap << "/"
                        << nuse
                        << dendl;
  }
  return r;
}

int ExtBlkDevThinNVMe::collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm)
{
  (*pm)[prefix + "thin_nvme"] = "1";
  return 0;
}
