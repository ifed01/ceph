// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <vector>
#include <algorithm>
#include <ostream>

#include "common/strtol.h"
#include "Compression.h"


int Compression::to_int(const std::string &name,
			CompressionProfile &profile,
			int *value,
			const std::string &default_value,
			ostream *ss)
{
  if (profile.find(name) == profile.end() ||
      profile.find(name)->second.size() == 0)
    profile[name] = default_value;
  std::string p = profile.find(name)->second;
  std::string err;
  int r = strict_strtol(p.c_str(), 10, &err);
  if (!err.empty()) {
    *ss << "could not convert " << name << "=" << p
	<< " to int because " << err
	<< ", set to default " << default_value << std::endl;
    *value = strict_strtol(default_value.c_str(), 10, &err);
    return -EINVAL;
  }
  *value = r;
  return 0;
}

int Compression::to_bool(const std::string &name,
			 CompressionProfile &profile,
			 bool *value,
			 const std::string &default_value,
			 ostream *ss)
{
  if (profile.find(name) == profile.end() ||
      profile.find(name)->second.size() == 0)
    profile[name] = default_value;
  const std::string p = profile.find(name)->second;
  *value = (p == "yes") || (p == "true");
  return 0;
}

int Compression::to_string(const std::string &name,
			   CompressionProfile &profile,
			   std::string *value,
			   const std::string &default_value,
			   ostream *ss)
{
  if (profile.find(name) == profile.end() ||
      profile.find(name)->second.size() == 0)
    profile[name] = default_value;
  *value = profile[name];
  return 0;
}

