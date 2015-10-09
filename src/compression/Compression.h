// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_COMPRESSION_H
#define CEPH_COMPRESSION_H

/*! @file Compression.h
    @brief Base class for compression plugins implementors

 */ 

#include <vector>

#include "CompressionInterface.h"

namespace ceph {

  class Compression : public CompressionInterface {
  public:

    CompressionProfile _profile;

    virtual ~Compression() {}

    virtual int init(CompressionProfile &profile, ostream *ss) {
      _profile = profile;
      return 0;
    }

    virtual const CompressionProfile &get_profile() const {
      return _profile;
    }

    virtual uint64_t get_block_size(uint64_t stripe_width) const
    {
            return stripe_width * 32; //FIXME: to make configurable or dependant from the compression method
    }

    static int to_int(const std::string &name,
		      CompressionProfile &profile,
		      int *value,
		      const std::string &default_value,
		      ostream *ss);

    static int to_bool(const std::string &name,
		       CompressionProfile &profile,
		       bool *value,
		       const std::string &default_value,
		       ostream *ss);

    static int to_string(const std::string &name,
			 CompressionProfile &profile,
			 std::string *value,
			 const std::string &default_value,
			 ostream *ss);

  };
}

#endif
