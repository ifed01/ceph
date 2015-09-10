// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_COMPRESSION_INTERFACE_H
#define CEPH_COMPRESSION_INTERFACE_H


#include <map>
#include <set>
#include <vector>
#include <iostream>
#include "include/memory.h"
#include "include/buffer.h"

using namespace std;

namespace ceph {

  typedef map<std::string,std::string> CompressionProfile;

  // inline ostream& operator<<(ostream& out, const CompressionProfile& profile) {
  //   out << "{";
  //   for (CompressionProfile::const_iterator it = profile.begin();
	 // it != profile.end();
	 // ++it) {
  //     if (it != profile.begin()) out << ",";
  //     out << it->first << "=" << it->second;
  //   }
  //   out << "}";
  //   return out;
  // }


  class CompressionInterface {
  public:
    virtual ~CompressionInterface() {}
    string name;

    /**
     * Initialize the instance according to the content of
     * **profile**. The **ss** stream is set with debug messages or
     * error messages, the content of which depend on the
     * implementation.
     *
     * Return 0 on success or a negative errno on error. When
     * returning on error, the implementation is expected to
     * provide a human readable explanation in **ss**.
     *
     * @param [in] profile a key/value map
     * @param [out] ss contains informative messages when an error occurs
     * @return 0 on success or a negative errno on error.
     */
    virtual int init(CompressionProfile &profile, ostream *ss) = 0;

    /**
     * Return the profile that was used to initialize the instance
     * with the **init** method.
     *
     * @return the profile in use by the instance
     */
    virtual const CompressionProfile &get_profile() const = 0;

    virtual int encode(const bufferlist &in,
                       bufferlist *encoded) = 0;
    virtual int decode(const bufferlist &in,
                       bufferlist *decoded) = 0;


  };

  typedef ceph::shared_ptr<CompressionInterface> CompressionInterfaceRef;

}

#endif
