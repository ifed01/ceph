// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

#ifndef COMPRESSION_PLUGIN_H
#define COMPRESSION_PLUGIN_H

#include "common/Mutex.h"
#include "CompressionInterface.h"

extern "C" {
  const char *__compression_version();
  int __compression_init(char *plugin_name, char *directory);
}

namespace ceph {

  class CompressionPlugin {
  public:
    void *library;

    CompressionPlugin() :
      library(0) {}
    virtual ~CompressionPlugin() {}

    virtual int factory(const std::string &directory,
			CompressionProfile &profile,
                        CompressionInterfaceRef *cs,
			ostream *ss) = 0;
  };

  class CompressionPluginRegistry {
  public:
    Mutex lock;
    bool loading;
    bool disable_dlclose;
    std::map<std::string,CompressionPlugin*> plugins;

    static CompressionPluginRegistry singleton;

    CompressionPluginRegistry();
    ~CompressionPluginRegistry();

    static CompressionPluginRegistry &instance() {
      return singleton;
    }

    int factory(const std::string &plugin,
		const std::string &directory,
		CompressionProfile &profile,
		CompressionInterfaceRef *cs,
		ostream *ss);

    int add(const std::string &name, CompressionPlugin *plugin);
    int remove(const std::string &name);
    CompressionPlugin *get(const std::string &name);

    int load(const std::string &plugin_name,
	     const std::string &directory,
	     CompressionPlugin **plugin,
	     ostream *ss);

    int preload(const std::string &plugins,
		const std::string &directory,
		ostream *ss);
  };
}

#endif
