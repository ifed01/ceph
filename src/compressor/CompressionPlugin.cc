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

#include <errno.h>
#include <dlfcn.h>

#include "ceph_ver.h"
#include "CompressionPlugin.h"
#include "common/errno.h"
#include "include/str_list.h"

#define PLUGIN_PREFIX "libcs_"
#define PLUGIN_SUFFIX ".so"
#define PLUGIN_INIT_FUNCTION "__compression_init"
#define PLUGIN_VERSION_FUNCTION "__compression_version"

CompressionPluginRegistry CompressionPluginRegistry::singleton;

CompressionPluginRegistry::CompressionPluginRegistry() :
  lock("CompressionPluginRegistry::lock"),
  loading(false),
  disable_dlclose(false)
{
}

CompressionPluginRegistry::~CompressionPluginRegistry()
{
  if (disable_dlclose)
    return;

  for (std::map<std::string,CompressionPlugin*>::iterator i = plugins.begin();
       i != plugins.end();
       ++i) {
    void *library = i->second->library;
    delete i->second;
    dlclose(library);
  }
}

int CompressionPluginRegistry::remove(const std::string &name)
{
  assert(lock.is_locked());
  if (plugins.find(name) == plugins.end())
    return -ENOENT;
  std::map<std::string,CompressionPlugin*>::iterator plugin = plugins.find(name);
  void *library = plugin->second->library;
  delete plugin->second;
  dlclose(library);
  plugins.erase(plugin);
  return 0;
}

int CompressionPluginRegistry::add(const std::string &name,
                                   CompressionPlugin* plugin)
{
  assert(lock.is_locked());
  if (plugins.find(name) != plugins.end())
    return -EEXIST;
  plugins[name] = plugin;
  return 0;
}

CompressionPlugin *CompressionPluginRegistry::get(const std::string &name)
{
  assert(lock.is_locked());
  if (plugins.find(name) != plugins.end())
    return plugins[name];
  else
    return 0;
}

int CompressionPluginRegistry::factory(const std::string &plugin_name,
				       const std::string &directory,
				       CompressorRef *cs,
				       ostream *ss)
{
  CompressionPlugin *plugin;
  {
    Mutex::Locker l(lock);
    plugin = get(plugin_name);
    if (plugin == 0) {
      loading = true;
      int r = load(plugin_name, directory, &plugin, ss);
      loading = false;
      if (r != 0)
	return r;
    }
  }

  return plugin->factory(directory, cs, ss);
}

static const char *an_older_version() {
  return "an older version";
}

int CompressionPluginRegistry::load(const std::string &plugin_name,
				    const std::string &directory,
				    CompressionPlugin **plugin,
				    ostream *ss)
{
  assert(lock.is_locked());
  std::string fname = directory + "/" PLUGIN_PREFIX
    + plugin_name + PLUGIN_SUFFIX;
  void *library = dlopen(fname.c_str(), RTLD_NOW);
  if (!library) {
    *ss << "load dlopen(" << fname << "): " << dlerror();
    return -EIO;
  }

  const char * (*compression_version)() =
    (const char *(*)())dlsym(library, PLUGIN_VERSION_FUNCTION);
  if (compression_version == NULL)
    compression_version = an_older_version;
  if (compression_version() != string(CEPH_GIT_NICE_VER)) {
    *ss << "expected plugin " << fname << " version " << CEPH_GIT_NICE_VER
	<< " but it claims to be " << compression_version() << " instead";
    dlclose(library);
    return -EXDEV;
  }

  int (*compression_init)(const char *, const char *) =
    (int (*)(const char *, const char *))dlsym(library, PLUGIN_INIT_FUNCTION);
  if (compression_init) {
    std::string name = plugin_name;
    int r = compression_init(name.c_str(), directory.c_str());
    if (r != 0) {
      *ss << "compression_init(" << plugin_name
	  << "," << directory
	  << "): " << cpp_strerror(r);
      dlclose(library);
      return r;
    }
  } else {
    *ss << "load dlsym(" << fname
	<< ", " << PLUGIN_INIT_FUNCTION
	<< "): " << dlerror();
    dlclose(library);
    return -ENOENT;
  }

  *plugin = get(plugin_name);
  if (*plugin == 0) {
    *ss << "load " << PLUGIN_INIT_FUNCTION << "()"
	<< "did not register " << plugin_name;
    dlclose(library);
    return -EBADF;
  }

  (*plugin)->library = library;

  *ss << __func__ << ": " << plugin_name << " ";

  return 0;
}

int CompressionPluginRegistry::preload(const std::string &plugins,
				       const std::string &directory,
				       ostream *ss)
{
  Mutex::Locker l(lock);
  list<string> plugins_list;
  get_str_list(plugins, plugins_list);
  for (list<string>::iterator i = plugins_list.begin();
       i != plugins_list.end();
       ++i) {
    CompressionPlugin *plugin;
    int r = load(*i, directory, &plugin, ss);
    if (r)
      return r;
  }
  return 0;
}
