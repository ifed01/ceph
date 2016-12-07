// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FreelistManager.h"
#include "ExtentFreelistManager.h"
#include "BitmapFreelistManager.h"

FreelistManager *FreelistManager::create(
  string type,
  KeyValueDB *kvdb,
  string prefix)
{
  // a bit of a hack... we hard-code the prefixes here.  we need to
  // put the freelistmanagers in different prefixes because the merge
  // op is per prefix, has to done pre-db-open, and we don't know the
  // freelist type until after we open the db.
  assert(prefix == "B" || prefix == "I");
  if (type == "extent")
    return new ExtentFreelistManager(kvdb, "B");
  if (type == "bitmap")
    return new BitmapFreelistManager(kvdb, "B", "b");
  if (type == "bitmap_index")
    return new BitmapFreelistManager(kvdb, "I", "i");
  return NULL;
}

void FreelistManager::setup_merge_operators(KeyValueDB *db)
{
  BitmapFreelistManager::setup_merge_operator(db, "b");
  BitmapFreelistManager::setup_merge_operator(db, "i");
}
