// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "HybridAllocator.h"

#define dout_context (T::get_context())
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << (std::string(this->get_type()) + "::").c_str()

/*int64_t HybridBtree2Allocator::allocate_raw(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint,
  PExtentVector* extents)
{
  ldout(get_context(), 10) << __func__ << std::hex
    << " want 0x" << want
    << " unit 0x" << unit
    << " max_alloc_size 0x" << max_alloc_size
    << " hint 0x" << hint
    << std::dec << dendl;
  ceph_assert(std::has_single_bit(unit));
  ceph_assert(want % unit == 0);

  uint64_t cached_chunk_offs = 0;
  if (try_get_from_cache(&cached_chunk_offs, want)) {
    extents->emplace_back(cached_chunk_offs, want);
    return want;
  }
  return HybridAllocatorBase<Btree2Allocator>::allocate_raw(want,
    unit, max_alloc_size, hint, extents);
}*/

#include "HybridAllocator_impl.h"
