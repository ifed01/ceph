#pragma once

#include <memory>
#include <limits>
#include <libpmemobj++/pool.hpp>
#include "common/ceph_mutex.h"
#include "global/global_context.h"
#include "os/bluestore/BitmapAllocator.h"
#include "pmemkv_types.h"

namespace pmem_kv {

class PMemAllocator {
	ceph::mutex lock = ceph::make_mutex("PMemAllocator::lock");
	pmem::obj::pool<pmem_kv::root> *pop = nullptr;
	std::unique_ptr<Allocator> alloc = nullptr;
        bool need_commit = false;
        alloc_log_handle alloc_handle;
        std::unique_ptr<alloc_bitmap> bmap;
	interval_set<uint64_t> release_set;
        size_t bmap_flush_count = 0;
public:
        virtual ~PMemAllocator()
        {
                shutdown();
        }
	size_t
	get_flushes() const {
                return bmap_flush_count;
        }
	inline size_t
	get_free() const
	{
		return alloc->get_free();
	}

	void create(pmem::obj::pool<pmem_kv::root> &_pop,
		  uint64_t _log_size) {
		ceph_assert(pop == nullptr);
                pop = &_pop;
		auto data_size = _pop.root()->data_size;
		pop->root()->init_allocations(*pop, _log_size);
		/*alloc.reset(new AvlAllocator(
			g_ceph_context, data_size, PMEM_PAGE_SIZE,
			"pmem allocator"));
		alloc.reset(new HybridAllocator(
			g_ceph_context, data_size,
					    PMEM_PAGE_SIZE,
			                    std::numeric_limits<int>::max(),
                                            "pmem allocator"));*/
		alloc.reset(new BitmapAllocator(g_ceph_context, data_size,
					    PMEM_PAGE_SIZE,
					    "pmem allocator"));
		alloc->init_add_free(0, data_size);
		bmap.reset(new alloc_bitmap(data_size));
        }
        void load(pmem::obj::pool<pmem_kv::root> &_pop,
                  std::function<void(byte* ptr)> fn) {
		ceph_assert(pop == nullptr);
		pop = &_pop;
		auto data_size = _pop.root()->data_size;
		std::cout << "? " << data_size << std::endl;
		/*alloc.reset(new AvlAllocator(
			g_ceph_context, data_size, PMEM_PAGE_SIZE,
			"pmem allocator"));
		alloc.reset(new HybridAllocator(
			            g_ceph_context, data_size,
				    PMEM_PAGE_SIZE,
                                    std::numeric_limits<int>::max(),
				    "pmem allocator"));*/
		alloc.reset(new BitmapAllocator(g_ceph_context, data_size,
						PMEM_PAGE_SIZE,
						"pmem allocator"));
		alloc->init_add_free(0, data_size);
		bmap.reset(new alloc_bitmap(data_size));
		pop->root()->replay_allocations(*bmap,
                        [&](bool is_alloc, uint64_t offs, uint64_t size) {
				/*std::cout << " >> " << offs
					  << "~" << size << std::endl;*/
                                ceph_assert(size != 0);
                                if (is_alloc) {
					alloc->init_rm_free(offs, size);
                                } else {
					alloc->init_add_free(offs, size);
                                }
                        });
		pop->root()->for_each(*bmap, fn);
        }
	void
	shutdown()
        {
		std::lock_guard l(lock);
		pop = nullptr;
                alloc.reset(nullptr);
		bmap.reset(nullptr);
                need_commit = false;
		release_set.clear();
		bmap_flush_count = 0;
        }
	pmem_kv::byte *
        allocate(uint64_t size, bool log_alloc)
        {
	        std::lock_guard l(lock);
	        PExtentVector extents;
	        int64_t r = alloc->allocate(size, size, size, 0, &extents);
	        if (r < 0) {
		        return nullptr;
	        }
		ceph_assert(size == (uint64_t)r);
		ceph_assert(extents.size() == 1);
		if (log_alloc) {
	                if (!need_commit) {
		                need_commit = true;
		                alloc_handle = pop->root()->get_log_handle();
	                }
	                pop->root()->log_alloc(alloc_handle, *bmap, true, extents[0].offset, size);
                }
		return pop->root()->get_data() + extents[0].offset;
        }
        void
	release(const pmem_kv::byte *ptr, uint64_t size, bool log_alloc)
        {
	        ceph_assert(ptr >= pop->root()->get_data());
	        uint64_t offset = ptr - pop->root()->get_data();
	        std::lock_guard l(lock);


                if (log_alloc) {
			release_set.union_insert(offset, size);
			if (!need_commit) {
		                need_commit = true;
		                alloc_handle = pop->root()->get_log_handle();
	                }
		        pop->root()->log_alloc(alloc_handle, *bmap, false, offset,
				               size);
                } else {
                        PExtentVector pext;
			pext.emplace_back(offset, size);
			alloc->release(pext);
		}
        }
	void
	log_alloc(const pmem_kv::byte *ptr, uint64_t size)
	{
		ceph_assert(ptr >= pop->root()->get_data());
		uint64_t offset = ptr - pop->root()->get_data();
		std::lock_guard l(lock);
		if (!need_commit) {
			need_commit = true;
			alloc_handle = pop->root()->get_log_handle();
		}
		pop->root()->log_alloc(alloc_handle, *bmap, true,
					offset, size);
		if (pop->root()->pmem_is_pmem) {
			pmem_flush(ptr, size);
		} else {
			pmem_msync(ptr, size);
                }
	}
        void
        commit()
        {
	        std::lock_guard l(lock);
	        if (need_commit) {
	                bool bmap_flushed = pop->root()->commit_allocator_log(*pop, alloc_handle, *bmap);
			if (bmap_flushed)
                                ++bmap_flush_count;
	                need_commit = false;

                        if (!release_set.empty()) {
				alloc->release(release_set);
				release_set.clear();
                        }
                }
        }
};
}; // namespace