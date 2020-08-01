#pragma once

/*#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "include/buffer.h"
#include "include/ceph_assert.h"
#include "include/intarith.h"
#include <algorithm>
#include <boost/intrusive/any_hook.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/variant.hpp>
#include <functional>
#include <libpmemobj++/allocation_flag.hpp>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/utils.hpp>
#include <libpmemobj.h>
#include <shared_mutex>
#include <stdexcept>
#include <string>*/

#include "common/Clock.h"
#include <libpmem.h>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>

namespace pmem_kv
{
        typedef std::string::value_type byte;
        using byte_array = byte[];
	typedef pmem::obj::persistent_ptr<byte_array> byte_array_ptr;

        // dummy smart ptr to provide access syntax for regular pointer
        // simular to other smart pointers.
        //
        template <class T>
        class dummy_ptr {
                T* p = nullptr;
        public:
		dummy_ptr(T *_p = nullptr) : p(_p) {}
		T* get() { return p; }
		const T* get() const { return p; }
		operator T *() { return p; }
		operator const T *() const { return p; }
		T* operator->() { return p; }
		const T* operator->() const { return p; }
		T& operator*() { return *p; }
		const T& operator*() const { return *p; }
		void reset(T* _p) { p = _p; }
        };

        const size_t PMEM_UNIT_SIZE = 0x100;
        const size_t PMEM_PAGE_SIZE = 0x100;

        struct pmem_page {
		byte b[PMEM_PAGE_SIZE];

		inline byte *
		data()
		{
			return b;
		}
		inline const byte *
		data() const
		{
			return b;
		}
	};

	using pmem_page_ptr = pmem::obj::persistent_ptr<pmem_page[]>;
	using pmem_pages_t = std::pair<uint32_t, pmem_page_ptr>;

        struct alloc_log_handle {
                uint64_t log_pos = 0;
                uint64_t selector = 0;
        };
        struct alloc_bitmap : public std::vector<byte> {
               alloc_bitmap(uint64_t capacity,
                          uint64_t page_size = PMEM_PAGE_SIZE) {
                      ceph_assert(capacity % (PMEM_PAGE_SIZE << 3) == 0);
		      resize(capacity / (PMEM_PAGE_SIZE << 3), 0);
               }
	};

        struct root {
		pmem::obj::p<bool> pmemobj_data;

                pmem_page_ptr abitmap[2]; // 0 - free, 1 - allocated
                pmem::obj::p<uint64_t> abitmap_size = 0;
                pmem::obj::p<uint64_t> effective_abitmap = 0;

                pmem_page_ptr alog;
                pmem::obj::p<uint64_t> alog_size = 0;
                pmem::obj::p<uint64_t> alog_pos = 0;

                pmem::obj::p<uint64_t> data_size = 0;

		bool is_ext_data;
                bool pmem_is_pmem;
                pmem_page_ptr data;
		pmem_page* data_ext = nullptr;

                void
		init_data_ext(void* pmem_addr, uint64_t size, bool _is_pmem) {
			ceph_assert(pmem_addr != nullptr && size != 0);
			ceph_assert((size % PMEM_PAGE_SIZE) == 0);
			ceph_assert((size % (PMEM_PAGE_SIZE << 3)) == 0);

			data_ext = reinterpret_cast<pmem_page*>(pmem_addr);
                        data_size = size;
                        is_ext_data = true;
			pmem_is_pmem = _is_pmem;
                }
		void
		init_data_pmemobj(pmem::obj::pool_base &pool, uint64_t size)
		{
			ceph_assert(data == nullptr);
			ceph_assert(size != 0);
			ceph_assert((size % PMEM_PAGE_SIZE) == 0);
			ceph_assert((size % (PMEM_PAGE_SIZE << 3)) == 0);

			pmem_page_ptr data_local;
			pmem::obj::make_persistent_atomic<pmem_page[]>(
				pool, data_local, size / PMEM_PAGE_SIZE);

			pmem::obj::transaction::run(pool, [&] {
                                data = data_local;
                                data_size = size;
			});
			is_ext_data = false;
                        pmem_is_pmem = false;
		}

                void init_allocations(pmem::obj::pool_base& pool,
                          uint64_t _log_size)
                {
			ceph_assert(data_size != 0);
			ceph_assert((_log_size % PMEM_PAGE_SIZE) == 0);
                        pmem_page_ptr abitmap_local[2];
			uint64_t bmap_size = data_size / (PMEM_PAGE_SIZE << 3);
			ceph_assert((bmap_size % PMEM_PAGE_SIZE) == 0);
			pmem::obj::make_persistent_atomic<pmem_page[]>(
				pool, abitmap_local[0], bmap_size / PMEM_PAGE_SIZE,
				pmem::obj::allocation_flag_atomic(
					POBJ_XALLOC_ZERO));
			pmem::obj::make_persistent_atomic<pmem_page[]>(
				pool, abitmap_local[1], bmap_size / PMEM_PAGE_SIZE,
				pmem::obj::allocation_flag_atomic(
					POBJ_XALLOC_ZERO));

                        pmem_page_ptr alog_local; 
                        pmem::obj::make_persistent_atomic<pmem_page[]>(
                                pool, alog_local,
				_log_size / PMEM_PAGE_SIZE);
			pmem::obj::transaction::run(pool, [&] {
				abitmap_size = bmap_size;
                                effective_abitmap = 0;
				abitmap[0] = abitmap_local[0];
				abitmap[1] = abitmap_local[1];

                                alog_size = _log_size;
                                alog_pos = 0;
                                alog = alog_local;
                        });
		}
                alloc_log_handle get_log_handle() {
                        alloc_log_handle h = {alog_pos, effective_abitmap};
                        return h;
                }
		byte *
		get_data()
		{
                        return is_ext_data ? (byte*)data_ext :(byte*) data[0].data();
                }
                bool commit_allocator_log(pmem::obj::pool_base& pool,
                                           const alloc_log_handle& h,
                                           alloc_bitmap& bmap) {

                        bool bitmap_flushed = false;
                        ceph_assert(h.log_pos <= alog_size);
                        ceph_assert((h.log_pos % (2 * sizeof(int64_t))) == 0);
			if (h.selector != effective_abitmap) {
				ceph_assert(h.log_pos == 0);
				ceph_assert(abitmap_size <= bmap.size());
				pmem_memcpy_nodrain(
					abitmap[h.selector][0].data(),
					bmap.data(), abitmap_size);
				bitmap_flushed = true;
			}
			pmem_drain();

			pmem::obj::transaction::run(pool, [&] {
                                alog_pos = h.log_pos;
                                if (bitmap_flushed) {
                                        effective_abitmap = h.selector;
                                }
                        });
                        return bitmap_flushed;
                }
                inline void log_alloc(alloc_log_handle& h,
                                      alloc_bitmap& bmap,
                                      bool allocated,
                                      uint64_t offs,
                                      uint64_t size) {
                        ceph_assert((offs % PMEM_PAGE_SIZE) == 0);
                        ceph_assert(offs + size <= data_size);
			if (h.selector == effective_abitmap) {
                                ceph_assert(h.log_pos >= alog_pos);
                                if (h.log_pos + 2 * sizeof(int64_t) <= alog_size) {
                                        int64_t *offset_ptr = (int64_t*)(alog[0].data() + h.log_pos);
                                        int64_t o = offs + 1; // adjust to be able to handle offset 0
					*offset_ptr = allocated ? o : -o;
					*(offset_ptr + 1) = (int64_t)size;
					h.log_pos += sizeof(int64_t) * 2;
                                } else {
                                        ++h.selector;
                                        h.selector &= 1;
                                        h.log_pos = 0;
                                }
                        }
                        uint64_t idx = offs / PMEM_PAGE_SIZE;
                        byte &b = bmap.at(idx >> 3);
                        if (allocated) {
				b |= (byte(1) << (idx & 7));
                        } else {
				b &= ~(byte(1) << (idx & 7));
			}
                }
		void
		replay_allocations(alloc_bitmap &bmap,
			           std::function<void(bool, uint64_t, uint64_t)> fn) {
			std::cout << "replay " <<  abitmap_size<< std::endl;
			std::cout << "replay " << effective_abitmap << std::endl;
			std::cout << "replay " << data_size << std::endl;
			std::cout << "replay " << alog_pos << std::endl;

                        uint64_t offs = 0;
			auto base_data = get_data();
			auto p = abitmap[effective_abitmap][0].data();
			ceph_assert(bmap.size() == abitmap_size);
			memcpy(bmap.data(), p, abitmap_size);
			for (uint64_t i = 0; i < abitmap_size; ++i) {
                                byte mask = 1;
                                byte b = p[i];
                                for(size_t j = 0; j < 8; ++j) {
                                        if (b & mask) {
                                                // imply allocated size to be present
                                                // at the beginning of the allocated entry
						uint64_t *p =
							(uint64_t*)(base_data + offs);
                                                fn(true, offs, *p);
                                        }
                                        mask <<= 1;
                                        offs += PMEM_PAGE_SIZE;
                                }
                        }
                        int64_t *p64 = (int64_t*)alog[0].data();
                        for (uint64_t i = 0; i < alog_pos;) {
                                auto o = *p64++;
                                auto sz = *p64++;
                                ceph_assert(o != 0);
				ceph_assert(sz > 0);
				uint64_t offs = std::abs(o) - 1;
				uint64_t idx = offs / PMEM_PAGE_SIZE;
				if (o > 0) {
					fn(true, offs, uint64_t(sz));
					bmap.at(idx >> 3) |= (byte(1) << (idx & 7));
				} else {
					fn(false, offs, uint64_t(sz));
					bmap.at(idx >> 3) &= ~(byte(1) << (idx & 7));
				}
				i += (sizeof(int64_t) << 1);
			}
		}
		void
		for_each(alloc_bitmap &bmap,
			 std::function<void(byte* ptr)> fn) {
			uint64_t offs = 0;
			auto base_data = get_data();
			auto p = bmap.data();
			for (uint64_t i = 0; i < bmap.size(); ++i) {
				byte mask = 1;
				byte b = p[i];
				for (size_t j = 0; j < 8; ++j) {
					if (b & mask) {
						fn(base_data + offs);
					}
					mask <<= 1;
					offs += PMEM_PAGE_SIZE;
				}
			}
		}
        };
}
