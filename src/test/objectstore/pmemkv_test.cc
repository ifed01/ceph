// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2019-2020, Intel Corporation */

/*
 * pmemkv.cpp -- implementation of pmem kv which uses vector to hold
 * values, string as a key and array to hold buckets
 */

#include <libpmemobj.h>
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/pool.hpp>
#include <stdexcept>

#include "kv/pmemkv.h"
#include "kv/ceph_pmemkv.h"

using namespace pmem_kv;

struct root {
	pmem::obj::p<bool> restart;
	pmem::obj::persistent_ptr<pmem_kv_dummy> d1;
};

void
show_usage(char *argv[])
{
	std::cerr << "usage: " << argv[0]
		  << " file-name [get key|put key value]" << std::endl;
}

const uint64_t GB = (uint64_t)1024 * 1024 * 1024;
int
main(int argc, char *argv[])
{
	if (argc < 3) {
		show_usage(argv);
		return 1;
	}

	const char *path = argv[1];
	
        PMemKeyValueDB kv(nullptr, path);

	pmem::obj::pool<root> pop;

	try {
                bool first = false;
                if (access(path, F_OK) != 0) {
			pop = pmem::obj::pool<root>::create(path, "pmemkv",
						 GB * 10 , S_IRWXU);
                        first = true;
		} else {
			pop = pmem::obj::pool<root>::open(path, "pmemkv");
		}

		DB int_kv_map;

		auto r = pop.root();

		if (first) {
			pmem::obj::transaction::run(pop, [&] {
				r->d1 = pmem::obj::make_persistent<
					pmem_kv_dummy>();
				r->d1->a = 1;
				r->d1->b = 2;
			});
			std::cout << "root/dummy created " << std::endl;
		}

		size_t total = 0;
		PMEMoid oid;

		if (first || r->restart) {
			std::cout << "restarted " << int_kv_map.size()
				  << std::endl;
			POBJ_FOREACH(pop.handle(), oid)
			{
				++total;
			}
			std::cout << total << " totals"
				  << std::endl;
                        total = 0;

			int_kv_map.test(pop, false);
			pmem::obj::transaction::run(pop, [&] {
				r->restart = false;
			});
			POBJ_FOREACH(pop.handle(), oid)
			{
				++total;
			}
			std::cout << total
				  << " totals, kvs: " << int_kv_map.size()
				  << std::endl;
			std::cout << "done " << std::endl;
		} else {
			
                        POBJ_FOREACH(pop.handle(), oid)
			{
				++total;
			}
                        int_kv_map.load_from_pool(pop);

			std::cout << total << " totals, kvs: " << int_kv_map.size() << std::endl;

                        std::cout << int_kv_map.size() << std::endl;
			int_kv_map.test(pop, true);
			std::cout << int_kv_map.size() << std::endl;

			pmem::obj::transaction::run(pop, [&] {
                                r->restart = true;
			});

			try {
				pmem::obj::transaction::run(pop, [&] {
					++r->d1->a;
					++r->d1->b;
					pmem::obj::transaction::abort(-1);
				});
			} catch (...) {
			  std::cout << "aborted " << r->d1->a << " "
					  << r->d1->b << " " << r->restart << std::endl;
			}
		}

	} catch (pmem::pool_error &e) {
		std::cerr << e.what() << std::endl;
		std::cerr
			<< "To create pool run: pmempool create obj --layout=pmemkv -s 100M path_to_pool"
			<< std::endl;
	} catch (std::exception &e) {
		std::cerr << e.what() << std::endl;
	}

	try {
		pop.close();
	} catch (const std::logic_error &e) {
		std::cerr << e.what() << std::endl;
	}
	return 0;
}
