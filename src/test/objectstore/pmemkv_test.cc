// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2019-2020, Intel Corporation */

/*
 * pmemkv.cpp -- implementation of pmem kv which uses vector to hold
 * values, string as a key and array to hold buckets
 */

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj.h>
#include <stdexcept>

#include "common/ceph_argparse.h"
#include "global/global_init.h"

#include "kv/pmemkv.h"
//#include "kv/ceph_pmemkv.h"

using namespace pmem_kv;

void
show_usage(char *argv[])
{
	std::cerr << "usage: " << argv[0]
		  << " file-name pmemobj|pmem|mem" << std::endl;
}

const uint64_t GB = (uint64_t)1024 * 1024 * 1024;
int
main(int argc, char *argv[])
{
	vector<const char *> args;
	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			       CODE_ENVIRONMENT_UTILITY,
			       CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
	common_init_finish(g_ceph_context);
	g_ceph_context->_conf.apply_changes(nullptr);

	if (argc < 3) {
		show_usage(argv);
		return 1;
	}

	const char *path = argv[1];
	bool data_in_pmemobj = string(argv[2]) == "pmemobj";
	bool data_in_pmem = string(argv[2]) == "pmem";

	// PMemKeyValueDB kv(nullptr, path);

	pmem::obj::pool<pmem_kv::root> pop;

	int is_pmem = 0;
        size_t len_res = 0;
	void *pmem_addr = nullptr;
	uint64_t data_size = GB * 1;
	uint64_t pool_size = GB * 1;

        if (data_in_pmem) {
                if (access(path, F_OK) != 0) {
                        pmem_addr = pmem_map_file(path, data_size, PMEM_FILE_CREATE,
			                    0, &len_res,
			                    &is_pmem);
	        } else {
		        pmem_addr = pmem_map_file(path, 0, 0, 0,
					          &len_res, &is_pmem);
	        }
		ceph_assert(pmem_addr != 0);
		ceph_assert(len_res == data_size);
		std::cout << "PMEM = " << is_pmem << " size = " << len_res
			  << std::endl;
	} else if (data_in_pmemobj) {
                pool_size += data_size;
	} else {
		pmem_addr = new pmem_kv::byte[data_size];
		std::cout << "MEM " << " size = " << data_size
			  << std::endl;
	}

	try {
		bool first = false;
                string pool_path(path);
		pool_path += "_pmemkv";
		if (access(pool_path.c_str(), F_OK) != 0) {
			std::cout << "Creating " << pool_path << " of "
                                  << pool_size << " bytes" << std::endl;
			pop = pmem::obj::pool<pmem_kv::root>::create(
				pool_path, "pmemkv", pool_size,
                                S_IRWXU);
			first = true;
		} else {
			std::cout << "Opening " << pool_path << std::endl;
			pop = pmem::obj::pool<pmem_kv::root>::open(
				pool_path, "pmemkv");
		}

		DB int_kv_map;

		auto r = pop.root();
		if (data_in_pmemobj) {
			if (first) {
				r->init_data_pmemobj(pop, data_size);
			}
		} else {
			ceph_assert(data_in_pmem || first);
			r->init_data_ext(pmem_addr, data_size, is_pmem != 0);
			std::cout << "data ? " << r->data_size << " "
				  << (uint64_t)r->get_data() << " vs. "
				  << (uint64_t)pmem_addr << std::endl;
		}

		if (first) {
			std::cout << "first tests... " << std::endl;
			int_kv_map.test(pop, true, 100000, 500000);
			std::cout << "first tests once again... " << std::endl;
			int_kv_map.test(pop, true, 400000, 500000);
			std::cout << "test... " << std::endl;
			int_kv_map.test2(pop);

			std::cout << "test3... " << std::endl;
			int_kv_map.test3(pop);
			std::cout << "test3_1... " << std::endl;
			int_kv_map.test3_1(pop);
			std::cout << "test3_2... " << std::endl;
			int_kv_map.test3_2(pop);
			std::cout << "test3_3... " << std::endl;
			int_kv_map.test3_3(pop);
		} else {

			std::cout << "restarted " << int_kv_map.size()
				  << std::endl;

			int_kv_map.test3(pop);
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
	if (data_in_pmem) {
		pmem_unmap(pmem_addr, len_res);
	} else if (!data_in_pmemobj) {
		delete [] (pmem_kv::byte*)pmem_addr;
	}

	return 0;
}
