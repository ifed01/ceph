#include "pmemkv.h"
#include "include/stringify.h"
#include <sstream>

const pmem_kv::buffer_view
pmem_kv::string_to_view(const std::string &s)
{
	pmem_kv::buffer_view sv;
	sv.append(s.c_str(), s.size());
	return sv;
}

const pmem_kv::buffer_view
pmem_kv::string_to_view(const char *s)
{
	pmem_kv::buffer_view sv;
	sv.append(s, strlen(s));
	return sv;
}

std::ostream &
pmem_kv::operator<<(std::ostream &out, const pmem_kv::pmem_kv_entry &e)
{
	e.dump(out);
	return out;
}

#ifndef USE_CEPH_BUFFER_LIST
std::ostream &
pmem_kv::operator<<(std::ostream &out, const pmem_kv::buffer_view &e)
{
	e.dump(out);
	return out;
}
#endif

void
pmem_kv::DB::test(pmem::obj::pool_base &pool, bool remove)
{
	/*{
	  my_test mt;
		volatile_buffer bv(std::move(mt));
	}*/

	bool was_empty = kv_set.empty();
	pmem_kv::volatile_buffer fake_key = string("fakeeee keyyyy");

	if (was_empty) {
		// basic cases on empty kv
		ceph_assert(empty());
		ceph_assert(size() == 0);
		auto p = get(fake_key);
		std::cout << 2 << std::endl;

		ceph_assert(p.get() == nullptr);

		ceph_assert(begin().at_end());
		ceph_assert((*begin()).get() == nullptr);
		std::cout << 3 << std::endl;
	}
	if (was_empty) {
		// basic ops on a single key
		batch batch;
		pmem_kv::buffer_view k = string_to_view("some_key");
		pmem_kv::buffer_view v = string_to_view("some_value");

		// create
		batch.set(k, v);

		apply_batch(pool, batch);

		ceph_assert(!empty());
		ceph_assert(size() == 1);
		std::cout << 4 << std::endl;

		auto p = get(fake_key);
		ceph_assert(p.get() == nullptr);

		// read
		p = get(k);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p == first());
		ceph_assert(p->key_view() == k);
		ceph_assert(p->value_view() == v);

		ceph_assert(!begin().at_end());
		ceph_assert(first() == last());
		ceph_assert(*begin() == *find(k));
		ceph_assert(*begin() == *lower_bound(k));
		ceph_assert(upper_bound(k).at_end());
		ceph_assert((*upper_bound(k)).get() == nullptr);
		auto it = begin();
		ceph_assert(!begin().at_end());

		// remove
		batch.reset();

		batch.remove((*it)->key_view());

		apply_batch(pool, batch);
		std::cout << 5 << std::endl;

		ceph_assert(size() == 0);
		ceph_assert(begin().at_end());
		ceph_assert(*begin() == nullptr);
		ceph_assert(it.at_end());
		ceph_assert(*it == nullptr);

		// multi-chunk bufferlist + move semantics
		{
			std::cout << 59 << std::endl;
			batch.reset();
			std::string kk("some key2");
			bufferlist key_bl;
			key_bl.append(kk);
			bufferlist val_bl;
			uint32_t GB = uint32_t(1024) * 1024 * 1024;
			val_bl.append('1');
			val_bl.append_zero(GB / 2);
			val_bl.append('2');
			val_bl.append_zero(GB / 2);
			val_bl.append('3');

			std::cout << 591 << std::endl;

			batch.set(std::move(key_bl), std::move(val_bl));
			ceph_assert(key_bl.length() == 0);
			ceph_assert(val_bl.length() == 0);
			std::cout << 592 << std::endl;

			apply_batch(pool, batch);
			std::cout << 6 << std::endl;

			p = get(kk);
			ceph_assert(p.get() != nullptr);
			ceph_assert(p == first());
			ceph_assert(p->key_view() == kk);
			ceph_assert(p->value_view().length() == GB + 3);
			// smoke content verification
			ceph_assert(p->value_view().c_str()[0] == '1');
			ceph_assert(p->value_view().c_str()[1] == 0);
			ceph_assert(p->value_view().c_str()[GB / 2] == 0);
			ceph_assert(p->value_view().c_str()[GB / 2 + 1] == '2');
			ceph_assert(p->value_view().c_str()[GB / 2 + 2] == 0);
			ceph_assert(p->value_view().c_str()[GB] == 0);
			ceph_assert(p->value_view().c_str()[GB + 1] == 0);
			ceph_assert(p->value_view().c_str()[GB + 2] == '3');

			erase(pool, kk);
		}
	}
	if (was_empty) {
		// basic ops on 3 keys
		batch batch;
		pmem_kv::buffer_view k = string_to_view("some5_key");
		pmem_kv::buffer_view v = string_to_view("some5_value");
		pmem_kv::buffer_view k2 = string_to_view("some2_key");
		pmem_kv::buffer_view v2 = string_to_view("some2_value");
		pmem_kv::buffer_view k3 = string_to_view("some3_key");
		pmem_kv::buffer_view v3 = string_to_view("some3_value");

		// create
		batch.set(k2, v2);
		batch.set(k, v);
		batch.set(k3, v3);
		apply_batch(pool, batch);
		std::cout << 7 << std::endl;

		ceph_assert(!empty());
		ceph_assert(size() == 3);

		auto p = get(fake_key);
		ceph_assert(p.get() == nullptr);

		// read
		p = get(k3);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p != first());
		ceph_assert(p != last());
		ceph_assert(p->key_view() == k3);
		ceph_assert(p->value_view() == v3);

		p = get(k2);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p == first());
		ceph_assert(p != last());
		ceph_assert(p->key_view() == k2);
		ceph_assert(p->value_view() == v2);

		p = get(k);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p != first());
		ceph_assert(p == last());
		ceph_assert(p->key_view() == k);
		ceph_assert(p->value_view() == v);

		// iterate
		auto it = begin();
		ceph_assert(!it.at_end());
		ceph_assert(it->key_view() == k2);
		++it;
		ceph_assert(it->key_view() == k3);
		++it;
		ceph_assert(it->key_view() == k);

		// remove
		it = begin();
		ceph_assert(!it.at_end());
		while (!it.at_end()) {
			batch.reset();
			batch.remove((*it)->key_view());
			// std::cout << " removing " <<  **it << std::endl;
			apply_batch(pool, batch);
			++it;
		}

		ceph_assert(size() == 0);
		ceph_assert(begin().at_end());
		ceph_assert(*begin() == nullptr);
		ceph_assert(it.at_end());
		ceph_assert(*it == nullptr);
	}
	std::cout << 8 << std::endl;

	{
		// bulk fill in batches of 10000 entries
		size_t base = 100000;
		size_t max_entries = base + 500000;
		size_t entries_per_tr = 1000;
		if (was_empty) {
			for (size_t i = base; i < max_entries;
			     i += entries_per_tr) {
				if ((i % 100000) == 0) {
					std::cout << "inserting " << i
						  << std::endl;
				}
				batch batch;

				for (size_t j = 0; j < entries_per_tr; j++) {
					batch.set(std::move(stringify(i + j)),
						  std::move(std::string(
							  j + 1,
							  (i / entries_per_tr) &
								  0xff)));
				}

				apply_batch(pool, batch);
			}
		}

		auto p = get(fake_key);
		ceph_assert(p.get() == nullptr);

		std::string end_key = stringify(max_entries);
		p = get(end_key);
		ceph_assert(p.get() == nullptr);

		std::string first_key("100000");
		p = get(first_key);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p == first());
		ceph_assert(p->key() == first_key);
		ceph_assert(p->value() == std::string(1, 100));

		std::string second_key("100001");
		p = get(second_key);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p->key() == second_key);
		ceph_assert(p->value() == std::string(2, 100));

		{
			// enumerating for
			size_t offset = 200000;
			auto it = lower_bound(string_to_view("200000"));
			ceph_assert(!it.at_end());
			size_t sz = offset;
			while (it->key_view() < string_to_view("300000")) {
				auto p = *it;
				++it;
				if (p->key_view().length() < 6)
					continue;
				ceph_assert(
					p->value_view() ==
					string_to_view(std::string(
						1 + (sz % entries_per_tr),
						(sz / entries_per_tr) & 0xff)));
				++sz;
			}
			ceph_assert(sz - offset == 100000);
		}
	}

	if (remove) {
		{
			// remove by prefix
			batch bat;
			std::cout << "removing by prefix '3'" << std::endl;
			bat.remove_by_prefix(string("3"));
			auto size0 = size();
			apply_batch(pool, bat);
			std::cout << "removed " << size0 - size()
				  << std::endl;
			ceph_assert((size0 - size()) == 100000);
		}
		{
			// remove range
			batch bat;
			std::cout << "removing range 450000-499999999"
				  << std::endl;
			bat.remove_range(
				string("450000"),
				string("499999999")); // end is intentionally
						      // larger
			auto size0 = size();
			apply_batch(pool, bat);
			std::cout << "removed " << size0 - size() << std::endl;
			ceph_assert((size0 - size()) == 50000);
		}
		{
			// remove range
			batch bat;
			std::cout << "removing range 410000-442999"
				  << std::endl;
			bat.remove_range(
				string("410000"),
				string("443000"));
			auto size0 = size();
			apply_batch(pool, bat);
			std::cout << "removed " << size0 - size() << std::endl;
			ceph_assert((size0 - size()) == 33000);
		}

                size_t size0 = size();
		iterator it = end();
                --it;
		ceph_assert(it->key() == "599999");
		size_t i = 0;
                // reverse enumeration
		while (i < 50000) {
			size_t entries_per_tr = 1000;
			batch batch;
			if ((i % 10000) == 0) {
				std::cout << "removing " << i << " \""
					  << it->key() << "\"" << std::endl;
			}
			for (size_t j = 0; j < entries_per_tr; j++) {
				batch.remove((*it)->key_view());
				--it;
				++i;
			}

			apply_batch(pool, batch);
		}
		std::cout << "removed " << size0 - size() << std::endl;
		ceph_assert((size0 - size()) == 50000);
                i = 0;
		it = begin();
		while (!it.at_end()) {
			size_t entries_per_tr = 1000;
			batch batch;
			if ((i % 100000) == 0) {
				std::cout << "removing " << i << " \""
					  << it->key() << "\"" << std::endl;
			}
			for (size_t j = 0; j < entries_per_tr; j++) {
				batch.remove((*it)->key_view());
				++it;
				++i;
			}

			apply_batch(pool, batch);
		}
		std::cout << "removed " << i << std::endl;
		ceph_assert(size() == 0);
		ceph_assert(begin().at_end());
	}
}
