#include <sstream>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/allocation_flag.hpp>
#include "include/stringify.h"
#include "pmemkv.h"

const pmem_kv::buffer_view
pmem_kv::string_to_view(const std::string &s)
{
	return pmem_kv::buffer_view(s.c_str(), s.size());
}

const pmem_kv::buffer_view
pmem_kv::string_to_view(const char *s)
{
	return pmem_kv::buffer_view(s, strlen(s));
}

std::ostream &
pmem_kv::operator<<(std::ostream &out, const pmem_kv::pmem_kv_entry2 &e)
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


pmem_kv::pmem_kv_entry2_ptr
pmem_kv::pmem_kv_entry2::allocate(const pmem_kv::volatile_buffer &k,
				  const pmem_kv::volatile_buffer &v)
{
	pmem_kv_entry2_ptr res;
	auto sz = k.length() + v.length();
	size_t num_pages =
		p2roundup(sz + HEADER_SIZE, PMEM_PAGE_SIZE) / PMEM_PAGE_SIZE;
	res = pmem::obj::make_persistent<pmem_kv_entry2[]>(num_pages);
	ceph_assert(res[0].key_size == 0);
	ceph_assert(res[0].val_size == 0);
	ceph_assert(res[0].allocated == 0);
	res[0].allocated = num_pages * PMEM_PAGE_SIZE - HEADER_SIZE;
	res[0].assign(k, v, false);
	return res;
}

pmem_kv::pmem_kv_entry2_ptr
pmem_kv::pmem_kv_entry2::allocate_atomic_volatile(pmem::obj::pool_base &pool,
					 const pmem_kv::volatile_buffer &k,
					 const pmem_kv::volatile_buffer &v)
{
	pmem_kv::pmem_kv_entry2_ptr res;
	auto sz = k.length() + v.length();
	size_t num_pages =
		p2roundup(sz + HEADER_SIZE, PMEM_PAGE_SIZE) / PMEM_PAGE_SIZE;

	pmem::obj::make_persistent_atomic<pmem_kv_entry2[]>(
		pool, res, num_pages,
		pmem::obj::allocation_flag_atomic(POBJ_XALLOC_ZERO));
	ceph_assert(res[0].key_size == 0);
	ceph_assert(res[0].val_size == 0);
	ceph_assert(res[0].allocated == 0);
        res[0].allocated = -num_pages * PMEM_PAGE_SIZE - HEADER_SIZE;
        res[0].assign(k, v, false);

        return res;
}

void
pmem_kv::pmem_kv_entry2::assign(const pmem_kv::volatile_buffer &k,
				const pmem_kv::volatile_buffer &v,
                                bool need_snapshot)
{
	key_size = k.length();
	val_size = v.length();
	size_t k_v_size = key_size + val_size;
	ceph_assert(get_allocated() >= k_v_size);
	if (need_snapshot) {
		pmemobj_tx_add_range_direct(data, k_v_size);
	}
	k.copy_out(0, key_size, data);
	if (val_size) {
		v.copy_out(0, val_size, data + key_size);
	}
}

/*bool
pmem_kv::pmem_kv_entry2::can_assign_value(
	const pmem_kv::volatile_buffer &v) const
{
	return get_allocated() >= key_size + v.length();
}

bool
pmem_kv::pmem_kv_entry2::try_assign_value(const pmem_kv::volatile_buffer &v)
{
	bool res = false;
	ceph_assert(key_size != 0);
	if (get_allocated() >= key_size + v.length()) {
		val_size = v.length();
		if (val_size) {
			pmemobj_tx_add_range_direct(data + key_size, val_size);
			v.copy_out(0, val_size, data + key_size);
		}
		res = true;
	}
	return res;
}*/

size_t
pmem_kv::volatile_buffer::get_hash() const
{
	switch (which()) {
		case Null: {
			return 0;
		}
		case BufferView: {
			return std::hash<buffer_view>{}(
				boost::get<buffer_view>(*this));
		}
		case BufferList: {
			size_t res = 0;
			auto &bl = boost::get<bufferlist>(*this);
			auto it = bl.buffers().begin();
			while (it != bl.buffers().end()) {
				std::string_view sv(it->c_str(), it->length());
				res += std::hash<std::string_view>{}(sv);
				++it;
			}
			return res;
		}
		case String: {
			return std::hash<std::string>{}(
				boost::get<std::string>(*this));
		}
		case PMemPages:
		case PMemKVEntry2:
		default:
			ceph_assert(false);
	}
	return 0;
}

/*#define LOG_TIME_START ;
#define LOG_TIME(idx) ;
*/

#define LOG_TIME_START_DEF \
        auto t0 = mono_clock::now();

#define LOG_TIME_START \
        t0 = mono_clock::now();
#define LOG_TIME(idx)                                                          \
	if (f)                                                                 \
		f(idx, mono_clock::now() - t0);

void
pmem_kv::DB::commit_batch_set(
	pmem::obj::pool_base &pool,
        batch *bp,
        size_t batch_count,
	std::function<void(BatchTimes, const ceph::timespan &)> f)
{
	pmem::obj::transaction::run(pool, [&] {
		for (size_t i = 0; i < batch_count; ++i) {
			_commit_batch(bp[i], f);
		}
	});
}

void
pmem_kv::DB::_commit_batch(batch &b,
	std::function<void(BatchTimes, const ceph::timespan &)> f)
{
	mono_clock::time_point t0;
        LOG_TIME_START;

	for (auto &p : b.ops) {
		auto &preproc_val = std::get<2>(p);
		switch (std::get<0>(p)) {
			case batch::SET:
                        {
				/*t0 = mono_clock::now();
				kv_set_t::insert_commit_data commit_data;
				auto ip = kv_set.insert_check(key, Compare(),
							      commit_data);
				LOG_TIME(SET_LOOKUP_TIME);
				t0 = mono_clock::now();
				auto &val = std::get<2>(p).vbuf;
				if (!ip.second) {
					int path = 0;
					if (!ip.first->kv_pair[0]
						     .try_assign_value(val)) {
						entry::release(
							ip.first->kv_pair);
						ip.first->kv_pair =
							entry::allocate(key,
									val);
						path = 1;
					}

					LOG_TIME(BatchTimes(
						SET_EXISTED0_TIME + path));
				} else {
					auto kv = entry::allocate(key, val);
					LOG_TIME(SET_MAKE_NEW_PERSISTENT_TIME);
					auto t0 = mono_clock::now();
					kv_set.insert_commit(
						*new volatile_map_entry(kv),
						commit_data);
					inc_op(key);
					LOG_TIME(SET_INSERT_TIME);
				}
				LOG_TIME(SET_EXEC_TIME);*/
				break;
			}
			case batch::SET_PREEXEC:
                        {
				if (preproc_val.entry_ptr) {
					preproc_val.entry_ptr->persist();
					preproc_val.entry_ptr = nullptr;
				}

                                preproc_val.kv_to_release.clear_and_dispose(Release());
				break;
			}
			case batch::REMOVE:
			case batch::REMOVE_PREFIX:
			case batch::REMOVE_RANGE:
                        {
				ceph_assert(preproc_val.entry_ptr == nullptr);
				preproc_val.kv_to_release
					.clear_and_dispose(Release());
				break;
			}
			case batch::MERGE:
                        {
				if (preproc_val.entry_ptr) {
					preproc_val.entry_ptr->persist();
					/*if (!preproc_val.vbuf.is_null()) {
					        bool r = preproc_val.entry_ptr
						        ->try_assign_value(preproc_val.vbuf);
					        ceph_assert(r);
                                        }*/
					preproc_val.entry_ptr = nullptr;
				}

				preproc_val.kv_to_release.clear_and_dispose(
					Release());
				break;
			}
		}
	}
	LOG_TIME(COMMIT_BATCH_TIME);
}

void
pmem_kv::DB::submit_batch(
	pmem::obj::pool_base &pool,
        batch &b,
        std::function<void(BatchTimes, const ceph::timespan &)> f)
{
	mono_clock::time_point t0;
        LOG_TIME_START;

	std::lock_guard l(general_mutex);
	LOG_TIME(SUBMIT_BATCH_WAIT_LOCK_TIME);

        LOG_TIME_START;
	for (auto &p : b.ops) {
		auto &key = std::get<1>(p);
		switch (std::get<0>(p)) {
			case batch::SET: {
				/*t0 = mono_clock::now();
				kv_set_t::insert_commit_data commit_data;
				auto ip = kv_set.insert_check(key, Compare(),
							      commit_data);
				LOG_TIME(SET_LOOKUP_TIME);
				t0 = mono_clock::now();
				auto &val = std::get<2>(p).vbuf;
				if (!ip.second) {
					int path = 0;
					if (!ip.first->kv_pair[0]
						     .try_assign_value(val)) {
						entry::release(
							ip.first->kv_pair);
						ip.first->kv_pair =
							entry::allocate(key,
									val);
						path = 1;
					}

					LOG_TIME(BatchTimes(SET_EXISTED0_TIME +
							    path));
				} else {
					auto kv = entry::allocate(key, val);
					LOG_TIME(SET_MAKE_NEW_PERSISTENT_TIME);
					auto t0 = mono_clock::now();
					kv_set.insert_commit(
						*new volatile_map_entry(kv),
						commit_data);
					inc_op(key);
					LOG_TIME(SET_INSERT_TIME);
				}
				LOG_TIME(SET_EXEC_TIME);*/
				break;
			}
			case batch::SET_PREEXEC: {
                                LOG_TIME_START_DEF;

				auto &preproc_val = std::get<2>(p);

				volatile_map_entry *entry_ptr =
					preproc_val.entry_ptr;
				ceph_assert(entry_ptr != nullptr);
				iterator_imp preexec_it =
					preproc_val.it
						.get_validate_iterator_impl(
							kv_set.end());
				if (preexec_it != kv_set.end()) {
					if (key ==
					    string_to_view(
						    preproc_val.it
							    .get_current_key())) {
						std::swap(entry_ptr->kv_pair,
							  preexec_it->kv_pair);
						preproc_val.entry_ptr =
							&(*preexec_it);
						preproc_val.release(entry_ptr);

						LOG_TIME(SET_PREEXEC_EXISTED1_TIME);
					} else {
						kv_set.insert(preexec_it,
							      *entry_ptr);
						LOG_TIME(SET_PREEXEC_EXISTED2_TIME);
					}
				} else {
                                        LOG_TIME_START_DEF;

					kv_set_t::insert_commit_data
						commit_data;
					auto ip = kv_set.insert_check(
						key, Compare(), commit_data);

					LOG_TIME(SET_PREEXEC_LOOKUP_TIME);
                                        LOG_TIME_START;

					if (!ip.second) {
						std::swap(ip.first->kv_pair,
							  entry_ptr->kv_pair);
						preproc_val.entry_ptr =
							&(*ip.first);
						preproc_val.release(entry_ptr);
						LOG_TIME(BatchTimes(
							SET_PREEXEC_EXISTED0_TIME));
					} else {
						/*LOG_TIME(
							SET_MAKE_NEW_PERSISTENT_TIME);*/
						kv_set.insert_commit(
							*entry_ptr,
							commit_data);
						inc_op(key);
						LOG_TIME(SET_PREEXEC_INSERT_TIME);
					}
				}
				LOG_TIME(SET_PREEXEC_TIME);
				break;
			}
			case batch::REMOVE: {
                                LOG_TIME_START_DEF;

				auto &preproc_val = std::get<2>(p);
				auto it = kv_set.find(key, Compare());
				LOG_TIME(REMOVE_LOOKUP_TIME);
                                LOG_TIME_START;

				if (it != kv_set.end()) {
					inc_op(key);
                                        auto& e = *it;
					it = kv_set.erase(it);
					preproc_val.release(&e);
				}
				LOG_TIME(REMOVE_EXEC_TIME);
				break;
			}
			case batch::REMOVE_PREFIX: {
                                LOG_TIME_START_DEF;

				auto it = kv_set.lower_bound(key, Compare());
				auto &preproc_val = std::get<2>(p);

				LOG_TIME(REMOVE_LOOKUP_TIME);
                                LOG_TIME_START;

				while (it != kv_set.end() &&
				       key.is_prefix_for(it->key_view())) {
				        inc_op(it->key_view());
					auto &e = *it;
					it = kv_set.erase(it);
					preproc_val.release(&e);
				}
				LOG_TIME(REMOVE_EXEC_TIME);
				break;
			}
			case batch::REMOVE_RANGE: {
                                LOG_TIME_START_DEF;

				auto &preproc_val = std::get<2>(p);
				auto &key_end = preproc_val.vbuf;
				auto it = kv_set.lower_bound(key, Compare());
				LOG_TIME(REMOVE_LOOKUP_TIME);
                                LOG_TIME_START;

				while (it != kv_set.end() &&
				       key_end > it->key_view()) {
				        inc_op(it->key_view());
					auto &e = *it;
					it = kv_set.erase(it);
					preproc_val.release(&e);
				}
				LOG_TIME(REMOVE_EXEC_TIME);
				break;
			}
			case batch::MERGE: {
                                LOG_TIME_START_DEF;

				kv_set_t::insert_commit_data commit_data;
				auto ip = kv_set.insert_check(key, Compare(),
							      commit_data);
				LOG_TIME(MERGE_LOOKUP_TIME);
                                LOG_TIME_START;

				auto &preproc_val = std::get<2>(p);
				ceph_assert(preproc_val.entry_ptr == nullptr);
				bool present = !ip.second;
				buffer_view orig_bv;
				if (present) {
					orig_bv = ip.first->value_view();
				}
				volatile_buffer new_v = _handle_merge(
					key, preproc_val.vbuf, orig_bv);

				//preproc_val.vbuf = volatile_buffer();
				if (!present) {
					auto v = entry::allocate_atomic_volatile(
						pool, key, new_v);
					preproc_val.entry_ptr =
						new volatile_map_entry(v);
					kv_set.insert_commit(
						*preproc_val.entry_ptr,
						commit_data);
					inc_op(key);
				} else {
					ceph_assert(preproc_val.entry_ptr ==
						    nullptr);
					auto new_kv_pair =
						entry::allocate_atomic_volatile(
						pool, key, new_v);

					preproc_val.release(
						new volatile_map_entry(
							ip.first->kv_pair));

                                        preproc_val.entry_ptr = &(*ip.first);
					ip.first->kv_pair = new_kv_pair;
				}
				/*} else if (!ip.first->can_assign_value(new_v)) {
					ceph_assert(preproc_val.entry_ptr ==
						    nullptr);
					auto new_kv_pair =
						entry::allocate_atomic_volatile(
						pool, key, new_v);

					preproc_val.release(
						new volatile_map_entry(
							ip.first->kv_pair));

                                        preproc_val.entry_ptr = &(*ip.first);
					ip.first->kv_pair = new_kv_pair;
				} else {
					std::swap(preproc_val.vbuf, new_v);
					preproc_val.entry_ptr = &(*ip.first);
				}*/
				LOG_TIME(MERGE_EXEC_TIME);

				break;
			}
		}
	}
	LOG_TIME(SUBMIT_BATCH_EXEC_TIME);
}

void
pmem_kv::DB::test(pmem::obj::pool_base &pool, bool remove)
{
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
		batch batch(*this, pool, true);
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

		batch.dispose();
		std::cout << 4.5 << std::endl;

		// read
		p = get(k);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p == first());
		ceph_assert(p[0].key_view() == k);
		ceph_assert(p[0].value_view() == v);

		ceph_assert(!begin().at_end());
		ceph_assert(first() == last());
		ceph_assert(*begin() == *find(k));
		ceph_assert(*begin() == *lower_bound(k));
		ceph_assert(upper_bound(k).at_end());
		ceph_assert((*upper_bound(k)).get() == nullptr);
		auto it = begin();
		ceph_assert(!begin().at_end());

		// remove

		std::cout << 4.9 << std::endl;
		batch.remove((*it)[0].key_view());
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
			batch.dispose();
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
			ceph_assert(p[0].key_view() == kk);
			ceph_assert(p[0].value_view().length() == GB + 3);
			// smoke content verification
			ceph_assert(p[0].value_view().c_str()[0] == '1');
			ceph_assert(p[0].value_view().c_str()[1] == 0);
			ceph_assert(p[0].value_view().c_str()[GB / 2] == 0);
			ceph_assert(p[0].value_view().c_str()[GB / 2 + 1] == '2');
			ceph_assert(p[0].value_view().c_str()[GB / 2 + 2] == 0);
			ceph_assert(p[0].value_view().c_str()[GB] == 0);
			ceph_assert(p[0].value_view().c_str()[GB + 1] == 0);
			ceph_assert(p[0].value_view().c_str()[GB + 2] == '3');

			erase(pool, kk);
		}
	}
	if (was_empty) {
		// basic ops on 3 keys
		batch batch(*this, pool, true);
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
		ceph_assert(p[0].key_view() == k3);
		ceph_assert(p[0].value_view() == v3);

		p = get(k2);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p == first());
		ceph_assert(p != last());
		ceph_assert(p[0].key_view() == k2);
		ceph_assert(p[0].value_view() == v2);

		p = get(k);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p != first());
		ceph_assert(p == last());
		ceph_assert(p[0].key_view() == k);
		ceph_assert(p[0].value_view() == v);

		// iterate
		auto it = begin();
		ceph_assert(!it.at_end());
		ceph_assert((*it)[0].key_view() == k2);
		++it;
		ceph_assert((*it)[0].key_view() == k3);
		++it;
		ceph_assert((*it)[0].key_view() == k);

		// remove
		it = begin();
		ceph_assert(!it.at_end());
		while (!it.at_end()) {
			batch.dispose();
			batch.remove((*it)[0].key_view());
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
			auto t0 = mono_clock::now();
			ceph::timespan times[BatchTimes::MAX_TIMES] = {
				ceph::make_timespan(0)};
			size_t counts[BatchTimes::MAX_TIMES] = {0};
			for (size_t i = base; i < max_entries;
			     i += entries_per_tr) {
				if ((i % 100000) == 0) {
					std::cout << "inserting " << i
						  << std::endl;
				}
				batch batch(*this, pool, true);

				for (size_t j = 0; j < entries_per_tr; j++) {
					batch.set(std::move(stringify(i + j)),
						  std::move(std::string(
							  j + 1,
							  (i / entries_per_tr) &
								  0xff)));
				}

				apply_batch(pool, batch, [&](DB::BatchTimes idx, const ceph::timespan& t) {
                                          times[idx] += t;
				          ++counts[idx];
                                });
			}
			std::cout << "bulk completed in "
				  << double((mono_clock::now() - t0).count()) / 1E9 << std::endl;
			for (size_t i = 0; i < BatchTimes::MAX_TIMES;
			     ++i) {
				std::cout << i << " :" << counts[i] << " in "
					  << double(times[i].count()) / 1E9 << std::endl;
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
		ceph_assert(p[0].key() == first_key);
		ceph_assert(p[0].value() == std::string(1, 100));

		std::string second_key("100001");
		p = get(second_key);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p[0].key() == second_key);
		ceph_assert(p[0].value() == std::string(2, 100));

		{
			// enumerating for
			size_t offset = 200000;
			auto it = lower_bound(string_to_view("200000"));
			ceph_assert(!it.at_end());
			size_t sz = offset;
			while ((*it)[0].key_view() < string_to_view("300000")) {
				auto p = *it;
				++it;
				if (p[0].key_view().length() < 6)
					continue;
				ceph_assert(
					p[0].value_view() ==
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
			batch bat(*this, pool, true);
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
			batch bat(*this, pool, true);
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
			batch bat(*this, pool, true);
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
		ceph_assert((*it)[0].key() == "599999");
		size_t i = 0;
                // reverse enumeration
		while (i < 50000) {
			size_t entries_per_tr = 1000;
			batch batch(*this, pool, true);
			if ((i % 10000) == 0) {
				std::cout << "removing " << i << " \""
					  << (*it)[0].key() << "\""
					  << std::endl;
			}
			for (size_t j = 0; j < entries_per_tr; j++) {
				batch.remove((*it)[0].key_view());
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
			batch batch(*this, pool, true);
			if ((i % 100000) == 0) {
				std::cout << "removing " << i << " \""
					  << (*it)[0].key() << "\""
					  << std::endl;
			}
			for (size_t j = 0; j < entries_per_tr; j++) {
				batch.remove((*it)[0].key_view());
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

void
pmem_kv::DB::test2(pmem::obj::pool_base &pool)
{
	bool was_empty = kv_set.empty();

	if (was_empty) {
                size_t num_pages = for_each<pmem_kv_entry2>(pool);
		ceph_assert(num_pages == 0);

                byte sample[PMEM_PAGE_SIZE * 3] = {0};
                pmem_kv_entry2_ptr res, res2;
		pmem::obj::make_persistent_atomic<pmem_kv_entry2[]>(
			pool, res, 3,
                        pmem::obj::allocation_flag_atomic(POBJ_XALLOC_ZERO));
		pmem::obj::make_persistent_atomic<pmem_kv_entry2[]>(
			pool, res2, 10);

                ceph_assert(memcmp(res.get(), sample, PMEM_PAGE_SIZE * 3) == 0);

		num_pages = for_each<pmem_kv_entry2>(pool);
		std::cout << " num pages = " << num_pages << std::endl;
		ceph_assert(num_pages != 0);

		for_each<pmem_kv_entry2>(pool,
			[&](pmemoid& o) {
				pmem_kv_entry2_ptr p = 
					(entry*) pmemobj_direct(o);
				pmem::obj::delete_persistent_atomic<pmem_kv_entry2[]>(
					p, 0);
			});
		num_pages = for_each<pmem_kv_entry2>(pool);
		ceph_assert(num_pages == 0);
	}
}


struct test3_data{
	pmem::obj::p<uint64_t> cnt = 0;
	pmem_kv::pmem_kv_entry2_ptr entry = nullptr;
};

void
pmem_kv::DB::test3(pmem::obj::pool_base &pool)
{
	pmem::obj::persistent_ptr <test3_data> p = nullptr;
	pmem::obj::make_persistent_atomic<test3_data>(
		pool, p,
		pmem::obj::allocation_flag_atomic(POBJ_XALLOC_ZERO));
	mono_clock::time_point t0 = mono_clock::now();
        size_t count = 5000000;
	for (size_t i = 0; i < count; ++i) {
		pmem::obj::transaction::run(pool, [&] {
			p->cnt++;
		});
	}

        auto t = double((mono_clock::now() - t0).count()) / 1E9;
        std::cout << "test3 completed in "
		  << t
                  << " iops " << count / t
		  << std::endl;
	pmem::obj::delete_persistent_atomic<test3_data>(p);
}

void
pmem_kv::DB::test3_1(pmem::obj::pool_base &pool)
{
	pmem::obj::persistent_ptr<test3_data> p = nullptr;
	pmem::obj::make_persistent_atomic<test3_data>(
		pool, p, pmem::obj::allocation_flag_atomic(POBJ_XALLOC_ZERO));
	mono_clock::time_point t0 = mono_clock::now();
	size_t count = 5000000;
	const size_t sample_size =
		PMEM_PAGE_SIZE * 1;
	unsigned char sample[sample_size] = {0xff};
	for (size_t i = 0; i < count; ++i) {
		pmem::obj::transaction::run(pool, [&] {
			if (p->entry != nullptr) {
				pmem::obj::delete_persistent<pmem_kv_entry2[]>(
					p->entry, 1);
                        }
			p->entry = pmem::obj::make_persistent<
					pmem_kv_entry2[]>(sample_size / PMEM_PAGE_SIZE);
			pmem_memcpy_nodrain(p->entry.get(), sample,
					    sample_size);
		});
	}

        auto t = double((mono_clock::now() - t0).count()) / 1E9;
	std::cout << "test3_1 completed in "
		  << t
                  << " iops " << count / t
		  << std::endl;
	pmem::obj::delete_persistent_atomic<test3_data>(p);
}

void
pmem_kv::DB::test3_2(pmem::obj::pool_base &pool)
{
	pmem::obj::persistent_ptr<test3_data> p = nullptr;
	pmem::obj::make_persistent_atomic<test3_data>(
		pool, p, pmem::obj::allocation_flag_atomic(POBJ_XALLOC_ZERO));
	mono_clock::time_point t0 = mono_clock::now();
	size_t count = 5000000;
	const size_t sample_size = PMEM_PAGE_SIZE * 1;
	unsigned char sample[sample_size] = {0xff};
	for (size_t i = 0; i < count; ++i) {
		if (p->entry != nullptr) {
			pmem::obj::delete_persistent_atomic<pmem_kv_entry2[]>(
				p->entry, 1);
		}
		pmem::obj::make_persistent_atomic<pmem_kv_entry2[]>(
			pool, p->entry, sample_size / PMEM_PAGE_SIZE);

		pmem::obj::transaction::run(pool, [&] {
		        pmem_memcpy_nodrain( (void*)p->entry.get(), sample,
				            sample_size);
		});
	}

        auto t = double((mono_clock::now() - t0).count()) / 1E9;
	std::cout << "test3_2 completed in "
		  << t
                  << " iops " << count / t
		  << std::endl;
	pmem::obj::delete_persistent_atomic<test3_data>(p);
}

std::atomic_int running = 0;
#include "common/Thread.h"
#include <initializer_list>
struct TestThread : public Thread {
      pmem::obj::pool_base &pool;
      size_t count;
      TestThread(pmem::obj::pool_base &_pool, size_t _count)
	  : pool(_pool), count(_count)
      {
      }
      /*TestThread(std::initializer_list<TestThread> l) : pool(l.pool), count(l.count)
      {
      }*/
      void *
      entry() override
      {
	      pmem::obj::persistent_ptr<test3_data> p = nullptr;
	      pmem::obj::make_persistent_atomic<test3_data>(
		      pool, p,
		      pmem::obj::allocation_flag_atomic(POBJ_XALLOC_ZERO));
	      const size_t sample_size = PMEM_PAGE_SIZE;
	      unsigned char sample[sample_size] = {0xff};
              size_t conflicts = 0;
	      for (size_t i = 0; i < count; ++i) {
		      pmem::obj::transaction::run(pool, [&] {
			      if (++running > 1) {
                                        ++conflicts;
			      }
			      if (p->entry != nullptr) {
				      pmem::obj::delete_persistent<
					      pmem_kv::pmem_kv_entry2[]>(p->entry, 1);
			      }
			      p->entry = pmem::obj::make_persistent<
				      pmem_kv::pmem_kv_entry2[]>(
				              sample_size / PMEM_PAGE_SIZE);
			      pmem_memcpy_nodrain(p->entry.get(), sample,
						  sample_size);
                              --running;
		      });
	      }
	      std::cout << "conflicts detected " << conflicts << std::endl;
	      pmem::obj::delete_persistent_atomic<test3_data>(p);
	      return NULL;
      }
      /*void *
      entry() override
      {
	      pmem::obj::persistent_ptr<test3_data> p = nullptr;
	      pmem::obj::make_persistent_atomic<test3_data>(
		      pool, p,
		      pmem::obj::allocation_flag_atomic(POBJ_XALLOC_ZERO));
	      const size_t sample_size = PMEM_PAGE_SIZE * 1;
	      unsigned char sample[sample_size] = {0xff};
	      for (size_t i = 0; i < count; ++i) {
		      if (p->entry != nullptr) {
			      pmem::obj::delete_persistent_atomic<
				      pmem_kv::pmem_kv_entry2[]>(p->entry, 1);
		      }
		      pmem::obj::make_persistent_atomic<
			      pmem_kv::pmem_kv_entry2[]>(
			      pool, p->entry, sample_size / PMEM_PAGE_SIZE);

		      pmem::obj::transaction::run(pool, [&] {
			      pmem_memcpy_nodrain((void *)p->entry.get(),
						  sample, sample_size);
		      });
	      }

	      pmem::obj::delete_persistent_atomic<test3_data>(p);
	      return NULL;
      }*/
};

void
pmem_kv::DB::test3_3(pmem::obj::pool_base &pool)
{
        const size_t thread_count = 8;
        const size_t ops_count = 5000000;
	TestThread ths[thread_count] = { 
                TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
                TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		/*TestThread(pool, ops_count),
		TestThread(pool, ops_count),
                TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),*/
                /*TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),*/

                /*TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
                TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
                TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
                TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),*/

                /*TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
                TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count),
		TestThread(pool, ops_count)*/
        };

        mono_clock::time_point t0 = mono_clock::now();
	for (size_t i = 0; i < thread_count; ++i) {
		ths[i].create("thread");
	}
	for (size_t i = 0; i < thread_count; ++i) {
		ths[i].join();
	}
	auto t = double((mono_clock::now() - t0).count()) / 1E9;
	std::cout << "test3_3 completed in "
		  << t
                  << " iops " << thread_count * ops_count / t
		  << std::endl;
}
