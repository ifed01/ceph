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
pmem_kv::operator<<(std::ostream &out, const pmem_kv::pmem_kv_entry3 &e)
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


pmem_kv::pmem_kv_entry3_ptr
pmem_kv::pmem_kv_entry3::allocate(PMemAllocator &alloc,
			    const volatile_buffer &k,
			    const volatile_buffer &v,
                            bool log_alloc)
{
	auto sz = k.length() + v.length();
	size_t to_alloc = p2roundup(sz + HEADER_SIZE, PMEM_PAGE_SIZE);
	auto p = alloc.allocate(to_alloc, log_alloc);
	ceph_assert(p != nullptr);
	pmem_kv_entry3_ptr res(reinterpret_cast<pmem_kv_entry3*>(p));
	res[0].allocated = to_alloc;
	res[0].assign(k, v);
	return res;
}

void
pmem_kv::pmem_kv_entry3::assign(const pmem_kv::volatile_buffer &k,
				const pmem_kv::volatile_buffer &v)
{
	key_size = k.length();
	val_size = v.length();
	int64_t k_v_size = key_size + val_size;
	ceph_assert(get_available() >= k_v_size);
	k.copy_out(0, key_size, data);
	if (val_size) {
		v.copy_out(0, val_size, data + key_size);
	}
}

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
		case PMemKVEntry3:
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
	pmem::obj::pool<pmem_kv::root> &pool,
        batch *bp,
        size_t batch_count,
	std::function<void(BatchTimes, const ceph::timespan &)> f)
{
	for (size_t i = 0; i < batch_count; ++i) {
		_commit_batch(bp[i], f);
	}
	flush();
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
			case batch::MERGE:
                        {
				if (preproc_val.entry_ptr != nullptr) {
                                        auto kv = preproc_val.entry_ptr->kv_pair;
					alloc.log_alloc(kv->as_byteptr(),
							kv->get_allocated());
				}
				preproc_val.entry_ptr = nullptr;
                                // fall through
			}
			case batch::REMOVE:
			case batch::REMOVE_PREFIX:
			case batch::REMOVE_RANGE:
                        {
				ceph_assert(preproc_val.entry_ptr == nullptr);
				preproc_val.kv_to_release
					.clear_and_dispose(Release(get_allocator()));
				break;
			}
                        default:
				ceph_assert(false);
		}
	}
	LOG_TIME(COMMIT_BATCH_TIME);
	b.ops.clear();
}

void
pmem_kv::DB::submit_batch(
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
						preproc_val.release(entry_ptr);
						// preserve existing entry
						// point to log_allocation on it
						preproc_val.entry_ptr =
							&(*preexec_it);

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
						preproc_val.release(entry_ptr);
						// preserve exiisting entry
                                                // point to log_allocation on it
                                                preproc_val.entry_ptr =
                                                        &(*ip.first);
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

				if (!present) {
					auto v = entry::allocate(
						get_allocator(), key, new_v,
                                                false);
					preproc_val.entry_ptr =
						new volatile_map_entry(v);
					kv_set.insert_commit(
						*preproc_val.entry_ptr,
						commit_data);
					inc_op(key);
				} else {
                                        //FIXME minor: excessive new volatile_map_entry here
					preproc_val.release(
						new volatile_map_entry(
							ip.first->kv_pair));
					ip.first->kv_pair = entry::allocate(
						get_allocator(), key, new_v,
                                                false);
					// preserve existing entry
					// point to log_allocation on it
					preproc_val.entry_ptr = &(*ip.first);
				}
				LOG_TIME(MERGE_EXEC_TIME);

				break;
			}
		}
	}
	LOG_TIME(SUBMIT_BATCH_EXEC_TIME);
}

void
pmem_kv::DB::test(pmem::obj::pool<pmem_kv::root> &pool, bool remove,
                  uint64_t base, uint64_t count)
{
	std::cout << "start1" << std::endl;
	create(pool, 1024 * 1024);
	std::cout << "created1" << std::endl;

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

		p = get(fake_key);
		ceph_assert(p.get() == nullptr);

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
		/*{
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

			erase(pool, kk, true);
		}*/
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
			batch.remove((*it)[0].key_view());
			apply_batch(pool, batch);
			++it;
		}

		ceph_assert(size() == 0);
		ceph_assert(begin().at_end());
		ceph_assert(*begin() == nullptr);
		ceph_assert(it.at_end());
		ceph_assert(*it == nullptr);
	}
	size_t entries_per_tr = 1000;
	std::cout << 8 << std::endl;
	{
		// bulk fill in batches of 10000 entries
		size_t max_entries = base + count;
		if (was_empty) {
			auto t0 = mono_clock::now();
			ceph::timespan times[BatchTimes::MAX_TIMES + 1] = {
				ceph::make_timespan(0)};
			size_t counts[BatchTimes::MAX_TIMES + 1] = {0};
			for (size_t i = base; i < max_entries;
			     i += entries_per_tr) {
				if ((i % 100000) == 0) {
					std::cout << "inserting " << i
						  << std::endl;
				}
				batch batch(*this, pool, true);

				for (size_t j = 0; j < entries_per_tr; j++) {
					auto t0 = mono_clock::now();
					batch.set(std::move(stringify(i + j)),
						  std::move(std::string(
							  j + 1,
							  (i / entries_per_tr) &
								  0xff)));
					times[MAX_TIMES] += mono_clock::now() - t0;
					++counts[MAX_TIMES];

				}

				apply_batch(pool, batch, [&](DB::BatchTimes idx, const ceph::timespan& t) {
                                          times[idx] += t;
				          ++counts[idx];
                                });
			}
			std::cout << "bulk completed in "
				  << double((mono_clock::now() - t0).count()) / 1E9 << std::endl;
			for (size_t i = 0; i <= BatchTimes::MAX_TIMES;
			     ++i) {
				std::cout << i << " :" << counts[i] << " in "
					  << double(times[i].count()) / 1E9 << std::endl;
			}
			std::cout << "Available =  " << get_free()
				  << std::endl;
		}
		auto p = get(fake_key);
		ceph_assert(p.get() == nullptr);

		std::string end_key = stringify(max_entries);
		p = get(end_key);
		ceph_assert(p.get() == nullptr);

		std::string first_key(stringify(base));
		p = get(first_key);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p == first());
		ceph_assert(p[0].key() == first_key);
		ceph_assert(p[0].value() == std::string(1,
                        (base / entries_per_tr) & 0xff));

		std::string second_key(stringify(base+1));
		p = get(second_key);
		ceph_assert(p.get() != nullptr);
		ceph_assert(p[0].key() == second_key);
		ceph_assert(p[0].value() == std::string(2,
                        (base / entries_per_tr) & 0xff));
		{
			// enumerating for
			size_t offset = base + 100000;
			auto it = lower_bound(string_to_view(stringify(offset)));
			ceph_assert(!it.at_end());
			size_t sz = offset;
			while ((*it)[0].key_view() < string_to_view(stringify(base+200000))) {
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
		auto base_str = stringify(base);
		{
			// remove by prefix
			batch bat(*this, pool, true);
			std::cout << "removing by prefix " << char(base_str.at(0)+ 2) << std::endl;
			bat.remove_by_prefix(string(1, char(base_str.at(0) + 2))); // 3 for base == 100000
			auto size0 = size();
			apply_batch(pool, bat);
			std::cout << "removed " << size0 - size()
				  << std::endl;
			ceph_assert((size0 - size()) == 100000);
		}
		{
			// remove range
			batch bat(*this, pool, true);
                        string start, end;
			start = string(1, char(base_str.at(0) + 3)) + string("50000");
			end = string(1, char(base_str.at(0) + 3)) + string("99999999");
			std::cout << "removing range "
                                  << start << "-" << end
				  << std::endl;
			bat.remove_range(
				start,
				end); // end is intentionally
						      // larger
			auto size0 = size();
			apply_batch(pool, bat);
			std::cout << "removed " << size0 - size() << std::endl;
			ceph_assert((size0 - size()) == 50000);
		}
		{
			// remove range
			batch bat(*this, pool, true);
			string start, end;
			start = string(1, char(base_str.at(0) + 3)) + string("10000");
			end = string(1, char(base_str.at(0) + 3)) + string("43000");
			std::cout << "removing range "
                                  << start << "-" << end
				  << std::endl;
			bat.remove_range(
				start,
				end);
			auto size0 = size();
			apply_batch(pool, bat);
			std::cout << "removed " << size0 - size() << std::endl;
			ceph_assert((size0 - size()) == 33000);
		}

                size_t size0 = size();
		iterator it = end();
                --it;
		string k = string(1, char(base_str.at(0) + 4)) +
			string("99999");
		ceph_assert((*it)[0].key() == k);
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
	shutdown();
}

void
pmem_kv::DB::test2(pmem::obj::pool<pmem_kv::root> &pool)
{
	std::cout << "start" << std::endl;
	create(pool, 1024 * 1024);
	std::cout << "created" << std::endl;

	pmem_kv::volatile_buffer k0 = string("fake non-present key");
	pmem_kv::volatile_buffer k1 = string("key1");
	pmem_kv::volatile_buffer v1 = string("value1");
	pmem_kv::volatile_buffer k2 = string("keykeykey2");
	pmem_kv::volatile_buffer v2 = string("valueuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu2");

        insert(pool, k1, v1, false);
	insert(pool, k2, v2, true);

        ceph_assert(size() == 2);

	auto p = get(k0);
	ceph_assert(p == nullptr);

	p = get(k1);
	ceph_assert(p != nullptr);
	ceph_assert(p == first());
	ceph_assert(k1 == p->key_view());
	ceph_assert(v1 == p->value_view());

	p = get(k2);
	ceph_assert(p != nullptr);
	ceph_assert(k2 == p->key_view());
	ceph_assert(v2 == p->value_view());

        shutdown();
	std::cout << "shut down" << std::endl;
	load_from_pool(pool);
	std::cout << "loaded " << size() << std::endl;

        ceph_assert(size() == 2);
	p = get(k0);
	ceph_assert(p == nullptr);

	p = get(k1);
	ceph_assert(p != nullptr);
	ceph_assert(p == first());
	ceph_assert(k1 == p->key_view());
	ceph_assert(v1 == p->value_view());

	p = get(k2);
	ceph_assert(p != nullptr);
	ceph_assert(k2 == p->key_view());
	ceph_assert(v2 == p->value_view());
	shutdown();
}


struct test3_data{
	pmem::obj::p<uint64_t> cnt = 0;
	pmem_kv::pmem_kv_entry3_ptr entry = nullptr;
};

void
pmem_kv::DB::test3(pmem::obj::pool<pmem_kv::root> &pool)
{
	load_from_pool(pool);

	mono_clock::time_point t0 = mono_clock::now();
        size_t count = 3000000;
	string s0 = string(128, 'a');
	for (size_t i = 0; i < count;) {
		for (size_t j = 0; j < 1000; ++j) {
			pmem_kv::volatile_buffer k = stringify(i);
			pmem_kv::volatile_buffer v = s0 + stringify(i);
			insert(pool, k, v, j == 999);
                        ++i;
		}
	}

        auto t = double((mono_clock::now() - t0).count()) / 1E9;
        std::cout << "test3 insert completed in "
		  << t << " "
                  << count / t << " iops, "
		  << alloc.get_flushes() << " allocator flushes "
		  << get_free() << " available "
		  << std::endl;
	t0 = mono_clock::now();
	shutdown();
        load_from_pool(pool);
	t = double((mono_clock::now() - t0).count()) / 1E9;
	std::cout << "test3 load completed in " << t << " iops " << count / t
		  << " " << get_free() << " available "
		  << std::endl;
	//ceph_assert(size() == count + count0);
	t0 = mono_clock::now();
	for (size_t i = 0; i < count; ++i) {
		pmem_kv::volatile_buffer k = stringify(i);
		pmem_kv::volatile_buffer v = s0 + stringify(i);

		auto p = get(k);
		ceph_assert(p != nullptr);
		ceph_assert(k == p->key_view());
		ceph_assert(v == p->value_view());
	}
	t = double((mono_clock::now() - t0).count()) / 1E9;
	std::cout << "test3 verify completed in " << t << " iops " << count / t
		  << std::endl;
	shutdown();
}

void
pmem_kv::DB::test3_1(pmem::obj::pool<pmem_kv::root> &pool)
{
	/*pmem::obj::persistent_ptr<test3_data> p = nullptr;
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
	pmem::obj::delete_persistent_atomic<test3_data>(p);*/
}

void
pmem_kv::DB::test3_2(pmem::obj::pool<pmem_kv::root> &pool)
{
	/*pmem::obj::persistent_ptr<test3_data> p = nullptr;
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
	pmem::obj::delete_persistent_atomic<test3_data>(p);*/
}

/*std::atomic_int running = 0;
#include "common/Thread.h"
#include <initializer_list>
struct TestThread : public Thread {
      pmem::obj::pool<pmem_kv::root> &pool;
      size_t count;
      TestThread(pmem::obj::pool<pmem_kv::root> &_pool, size_t _count)
	  : pool(_pool), count(_count)
      {
      }
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
};*/

void
pmem_kv::DB::test3_3(pmem::obj::pool<pmem_kv::root> &pool)
{
/*        const size_t thread_count = 8;
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
		  << std::endl;*/
}
