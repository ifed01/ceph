#include "common/debug.h"
#include "ceph_pmemkv.h"

#define dout_context cct
#define dout_subsys ceph_subsys_rocksdb
#undef dout_prefix
#define dout_prefix *_dout << "pmemdb: "


const char* pmem_pool_name = "pmemkv";
const uint64_t pmem_pool_size = 16 * (uint64_t)1024 * 1024 * 1024;


std::string
PMemKeyValueDB::make_key(const std::string &prefix, const std::string &key)
{
	std::string out = prefix;
	out.push_back(0);
	out.append(key);
	return out;
}
std::string
PMemKeyValueDB::make_key(const std::string &prefix,
                         const char *key,
			 size_t keylen)
{
	std::string out = prefix;
	out.push_back(0);
	out.append(key, keylen);
	return out;
}
std::string
PMemKeyValueDB::make_last_key(const std::string &prefix)
{
	std::string out = prefix;
	out.push_back(1);
	return out;
}

void
PMemKeyValueDB::split_key(const pmem_kv::buffer_view& in, string *prefix,
                          string *key)
{
	pmem_kv::volatile_buffer vb(in);
	vb.split(0, prefix, key);
}

void
PMemKeyValueDB::split_key(const pmem_kv::volatile_buffer& in, string *prefix,
                          string *key)
{
	in.split(0, prefix, key);
}

int
PMemKeyValueDB::init(std::string option_str)
{
	PerfCountersBuilder plb(cct, "pmemkv", l_pmemkv_first,
				l_pmemkv_last);
	plb.add_u64_counter(l_pmemkv_submit_ops, "ops", "Submitted ops");
	plb.add_time_avg(l_pmemkv_get_latency, "get_latency", "Get latency");
	plb.add_time_avg(l_pmemkv_submit_latency, "submit_latency",
			 "Submit Latency");
	plb.add_time_avg(l_pmemkv_submit_wait_latency, "submit_wait_latency",
			 "Submit Wait-On-Lock Latency");
	plb.add_time_avg(l_pmemkv_submit_start_latency, "submit_start_latency",
			 "Submit PMEM Transaction Start Latency");
	plb.add_time_avg(l_pmemkv_submit_complete_latency,
			 "submit_complete_latency",
			 "Submit PMEM Transaction Completion Latency");
	plb.add_time_avg(l_pmemkv_submit_set_lookup_latency, "submit_set_lookup_latency",
			 "Submit SetOp Lookup Latency");
	plb.add_time_avg(l_pmemkv_submit_set_exec_latency,
			 "submit_set_exec_latency",
			 "Submit SetOp Exec Latency");
	plb.add_time_avg(l_pmemkv_submit_set_existed0_latency,
			 "submit_set_existed0_latency",
			 "Submit SetSubOp Assign to Existed0 Latency");
	plb.add_time_avg(l_pmemkv_submit_set_existed1_latency,
			 "submit_set_existed1_latency",
			 "Submit SetSubOp Assign to Existed1 Latency");
	plb.add_time_avg(l_pmemkv_submit_set_existed2_latency,
			 "submit_set_existed2_latency",
			 "Submit SetSubOp Assign to Existed2 Latency");
	plb.add_time_avg(l_pmemkv_submit_set_existed3_latency,
			 "submit_set_existed3_latency",
			 "Submit SetSubOp Assign to Existed3 Latency");
	plb.add_time_avg(l_pmemkv_submit_set_make_new_persistent_latency,
			 "submit_set_make_new_persistent_latency",
			 "Submit SetSubOp Make New Persistent Latency");
	plb.add_time_avg(l_pmemkv_submit_set_insert_latency,
			 "submit_set_insert_latency",
			 "Submit SetSubOp Insert Latency");
	plb.add_time_avg(l_pmemkv_submit_remove_lookup_latency,
			 "submit_remove_lookup_latency",
			 "Submit RemoveOp Lookup Latency");
	plb.add_time_avg(l_pmemkv_submit_remove_exec_latency,
			 "submit_remove_exec_latency",
			 "Submit RemoveOp Exec Latency");
	plb.add_time_avg(l_pmemkv_submit_merge_lookup_latency,
			 "submit_merge_lookup_latency",
			 "Submit MergeOp Lookup Latency");
	plb.add_time_avg(l_pmemkv_submit_merge_exec_latency,
			 "submit_merge_exec_latency",
			 "Submit MergeOp Exec Latency");

	logger = plb.create_perf_counters();
	cct->get_perfcounters_collection()->add(logger);

	return 0;
}

int
PMemKeyValueDB::open(std::ostream &out, const std::string &cfs)
{
	try {
		pool = pmem::obj::pool<pmem_root>::open(path, pmem_pool_name);
		load_from_pool(pool);
		read_only = false;
		opened = true;
	} catch (...) {
		derr << __func__ << "failed to open pmem pool:" << path << dendl;
                return -1;
	}
	return 0;
}

int
PMemKeyValueDB::create_and_open(std::ostream &out, const std::string &cfs)
{
	try {
		::unlink(path.c_str());
		pool = pmem::obj::pool<pmem_root>::create(path,
                        pmem_pool_name,
                        pmem_pool_size,
                        S_IRWXU);
		read_only = false;
		opened = true;
	} catch (...) {
		derr << __func__ << "failed to create pmem pool:" << path
		     << dendl;
		return -1;
	}
	return 0;
}

int
PMemKeyValueDB::open_read_only(std::ostream &out, const std::string &cfs)
{
	try {
		pool = pmem::obj::pool<pmem_root>::open(path, pmem_pool_name);
		load_from_pool(pool);
		read_only = true;
                opened = true;
	} catch (...) {
		derr << __func__ << "failed to open(read-only) pmem pool:" << path
		     << dendl;
		return -1;
	}
	return 0;
}

void
PMemKeyValueDB::close()
{
	if (opened) {
		dout(0) << __func__
		        << " pmem pool closed:" << path << dendl;
		try {
			pool.close();
		} catch (const std::logic_error &e) {
			derr << e.what() << dendl;
		}
	}
	if (cct && logger) {
		cct->get_perfcounters_collection()->remove(logger);
		delete logger;
		logger = nullptr;
	}
	opened = read_only = false;
}

int
PMemKeyValueDB::submit_transaction(Transaction t)
{
	if (opened && !read_only) {
		utime_t start = ceph_clock_now();
		PMemKVTransactionImpl *_t =
			static_cast<PMemKVTransactionImpl *>(t.get());
		auto ops = _t->get_batch().get_ops_count();
		apply_batch(pool, _t->get_batch(),
                        [&](DB::ApplyBatchTimes idx,
                            const ceph::timespan& t) {
			        switch (idx) {
			        case ApplyBatchTimes::LOCK_TIME:
					logger->tinc(
						l_pmemkv_submit_wait_latency,
                                                t);
                                        break;
			        case ApplyBatchTimes::START_TIME:
					logger->tinc(
						l_pmemkv_submit_start_latency,
                                                t);
                                        break;
			        case ApplyBatchTimes::END_TIME:
					logger->tinc(
						l_pmemkv_submit_complete_latency,
                                                t);
                                        break;
			        case ApplyBatchTimes::SET_LOOKUP_TIME:
					logger->tinc(
						l_pmemkv_submit_set_lookup_latency,
                                                t);
                                        break;
			        case ApplyBatchTimes::SET_EXEC_TIME:
					logger->tinc(
						l_pmemkv_submit_set_exec_latency,
                                                t);
                                        break;
				case ApplyBatchTimes::SET_EXISTED0_TIME:
					logger->tinc(
						l_pmemkv_submit_set_existed0_latency,
							t);
					break;
				case ApplyBatchTimes::SET_EXISTED1_TIME:
					logger->tinc(
						l_pmemkv_submit_set_existed1_latency,
							t);
					break;
				case ApplyBatchTimes::SET_EXISTED2_TIME:
					logger->tinc(
						l_pmemkv_submit_set_existed2_latency,
							t);
					break;

				case ApplyBatchTimes::SET_EXISTED3_TIME:
					logger->tinc(
						l_pmemkv_submit_set_existed3_latency,
						t);
					break;
				case ApplyBatchTimes::SET_MAKE_NEW_PERSISTENT_TIME:
					logger->tinc(
						l_pmemkv_submit_set_make_new_persistent_latency,
						t);
					break;
				case ApplyBatchTimes::SET_INSERT_TIME:
					logger->tinc(
						l_pmemkv_submit_set_insert_latency,
						t);
					break;
			        case ApplyBatchTimes::REMOVE_LOOKUP_TIME:
					logger->tinc(
						l_pmemkv_submit_remove_lookup_latency,
                                                t);
                                        break;
			        case ApplyBatchTimes::REMOVE_EXEC_TIME:
					logger->tinc(
						l_pmemkv_submit_remove_exec_latency,
                                                t);
                                        break;
				case ApplyBatchTimes::MERGE_LOOKUP_TIME:
					logger->tinc(
						l_pmemkv_submit_merge_lookup_latency,
						t);
					break;
				case ApplyBatchTimes::MERGE_EXEC_TIME:
					logger->tinc(
						l_pmemkv_submit_merge_exec_latency,
						t);
					break;
                                default:
					ceph_assert(false);
                                        break;
			        }
                        });
		logger->inc(l_pmemkv_submit_ops, ops);
		logger->tinc(l_pmemkv_submit_latency, ceph_clock_now() - start);

		return 0;
	}
	ceph_assert(false);
	return -EPERM;
}

int
PMemKeyValueDB::get(const std::string &prefix, ///< [in] Prefix/CF for key
                    const std::set<std::string> &keys, ///< [in] Key to retrieve
                    std::map<std::string, ceph::buffer::list> *out ///< [out] Key value retrieved
                   )
{
    for (auto& key : keys) {
	    utime_t start = ceph_clock_now();
            string k = make_key(prefix, key);
            auto res_ptr = pmem_kv::DB::get(k);  // FIXME: we might need to copy data at this (or even inside DB::get) point since resulting pointer isn't guaranteed to exist forever
	    logger->tinc(l_pmemkv_get_latency, ceph_clock_now() - start);
	    if (res_ptr != 0) {
	            auto& vv = res_ptr[0].value_view();
                    auto insert_pair = out->emplace(key, bufferlist());
	            insert_pair.first->second.append(vv.c_str(), vv.length());
            } else {
	            return -ENOENT;
            }
    }
    return 0;
}
  
int PMemKeyValueDB::get(const std::string &prefix, ///< [in] prefix or CF name
	                const std::string &key,    ///< [in] key
		    ceph::buffer::list *value)
{
	utime_t start = ceph_clock_now();
	string k = make_key(prefix, key);
	auto res_ptr = pmem_kv::DB::get(
		k); // FIXME: we might need to copy data at this (or
		    // even inside DB::get) point since resulting
		    // pointer isn't guaranteed to exist forever
	logger->tinc(l_pmemkv_get_latency, ceph_clock_now() - start);

	if (res_ptr != 0) {
		auto &vv = res_ptr[0].value_view();
		value->append(vv.c_str(), vv.length());
	} else {
		return -ENOENT;
	}
	return 0;
}

pmem_kv::volatile_buffer
PMemKeyValueDB::_handle_merge(const pmem_kv::volatile_buffer &key,
			      const pmem_kv::volatile_buffer &new_value,
			      const pmem_kv::buffer_view &orig_value)
{
        string res_str;
        string prefix;
	string s;

	split_key(key, &prefix, nullptr);
	ceph_assert(!prefix.empty());
	auto it = merge_ops.find(prefix);
	ceph_assert(it != merge_ops.end());

        ceph_assert(new_value.length() != 0);
	const char *new_val_buf = new_value.try_get_continuous();
	if (!new_val_buf) {
		s.resize(new_value.length());
		new_value.copy_out(0, new_value.length(), &s.at(0));
		new_val_buf = &s.at(0);
	}

	if (orig_value.is_null()) {
		it->second->merge_nonexistent(new_val_buf,
                                              new_value.length(),
                                              &res_str);
	} else {
		it->second->merge(orig_value.c_str(),
				  orig_value.length(),
				  new_val_buf,
			          new_value.length(),
                                  &res_str);
	}
	return pmem_kv::volatile_buffer(std::move(res_str));
}


int PMemKeyValueDB:: WholeSpaceIteratorImpl::seek_to_first()
{
	dbiter = dbiter.get_kv().begin();
	return 0;
}

int
PMemKeyValueDB:: WholeSpaceIteratorImpl::seek_to_first(const std::string &prefix)
{
	pmem_kv::volatile_buffer k(pmem_kv::string_to_view(prefix));

        auto it = dbiter.get_kv().lower_bound(k);
	if (it.at_end() || !k.is_prefix_for((*it)[0].key_view())) {
		dbiter = dbiter.get_kv().end();
	} else {
                dbiter = it;
	}
        return 0;
}

int
PMemKeyValueDB:: WholeSpaceIteratorImpl::seek_to_last()
{
	dbiter = dbiter.get_kv().end();
	--dbiter;
	return 0;
}

int
PMemKeyValueDB:: WholeSpaceIteratorImpl::seek_to_last(const std::string &prefix)
{
	std::string last_key = make_last_key(prefix);
	pmem_kv::volatile_buffer k(pmem_kv::string_to_view(last_key));
	dbiter = dbiter.get_kv().find(k);
        --dbiter;
	return 0;
}

int
PMemKeyValueDB:: WholeSpaceIteratorImpl::upper_bound(const std::string &prefix,
					  const std::string &after)
{
	dbiter = std::move(
		dbiter.get_kv().upper_bound(std::move(make_key(prefix, after))));
	return 0;
}

int PMemKeyValueDB:: WholeSpaceIteratorImpl::lower_bound(const std::string &prefix,
					  const std::string &to)
{
        dbiter = std::move(
                dbiter.get_kv().lower_bound(std::move(make_key(prefix, to))));
	return 0;
}

bool PMemKeyValueDB::WholeSpaceIteratorImpl::valid()
{
	return dbiter.valid();
}

int
PMemKeyValueDB::WholeSpaceIteratorImpl::next()
{
        ++dbiter;
        return 0;
}

int
PMemKeyValueDB::WholeSpaceIteratorImpl::prev()
{
	--dbiter;
        return 0;
}

std::string 
PMemKeyValueDB::WholeSpaceIteratorImpl::key() 
{
	string key_tail;
	PMemKeyValueDB::split_key((*dbiter)[0].key_view(), nullptr, &key_tail);
        return key_tail;
}

std::pair<std::string, std::string>
PMemKeyValueDB::WholeSpaceIteratorImpl::raw_key()
{
	std::pair<std::string, std::string> res;
	split_key((*dbiter)[0].key_view(), &res.first, &res.second);
	ceph_assert(res.first.length() != 0);
	ceph_assert(res.second.length() != 0);
        return res;
}

bool PMemKeyValueDB::WholeSpaceIteratorImpl::raw_key_is_prefixed(const std::string &prefix)
{
	// Look for "prefix\0" right in key_view
	auto k = (*dbiter)[0].key_view();
	if ((k.length() > prefix.length()) &&
            (k.c_str()[prefix.length()] == '\0')) {
		return memcmp(k.c_str(), prefix.c_str(), prefix.length()) == 0;
	}
	return false;
}

ceph::bufferlist
PMemKeyValueDB::WholeSpaceIteratorImpl::value()
{
	auto v = (*dbiter)[0].value_view();
	ceph::bufferlist bl;
        bl.append(v.c_str(), v.length());
	return bl;
}

ceph::bufferptr
PMemKeyValueDB::WholeSpaceIteratorImpl::value_as_ptr() 
{
	auto v = (*dbiter)[0].value_view();
	return bufferptr(v.c_str(), v.length());
}

int
PMemKeyValueDB::WholeSpaceIteratorImpl::status()
{
        //FIXME minor: is this correct?
        return valid() ? 0 : 1;
}

size_t 
PMemKeyValueDB::WholeSpaceIteratorImpl::key_size()
{
	return (*dbiter)[0].key_view().length();
}

size_t
PMemKeyValueDB::WholeSpaceIteratorImpl::value_size()
{
	return (*dbiter)[0].value_view().length();
}
