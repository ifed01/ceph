#pragma once

#include <libpmemobj++/pool.hpp>
#include "kv/KeyValueDB.h"
#include "pmemkv_types.h"
#include "pmemkv.h"

enum { l_pmemkv_first = 754300,
       l_pmemkv_get_latency,
       l_pmemkv_precommit_latency,
       l_pmemkv_commit_ops,
       l_pmemkv_commit_latency,
       l_pmemkv_submit_latency,

       l_pmemkv_submit_batch_wait_latency,
       l_pmemkv_submit_batch_exec_latency,

       l_pmemkv_commit_batch_latency,

       l_pmemkv_submit_set_preexec_latency,
       l_pmemkv_submit_set_preexec_lookup_latency,
       l_pmemkv_submit_set_preexec_existed0_latency,
       l_pmemkv_submit_set_preexec_existed1_latency,
       l_pmemkv_submit_set_preexec_existed2_latency,
       l_pmemkv_submit_set_preexec_insert_latency,

       /*l_pmemkv_submit_set_lookup_latency,
       l_pmemkv_submit_set_exec_latency,
       l_pmemkv_submit_set_existed0_latency,
       l_pmemkv_submit_set_existed1_latency,
       l_pmemkv_submit_set_existed2_latency,
       l_pmemkv_submit_set_existed3_latency,
       l_pmemkv_submit_set_make_new_persistent_latency,
       l_pmemkv_submit_set_insert_latency,*/

       l_pmemkv_submit_remove_lookup_latency,
       l_pmemkv_submit_remove_exec_latency,
       l_pmemkv_submit_merge_lookup_latency,
       l_pmemkv_submit_merge_exec_latency,
       l_pmemkv_last,
};

class PMemKeyValueDB : public KeyValueDB, protected pmem_kv::DB
{
	CephContext *cct;
	PerfCounters *logger = nullptr;
	std::string path;
	ceph::mutex commit_lock = ceph::make_mutex("ceph_pmemkv::commit_lock");

        bool read_only = false;
        bool opened = false;

	std::map<std::string, std::shared_ptr<MergeOperator>> merge_ops;

        enum {
                MAX_BATCH = 128
        };
        std::array<batch, MAX_BATCH> batch_set;
        size_t cur_batch = 0;
        size_t ops_count = 0;

        const uint64_t pmem_pool_size = 65 * (uint64_t)1024 * 1024 * 1024;
        const uint64_t pmem_pool_usable_size = 64 * (uint64_t)1024 * 1024 * 1024;
        const uint64_t pmem_alloc_log_size = 512 * (uint64_t)1024 * 1024;

	void _log_latency(DB::BatchTimes idx,
			  const ceph::timespan &t);

	void _submit_transaction(batch &b);
	void _maybe_commit_transactions(bool force, batch &b);

protected:
	pmem_kv::volatile_buffer
	_handle_merge(const pmem_kv::volatile_buffer &key,
		      const pmem_kv::volatile_buffer &new_value,
		      const pmem_kv::buffer_view &orig_value) override;

public:
	pmem::obj::pool<pmem_kv::root> pool;

	static std::string make_key(const std::string &prefix,
				    const std::string &key);
	static std::string make_key(const std::string &prefix,
				    const char *key,
			            size_t keylen);
	static std::string make_last_key(const std::string &prefix);

	static void split_key(const pmem_kv::buffer_view &in, string *prefix,
			     string *key);

	static void split_key(const pmem_kv::volatile_buffer &in,
                             string *prefix,
			     string *key);
       
        PMemKeyValueDB(CephContext *c, const std::string &_path)
                : cct(c),
                  path(_path)/*,
                  allocator(cct,
                          pmem_pool_usable_size,
                          pmem_kv::PMEM_PAGE_SIZE,
                          0, //FIXME minor: cap max_mem
                          "pmem hybrid allocator")*/
	{
	}
	~PMemKeyValueDB()
	{
		close();
	}

        int init(std::string option_str = "") override;
	int open(std::ostream &out, const std::string &cfs = "") override;

	int
	create_and_open(std::ostream &out, const std::string &cfs = "") override;

	int
	open_read_only(std::ostream &out,
		       const std::string& cfs="") override;

	void
	close() override;

        class PMemKVTransactionImpl : public KeyValueDB::TransactionImpl {
		pmem_kv::DB::batch bat;

	public:
		PMemKVTransactionImpl(pmem_kv::DB &_kv,
                                      pmem::obj::pool_base &_pool)
                        : bat(_kv, _pool, true, false)
		{
		}
		pmem_kv::DB::batch&
		get_batch()
		{
		        return bat;
                }
		void set(const std::string &prefix, const std::string &k,
			 const ceph::bufferlist &bl) override
                {
			bat.set(std::move(PMemKeyValueDB::make_key(prefix, k)), bl);
		}
		void set(const std::string &prefix, const char *k, size_t keylen,
		    const ceph::bufferlist &bl) override
		{
			bat.set(std::move(PMemKeyValueDB::make_key(prefix, k, keylen)), bl);
		}
		void rmkey(const std::string &prefix, const std::string &k) override
		{
			bat.remove(std::move(PMemKeyValueDB::make_key(prefix, k)));
		}
		void rmkey(const std::string &prefix, const char *k,
			   size_t keylen) override
		{
			bat.remove(std::move(PMemKeyValueDB::make_key(prefix, k, keylen)));
		}

		void rm_single_key(const std::string &prefix,
				   const std::string &k) override
		{
			bat.remove(std::move(PMemKeyValueDB::make_key(prefix, k)));
		}

		void rmkeys_by_prefix(const std::string &prefix) override
		{
			bat.remove_by_prefix(prefix);
		}
		void rm_range_keys(const std::string &prefix,
				   const std::string &start,
				   const std::string &end) override
		{
			bat.remove_range(
                                std::move(PMemKeyValueDB::make_key(prefix, start)),
				std::move(PMemKeyValueDB::make_key(prefix, end)));
		}
		void merge(const std::string &prefix, const std::string &k,
			   const ceph::bufferlist &bl) override
		{
			bat.merge(std::move(PMemKeyValueDB::make_key(prefix, k)), bl);
		}
	};

	Transaction
	get_transaction() override
	{
		return std::make_shared<PMemKVTransactionImpl>(
                        *(pmem_kv::DB*)this, pool);
	}
	int submit_transaction(Transaction t) override;
	int submit_transaction_sync(Transaction t) override;

        /// Retrieve Keys
	int get(const std::string &prefix,	///< [in] Prefix/CF for key
	    const std::set<std::string> &keys, ///< [in] Key to retrieve
	    std::map<std::string, ceph::buffer::list>
		    *out ///< [out] Key value retrieved
	    ) override;
        int get(const std::string &prefix, ///< [in] prefix or CF name
	    const std::string &key,    ///< [in] key
	    ceph::buffer::list *value) override;
        using KeyValueDB::get;

        class WholeSpaceIteratorImpl
	    : public KeyValueDB::WholeSpaceIteratorImpl {
	protected:
               pmem_kv::DB::iterator dbiter;
	public:
	       explicit WholeSpaceIteratorImpl(pmem_kv::DB::iterator &&iter)
		    : dbiter(std::move(iter))
		{
		}

		int seek_to_first() override;
		int seek_to_first(const std::string &prefix) override;
		int seek_to_last() override;
		int seek_to_last(const std::string &prefix) override;
		int upper_bound(const std::string &prefix,
				const std::string &after) override;
		int lower_bound(const std::string &prefix,
				const std::string &to) override;
		bool valid() override;
		int next() override;
		int prev() override;
		std::string key() override;
		std::pair<std::string, std::string> raw_key() override;
		bool raw_key_is_prefixed(const std::string &prefix) override;
		ceph::bufferlist value() override;
		ceph::bufferptr value_as_ptr() override;
		int status() override;
		size_t key_size() override;
		size_t value_size() override;
	};

        WholeSpaceIterator
	get_wholespace_iterator(IteratorOpts opts = 0) override
	{
		return std::make_shared<WholeSpaceIteratorImpl>(std::move(begin()));
	}
	/*virtual Iterator
	get_iterator(const std::string &prefix)
	{
		return std::make_shared<PrefixIteratorImpl>(
			prefix, get_wholespace_iterator());
	}*/

        uint64_t
	get_estimated_size(std::map<std::string, uint64_t> &extra) override
	{
                //FIXME
	        return 0;
        }

  /// Setup one or more operators, this needs to be done BEFORE the DB is
	/// opened.
	int
	set_merge_operator(const std::string &prefix,
			   std::shared_ptr<MergeOperator> mop) override
	{
		ceph_assert(!opened);
		merge_ops.emplace(prefix, mop);
		return 0;
	}
};
