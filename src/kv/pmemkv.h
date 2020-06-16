#pragma once
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>
#include <libpmemobj.h>
#include <algorithm>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <functional>
#include <boost/intrusive/set.hpp>
#include <boost/variant.hpp>
#include "common/ceph_mutex.h"
#include "include/buffer.h"
#include "include/ceph_assert.h"

const size_t PMEM_UNIT_SIZE = 0x100;

namespace pmem_kv
{
using byte = std::string::value_type;
using byte_array = byte[];
using byte_array_ptr = pmem::obj::persistent_ptr<byte_array>;

struct buffer_view;
std::ostream &operator<<(std::ostream &out, const buffer_view &sv);
const buffer_view string_to_view(const std::string &s);
const buffer_view string_to_view(const char *s);

class buffer_view {
public:
	buffer_view()
	{
	}
	buffer_view(const byte *_data, size_t _size) : data(_data), size(_size)
	{
	}
	buffer_view(const std::string &s) : data(s.c_str()), size(s.size())
	{
	}
	void
	append(const byte *_data, size_t _size)
	{
		// multiple appends aren't supported atm
		ceph_assert(data == nullptr);
		ceph_assert(size == 0);

		data = _data;
		size = _size;
	}
	void
	dump(std::ostream &out) const
	{
		out << "(" << (void *)data << "~" << size << ":";

		if (data != nullptr)
			out.write(data, size);
		out << ")";
	}

	size_t
	length() const
	{
		return size;
	}
	const byte *
	c_str() const
	{
		return data;
	}

	bool
	operator<(const buffer_view &other) const
	{
		if (data == other.data && size == other.size)
			return false;
		if (data == nullptr || size == 0)
			return true;
		if (other.data == nullptr || other.size == 0)
			return false;
		auto r = memcmp(data, other.data, std::min(size, other.size));
		return (r < 0 || (r == 0 && size < other.size));
	}
	bool
	operator<=(const buffer_view &other) const
	{
		if (data == other.data && size == other.size)
			return true;
		if (data == nullptr || size == 0)
			return true;
		if (other.data == nullptr || other.size == 0)
			return false;
		auto r = memcmp(data, other.data, std::min(size, other.size));
		return (r < 0 || (r == 0 && size <= other.size));
	}
	bool
	operator==(const buffer_view &other) const
	{
		if (data == other.data && size == other.size)
			return true;
		if (size != other.size || data == nullptr || size == 0 ||
		    other.data == nullptr || other.size == 0)
			return false;
		return 0 == memcmp(data, other.data, size);
	}
	bool
	operator!=(const buffer_view &other) const
	{
		return !(*this == other);
	}

        /*buffer_view &
	operator=(const buffer_view &other)
	{
                data = other.data;
                size = other.size;
                return *this;
	}*/

	std::string
	to_str() const
	{
		return std::string(data, size);
	}

	void
	copy_out(size_t o, size_t len, byte *dest) const
	{
		ceph_assert(data != nullptr);
		ceph_assert(o + len <= size);
		memcpy(dest, data + o, len);
	}

        bool
	is_null() const
	{
                return data == nullptr;
	}

private:
	const byte *data = nullptr;
	size_t size = 0;
};

using volatile_buffer_base =
	boost::variant<void *, buffer_view, bufferlist, std::string>;

class volatile_buffer : public volatile_buffer_base {
	enum { Null, BufferView, BufferList, String };

public:
	volatile_buffer() : volatile_buffer_base((void *)nullptr)
	{
	}
	volatile_buffer(const buffer_view &_bv) : volatile_buffer_base(_bv)
	{
	}
	volatile_buffer(const bufferlist &_bl) : volatile_buffer_base(_bl)
	{
	}
	volatile_buffer(bufferlist &&_bl) : volatile_buffer_base(std::move(_bl))
	{
	}
	volatile_buffer(const std::string &_s) : volatile_buffer_base(_s)
	{
	}
	volatile_buffer(std::string &&_s) : volatile_buffer_base(std::move(_s))
	{
	}
	volatile_buffer(const volatile_buffer &other)
	    : volatile_buffer_base(other)
	{
	}
	volatile_buffer(volatile_buffer &&other)
	    : volatile_buffer_base(std::move(other))
	{
	}

	bool
	is_null() const
	{
		return which() == Null;
	}
	size_t
	length() const
	{
		switch (which()) {
			case Null: {
				return 0;
			}
			case BufferView: {
				return boost::get<buffer_view>(*this).length();
			}
			case BufferList: {
				return boost::get<bufferlist>(*this).length();
			}
			case String: {
				return boost::get<std::string>(*this).size();
			}
			default:
				ceph_assert(false);
		}
		return 0;
	}
	bool
	operator<(const buffer_view &other) const
	{
		switch (which()) {
			case Null: {
				return boost::get<void *>(*this) != nullptr;
			}
			case BufferView: {
				return boost::get<buffer_view>(*this) < other;
			}
			case BufferList: {
				auto &bl = boost::get<bufferlist>(*this);
				auto it = bl.begin();
				size_t pos = 0;
				size_t l = std::min<size_t>(bl.length(),
							    other.length());

				while (pos < l) {
					auto p = it.get_current_ptr();
					int r = memcmp(p.c_str(),
						       other.c_str() + pos,
						       l - pos);
					if (r != 0) {
						return r < 0;
					}
					it += p.length();
					pos += p.length();
				}
				return bl.length() < other.length();
			}
			case String: {
				return string_to_view(boost::get<std::string>(
					       *this)) < other;
			}
			default:
				ceph_assert(false);
		}
		return false;
	}
        bool
	operator<=(const buffer_view &other) const
	{
		switch (which()) {
			case Null: {
				return boost::get<void *>(*this) == nullptr;
			}
			case BufferView: {
				return boost::get<buffer_view>(*this) <= other;
			}
			case BufferList: {
				auto &bl = boost::get<bufferlist>(*this);
				auto it = bl.begin();
				size_t pos = 0;
				size_t l = std::min<size_t>(bl.length(),
							    other.length());

				while (pos < l) {
					auto p = it.get_current_ptr();
					int r = memcmp(p.c_str(),
						       other.c_str() + pos,
						       l - pos);
					if (r != 0) {
						return r < 0;
					}
					it += p.length();
					pos += p.length();
				}
				return bl.length() <= other.length();
			}
			case String: {
				return string_to_view(boost::get<std::string>(
					       *this)) <= other;
			}
			default:
				ceph_assert(false);
		}
		return false;
	}
	bool
	operator>(const buffer_view &other) const
	{
		return !(*this<=other);
	}
	bool
	operator>=(const buffer_view &other) const
	{
		return !(*this < other);
	}

	void
	copy_out(size_t o, size_t len, byte *dest) const
	{
		switch (which()) {
			case Null: {
				ceph_assert(false);
				break;
			}
			case BufferView: {
				boost::get<buffer_view>(*this).copy_out(o, len,
									dest);
				break;
			}
			case BufferList: {
				auto &bl = boost::get<bufferlist>(*this);

				ceph_assert(o + len <= bl.length());
				auto it = bl.begin();
				it += o;
				it.copy(len, dest);
				break;
			}
			case String: {
				boost::get<std::string>(*this).copy(dest, len,
								    o);
				break;
			}
			default:
				ceph_assert(false);
		}
	}
	void
	split(char separator, std::string *prefix, std::string *tail) const
	{
		switch (which()) {
			case Null: {
				ceph_assert(false);
				break;
			}
			case BufferView: {
				auto &in = boost::get<buffer_view>(*this);

				size_t prefix_len = in.length();
				size_t tail_len = 0;
				char *separator = (char *)memchr(in.c_str(), 0,
								 prefix_len);
				if (separator != NULL) {
					prefix_len =
						size_t(separator - in.c_str());
					tail_len = size_t(in.length() -
							  prefix_len - 1);
				}

				if (prefix)
					*prefix = std::string(in.c_str(),
							      prefix_len);
				if (tail && tail_len)
					*tail = std::string(separator + 1, tail_len);
				break;
			}
			case BufferList: {
				auto &bl = boost::get<bufferlist>(*this);

				auto it = bl.begin();
                                size_t pos = 0;
				if (prefix) {
					prefix->clear();
					prefix->reserve(bl.length());
				}
				while (it != bl.end()) {
					if (*it == separator) {
						if (tail) {
							tail->resize(bl.length() - pos - 1);
							it.copy(tail->length(), &tail->at(0));
						}
						it = bl.end();
					} else {
						if (prefix)
							*prefix += *it;
						++it;
						++pos;
					}
                                }
				break;
			}
			case String: {
				auto& in = boost::get<std::string>(*this);

	                        size_t prefix_len = in.length();
                                size_t tail_len = 0;
				char *separator = (char *)memchr(in.c_str(), 0,
								 prefix_len);
				if (separator != NULL) {
				        prefix_len = size_t(separator - in.c_str());
					tail_len = size_t(in.length() - prefix_len - 1);
                                }

				if (prefix)
					*prefix =
						std::string(in.c_str(), prefix_len);
				if (tail && tail_len)
					*tail = std::string(separator + 1, tail_len);
				break;
			}
			default:
				ceph_assert(false);
		}
	}
	const char *
	try_get_continuous() const
	{
		switch (which()) {
			case BufferView: {
				return boost::get<pmem_kv::buffer_view>(*this).c_str();
			}
			case BufferList: {
				auto &bl = boost::get<bufferlist>(*this);
				if (bl.get_num_buffers() == 1) {
					return bl.front().c_str();
				}
                                return nullptr;
			}
			case String: {
				return boost::get<std::string>(*this).c_str();
			}
                        case Null:
			default:
				ceph_assert(false);
		}
                return nullptr;
        }

	bool
	is_prefix_for(const buffer_view &bv) const
	{
		switch (which()) {
			case Null: {
				return false;
			}
			case BufferView: {
				auto &self_bv = boost::get<buffer_view>(*this);
				auto l = self_bv.length();
				return l <= bv.length() ?
                                        memcmp(self_bv.c_str(), bv.c_str(), l) == 0 :
                                        false;
			}
			case BufferList: {
				auto &bl = boost::get<bufferlist>(*this);

				size_t l = bl.length();
				if (l > bv.length()) {
					return false;
				}

				auto it = bl.begin();
				size_t pos = 0;
				while (pos < l) {
					auto p = it.get_current_ptr();
					int r = memcmp(p.c_str(),
						       bv.c_str() + pos,
						       l - pos);
					if (r != 0) {
						return false;
					}
					it += p.length();
					pos += p.length();
				}
				return true;
			}
			case String: {
				auto &self_str = boost::get<std::string>(*this);
				auto l = self_str.length();
				return l <= bv.length()
					? memcmp(self_str.c_str(), bv.c_str(),
						 l) == 0
					: false;
			}
			default:
				ceph_assert(false);
		}
		return false;
	}
};

const buffer_view string_to_view(const std::string &s);
const buffer_view string_to_view(const char *s);

class pmem_kv_entry {
	// FIXME minor: merge into a single 64bit field to reduce logging
	// overhead
	pmem::obj::p<uint32_t> key_size = 0;
	pmem::obj::p<uint32_t> val_size = 0;

	byte_array_ptr extern_data = nullptr;

	enum { MAX_EMBED_SIZE = PMEM_UNIT_SIZE - sizeof(extern_data) -
		       sizeof(key_size) - sizeof(val_size) };
	pmem::obj::array<byte, MAX_EMBED_SIZE> embed_data;

	void
	dispose()
	{
		if (extern_data != nullptr) {
			pmemobj_tx_free(extern_data.raw());
			extern_data = nullptr;
		}
	}

	void
	alloc_and_assign_extern(const volatile_buffer &s)
	{
		ceph_assert(extern_data == nullptr);
		ceph_assert( s.length() != 0);
		/*
		 * We need to cache pmemobj_tx_alloc return value and only after
		 * that assign it to _data, because when pmemobj_tx_alloc fails,
		 * it aborts transaction.
		 */
		byte_array_ptr res = pmemobj_tx_alloc(
			s.length(), pmem::detail::type_num<byte>());
		if (res == nullptr) {
			if (errno == ENOMEM)
				throw pmem::transaction_out_of_memory(
					"Failed to allocate persistent memory object")
					.with_pmemobj_errormsg();
			else
				throw pmem::transaction_alloc_error(
					"Failed to allocate persistent memory object")
					.with_pmemobj_errormsg();
		}
		s.copy_out(0, s.length(), res.get());
		extern_data = res;
	}

	void
	alloc_and_assign_extern(const volatile_buffer &s1,
				const volatile_buffer &s2)
	{
		ceph_assert(extern_data == nullptr);
		ceph_assert(s1.length() != 0);
		ceph_assert(s2.length() != 0);
		/*
		 * We need to cache pmemobj_tx_alloc return value and only after
		 * that assign it to _data, because when pmemobj_tx_alloc fails,
		 * it aborts transaction.
		 */
		byte_array_ptr res =
			pmemobj_tx_alloc(s1.length() + s2.length(),
					 pmem::detail::type_num<byte>());
		if (res == nullptr) {
			if (errno == ENOMEM)
				throw pmem::transaction_out_of_memory(
					"Failed to allocate persistent memory object")
					.with_pmemobj_errormsg();
			else
				throw pmem::transaction_alloc_error(
					"Failed to allocate persistent memory object")
					.with_pmemobj_errormsg();
		}
		s1.copy_out(0, s1.length(), res.get());
		s2.copy_out(0, s2.length(), res.get() + s1.length());
		extern_data = res;
	}

public:
	pmem_kv_entry()
	{
	}
	pmem_kv_entry(const volatile_buffer &k, const volatile_buffer &v)
	{
		assign(k, v);
	}

	~pmem_kv_entry()
	{
		dispose();
	}
	void
	assign(const volatile_buffer &k, const volatile_buffer &v)
	{
		dispose();
		key_size = k.length();
		val_size = v.length();
		size_t k_v_size = key_size + val_size;
		if (k_v_size <= MAX_EMBED_SIZE) {
			auto slice = embed_data.range(0, k_v_size);
			k.copy_out(0, key_size, &slice.at(0));
			if (val_size) {
				v.copy_out(0, val_size, &slice.at(k.length()));
			}
		} else if (key_size <= MAX_EMBED_SIZE) {
			auto slice = embed_data.range(0, key_size);
			k.copy_out(0, key_size, &slice.at(0));
			alloc_and_assign_extern(v);
		} else if (val_size <= MAX_EMBED_SIZE) {
			if (val_size) {
				auto slice = embed_data.range(0, val_size);
				v.copy_out(0, val_size, &slice.at(0));
			}
			alloc_and_assign_extern(k);
		} else {
			alloc_and_assign_extern(k, v);
		}
	}
	void
	dump(std::ostream &out) const
	{
		out << '{';
		out << "k:" << key_view() << ", v:" << value_view();
		out << '}';
	}
	const buffer_view
	key_view() const
	{
		buffer_view res;
		size_t k_v_size = key_size + val_size;
		if (k_v_size <= MAX_EMBED_SIZE) {
			res.append(embed_data.cdata(), key_size);
		} else if (key_size <= MAX_EMBED_SIZE) {
			res.append(embed_data.cdata(), key_size);
		} else if (val_size <= MAX_EMBED_SIZE) {
			res.append(extern_data.get(), key_size);
		} else {
			res.append(extern_data.get(), key_size);
		}
		return res;
	}
	const buffer_view
	value_view() const
	{
		buffer_view res;
		size_t k_v_size = key_size + val_size;
		if (k_v_size <= MAX_EMBED_SIZE) {
			res.append(embed_data.cdata() + key_size, val_size);
		} else if (key_size <= MAX_EMBED_SIZE) {
			res.append(extern_data.get(), val_size);
		} else if (val_size <= MAX_EMBED_SIZE) {
			res.append(embed_data.cdata(), val_size);
		} else {
			res.append(extern_data.get() + key_size, val_size);
		}
		return res;
	}

	std::string
	key() const
	{
		return key_view().to_str();
	}
	std::string
	value() const
	{
		return value_view().to_str();
	}
	bool
	operator<(const pmem_kv_entry &other_kv) const
	{
		return key_view() < other_kv.key_view();
		// return key_view().cmp(other_kv.key_view()) < 0;
	}
};

std::ostream &operator<<(std::ostream &out, const pmem_kv_entry &e);

struct pmem_kv_dummy {
	pmem::obj::p<uint64_t> a = 0;
	uint64_t b = 0;
};

using pmem_kv_entry_ptr = pmem::obj::persistent_ptr<pmem_kv_entry>;

struct volatile_map_entry {
	pmem_kv_entry_ptr kv_pair;
	boost::intrusive::set_member_hook<> set_hook;
	volatile_map_entry(pmem_kv_entry_ptr _kv_pair) : kv_pair(_kv_pair)
	{
	}
	bool
	operator<(const volatile_map_entry &other_kv) const
	{
		return *kv_pair < *(other_kv.kv_pair);
	}
};

class DB {
	friend class iterator;
	typedef boost::intrusive::set<
		volatile_map_entry,
		boost::intrusive::member_hook<
			volatile_map_entry, boost::intrusive::set_member_hook<>,
			&volatile_map_entry::set_hook>>
		kv_set_t;
	kv_set_t kv_set;
	uint64_t rm_seq = 0;
	ceph::shared_mutex general_mutex =
		ceph::make_shared_mutex("DB");

	struct Dispose {
		void
		operator()(volatile_map_entry *e)
		{
			delete e;
		}
	};

	struct Compare {

		bool
		operator()(const buffer_view &s,
			   const volatile_map_entry &e) const
		{
			return s < e.kv_pair->key_view();
		}

		bool
		operator()(const volatile_map_entry &e,
			   const buffer_view &s) const
		{
			return e.kv_pair->key_view() < s;
		}
		bool
		operator()(const volatile_buffer &b,
			   const volatile_map_entry &e) const
		{
			return b < e.kv_pair->key_view();
		}

		bool
		operator()(const volatile_map_entry &e,
			   const volatile_buffer &b) const
		{
			// return e.kv_pair->key_view().cmp(string_to_view(s)) <
			// 0;
			return !(b <= e.kv_pair->key_view());
			// return cmp(e.kv_pair->key_view(), string_to_view(s));
		}
	};
protected:
        virtual volatile_buffer _handle_merge(
          const volatile_buffer& key,
          const volatile_buffer &new_value,
          const buffer_view& orig_value)
	{
                  // default implementation simply performs no merge
                  return new_value;
	}

public:
	class batch {
		friend class DB;
		enum Op {
                        SET,
                        REMOVE,
                        REMOVE_PREFIX,
                        REMOVE_RANGE,
                        MERGE
                };
		std::vector<std::tuple<Op, volatile_buffer, volatile_buffer>> ops;

	public:
		void
		set(const volatile_buffer &key, const volatile_buffer &val)
		{
			ops.emplace_back(SET, key, val);
		}
		void
		set(volatile_buffer &&key, volatile_buffer &&val)
		{
			ops.emplace_back(SET, key, val);
		}
		void
		set(volatile_buffer &&key, const volatile_buffer &val)
		{
			ops.emplace_back(SET, key, val);
		}
		void
		set(const volatile_buffer &key, volatile_buffer &&val)
		{
			ops.emplace_back(SET, key, val);
		}
		void
		remove(const volatile_buffer &key)
		{
			ops.emplace_back(REMOVE, key, volatile_buffer());
		}
		void
		remove(volatile_buffer &&key)
		{
			ops.emplace_back(REMOVE, key, volatile_buffer());
		}
		void
		remove_by_prefix(const volatile_buffer &key)
		{
			ops.emplace_back(REMOVE_PREFIX, key, volatile_buffer());
		}
		void
		remove_by_prefix(volatile_buffer &&key)
		{
			ops.emplace_back(REMOVE_PREFIX, key, volatile_buffer());
		}
		void
		remove_range(const volatile_buffer &key1,
			     const volatile_buffer &key2)
		{
			ops.emplace_back(REMOVE_RANGE, key1, key2);
		}
		void
		remove_range(volatile_buffer &&key1, volatile_buffer &&key2)
		{
			ops.emplace_back(REMOVE_RANGE, key1, key2);
		}
		void
		merge(const volatile_buffer &key, const volatile_buffer &val)
		{
			ops.emplace_back(MERGE, key, val);
		}
		void
		merge(volatile_buffer &&key, const volatile_buffer &val)
		{
			ops.emplace_back(MERGE, key, val);
		}
		void
		merge(const volatile_buffer &key, volatile_buffer &&val)
		{
			ops.emplace_back(MERGE, key, val);
		}
		void
		merge(volatile_buffer &&key, volatile_buffer &&val)
		{
			ops.emplace_back(MERGE, key, val);
		}
		void
		reset()
		{
			ops.clear();
		}
                size_t
		get_ops_count() const
		{
			return ops.size();
                }
	};
	class iterator {
		friend class DB;

		enum Mode { LOWER_BOUND, UPPER_BOUND, FIND };
		DB *kv = nullptr;
		std::string cur_key;
		using iterator_imp = DB::kv_set_t::const_iterator;
		iterator_imp it;
		uint64_t seq = 0;

                // ctors are supposed to be called from within DB only.
                // hence they're declared private and lack locking.
                //
		iterator(DB &_kv, bool last = false)
		    : kv(&_kv), seq(_kv.rm_seq)
		{
                        // FIXME minor: may be modify begin/end iterators in a way they don't use real iterator
                        // but are dereferenced when needed?
                        // no need to lock KV when init such iterators 
                        // in this case
			if (last) {
				it = kv->kv_set.end();
			} else {
				it = kv->kv_set.begin();
				if (it != kv->kv_set.end()) {
					cur_key = it->kv_pair->key();
				}
			}
		}
		iterator(DB &_kv, const volatile_buffer &key,
			 Mode m)
		    : kv(&_kv), seq(_kv.rm_seq)
		{
			switch (m) {
				case LOWER_BOUND:
					it = kv->kv_set.lower_bound(key,
								    Compare());
					break;
				case UPPER_BOUND:
					it = kv->kv_set.upper_bound(key,
								    Compare());
					break;
				case FIND:
					it = kv->kv_set.find(key, Compare());
					break;
				default:
					ceph_assert(false);
			}
			if (it != kv->kv_set.end()) {
				cur_key = it->kv_pair->key();
			}
		}
        public:
		iterator()
		{
		}
		iterator(iterator &&other)
                        : kv(other.kv), it(other.it), seq(other.seq)
		{
                        cur_key.swap(other.cur_key);
		}

                DB&
		get_kv() const
		{
			ceph_assert(kv);
                        return *kv;
		}
                bool
		valid()
		{
                        return kv != nullptr && !at_end();
		}

		iterator &
		operator=(const iterator &other)
		{
			if (&other != this) {
				kv = other.kv;
				seq = other.seq;
				it = other.it;
				cur_key = other.cur_key;
			}
			return *this;
		}
		iterator &
		operator=(iterator &&other)
		{
			if (&other != this) {
				kv = other.kv;
				seq = other.seq;
				it = other.it;
				cur_key.swap(other.cur_key);
			}
			return *this;
		}
		iterator &
		operator++()
		{
			std::shared_lock l(kv->general_mutex);
			if (it != kv->kv_set.end()) {
				if (seq == kv->rm_seq) {
					++it;
				} else {
					it = kv->kv_set.upper_bound(
						volatile_buffer(string_to_view(
							cur_key)),
						Compare());
					seq = kv->rm_seq;
				}
				if (it != kv->kv_set.end()) {
					cur_key = it->kv_pair->key();
				} else {
					cur_key.clear();
				}
			}
			return *this;
		}
		iterator &
		operator--()
		{
			std::shared_lock l(kv->general_mutex);
			if (seq != kv->rm_seq) {
			        it = kv->kv_set.lower_bound(
				        volatile_buffer(string_to_view(cur_key)),
				        Compare());
			        seq = kv->rm_seq;
		        }
			if (it != kv->kv_set.begin()) {
				--it;
			}
			if (it != kv->kv_set.end()) {
				cur_key = it->kv_pair->key();
			} else {
				cur_key.clear();
			}
			return *this;
		}
		const pmem_kv_entry_ptr operator->()
		{
			std::shared_lock l(kv->general_mutex);
			if (seq != kv->rm_seq && !cur_key.empty()) {
				it = kv->kv_set.lower_bound(
					volatile_buffer(
						string_to_view(cur_key)),
					Compare());
				seq = kv->rm_seq;
				if (it != kv->kv_set.end()) {
					cur_key = it->kv_pair->key();
				} else {
					cur_key.clear();
				}
			}
			return cur_key.empty() ? pmem_kv_entry_ptr()
					       : it->kv_pair;
		}
		const pmem_kv_entry_ptr operator*()
		{
			return operator->();
		}
		bool
		at_end()
		{
			if (!cur_key.empty()) {
			        std::shared_lock l(kv->general_mutex);
			        if (seq != kv->rm_seq) {
				        it = kv->kv_set.lower_bound(
					        volatile_buffer(
						        string_to_view(cur_key)),
					                Compare());
				        seq = kv->rm_seq;
				        if (it != kv->kv_set.end()) {
					        cur_key = it->kv_pair->key();
				        } else {
					        cur_key.clear();
				        }
			        }
                        }
			return cur_key.empty();
		}
	};

	DB()
	{
	}
	virtual ~DB()
	{
		kv_set.clear_and_dispose(Dispose());
	}

	void
	load_from_pool(pmem::obj::pool_base &pool)
	{
		ceph_assert(kv_set.empty());
		std::lock_guard l(general_mutex);
		size_t entries = 0;
		auto entry_type_num = pmem::detail::type_num<pmem_kv_entry>();
		auto o = POBJ_FIRST_TYPE_NUM(pool.handle(), entry_type_num);
		while (!OID_IS_NULL(o)) {
			++entries;
			if ((entries % 100000) == 0) {
				std::cout << "Loading " << entries << std::endl;
			}

			pmem_kv_entry_ptr kv_ptr(
				(pmem_kv_entry *)pmemobj_direct(
					o)); // FIXME minor: in fact we just
					     // need the ability to create
					     // persistent_ptr from PMEMoid not
					     // from void*
			kv_set.insert(*new volatile_map_entry(kv_ptr));

			o = POBJ_NEXT_TYPE_NUM(o);
		}
	}

	void
	insert(pmem::obj::pool_base &pool,
               const volatile_buffer &k,
	       const volatile_buffer &v)
	{
		ceph_assert(k.length() != 0);
		ceph_assert(!v.is_null());
		std::lock_guard l(general_mutex);
		pmem::obj::transaction::run(pool, [&] {
			auto it = kv_set.find(k, Compare());
			if (it == kv_set.end()) {
				auto kv_ptr = pmem::obj::make_persistent<
					pmem_kv_entry>(k, v);
				kv_set.insert(*new volatile_map_entry(kv_ptr));
			} else {
				it->kv_pair->assign(k, v);
			}
		});
	}
	void
	erase(pmem::obj::pool_base &pool, const volatile_buffer &k)
	{
		ceph_assert(k.length() != 0);
		std::lock_guard l(general_mutex);
		pmem::obj::transaction::run(pool, [&] {
			auto it = kv_set.find(k, Compare());
			if (it != kv_set.end()) {
				pmem::obj::delete_persistent<pmem_kv_entry>(
					it->kv_pair);
				kv_set.erase_and_dispose(it, Dispose());
			}
		});
	}

	void
	apply_batch(pmem::obj::pool_base &pool, const batch &b)
	{
		if (b.ops.empty())
			return;
		std::lock_guard l(general_mutex);
		pmem::obj::transaction::run(pool, [&] {
			for (auto &p : b.ops) {
				auto &key = std::get<1>(p);
				/*std::cerr << key << " " << std::get<0>(p)
					  << std::endl;*/
				switch (std::get<0>(p)) {
                                        case batch::SET:
                                        {
						auto it = kv_set.find(
							key, Compare());
						auto &val = std::get<2>(p);
						//std::cerr << val << std::endl;
						if (it == kv_set.end()) {
							auto v = pmem::obj::
								make_persistent<
									pmem_kv_entry>(
									key,
									val);
							kv_set.insert(
								*new volatile_map_entry(
									v));
						} else {
							it->kv_pair->assign(
								key, val);
						}
                                                break;
				        }
					case batch::REMOVE: 
                                        {
						auto it = kv_set.find(
							key, Compare());
						if (it != kv_set.end()) {
							pmem::obj::delete_persistent<
								pmem_kv_entry>(
								it->kv_pair);
							kv_set.erase_and_dispose(
								it, Dispose());
							++rm_seq;
						}
						break;
					}
					case batch::REMOVE_PREFIX:
                                        {
						auto it = kv_set.lower_bound(
							key, Compare());

                                                bool any_remove = false;
						while (it != kv_set.end() &&
                                                       key.is_prefix_for(it->kv_pair->key_view())) {
							pmem::obj::delete_persistent<
								pmem_kv_entry>(
								it->kv_pair);
						        it = kv_set.erase_and_dispose(
								        it, Dispose());
                                                        any_remove = true;
						}
						if (any_remove) {
						        ++rm_seq;
                                                }
						break;
					}
					case batch::REMOVE_RANGE:
                                        {
						auto &key_end = std::get<2>(p);
						auto it = kv_set.lower_bound(
							key, Compare());
						bool any_remove = false;
						while (it != kv_set.end() &&
                                                       key_end > it->kv_pair->key_view()) {
							pmem::obj::delete_persistent<
								pmem_kv_entry>(
								it->kv_pair);
							it = kv_set.erase_and_dispose(
								it, Dispose());
							any_remove = true;
						}
						if (any_remove) {
							++rm_seq;
						}
						break;
					}
					case batch::MERGE:
                                        {
						auto it = kv_set.find(
							key, Compare());
						auto &val = std::get<2>(p);
						bool present =
							it != kv_set.end();
                                                buffer_view orig_bv;
						if (present) {
							orig_bv = 
								it->kv_pair->value_view();
						}
						volatile_buffer new_v = _handle_merge(
                                                        key, val, orig_bv);

						if (!present) {
							auto v = pmem::obj::
								make_persistent<
									pmem_kv_entry>(
									key,
									new_v);
							kv_set.insert(
								*new volatile_map_entry(
									v));
						} else {
							it->kv_pair->assign(
								key, new_v);
						}
						break;
					}
				}
			}
		});
	}

	bool
	empty()
	{
		std::shared_lock l(general_mutex);
		return kv_set.empty();
	}
	const pmem_kv_entry_ptr &
	first()
	{
		std::shared_lock l(general_mutex);
		ceph_assert(!kv_set.empty());
		return kv_set.begin()->kv_pair;
	}
	const pmem_kv_entry_ptr &
	last()
	{
		std::shared_lock l(general_mutex);
		ceph_assert(!kv_set.empty());
		return (--kv_set.end())->kv_pair;
	}
	const pmem_kv_entry_ptr
	get(const volatile_buffer &key)
	{
		std::shared_lock l(general_mutex);
		pmem_kv_entry_ptr ret;
		auto it = kv_set.find(key, Compare());
		if (it != kv_set.end()) {
			ret = it->kv_pair;
		}
		return ret;
	}

	size_t
	size()
	{
		std::shared_lock l(general_mutex);
		return kv_set.size();
	}

	iterator
	begin()
	{
		std::shared_lock l(general_mutex);
		return iterator(*this, false);
	}
	iterator
	end()
	{
		std::shared_lock l(general_mutex);
		return iterator(*this, true);
	}
	iterator
	lower_bound(const volatile_buffer &key)
	{
		std::shared_lock l(general_mutex);
		return iterator(*this, key, iterator::LOWER_BOUND);
	}
	iterator
	upper_bound(const volatile_buffer &key)
	{
		std::shared_lock l(general_mutex);
		return iterator(*this, key, iterator::UPPER_BOUND);
	}
	iterator
	find(const volatile_buffer &key)
	{
		std::shared_lock l(general_mutex);
		return iterator(*this, key, iterator::FIND);
	}

	void test(pmem::obj::pool_base &pool, bool remove = true);

}; // class DB
}; // namespace pmem_kv
