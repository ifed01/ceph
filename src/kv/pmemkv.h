#pragma once
#include "common/ceph_mutex.h"
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
#include <libpmem.h>
#include <libpmemobj++/allocation_flag.hpp>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>
#include <libpmemobj.h>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include "pmemkv_types.h"
#include "pmemkv_alloc.h"

namespace pmem_kv
{
class buffer_view;
class volatile_buffer;
}

template <>
struct std::hash<pmem_kv::buffer_view>;
template <>
struct std::hash<pmem_kv::volatile_buffer>;

namespace pmem_kv
{
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
	const byte*
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

	std::string
	to_str() const
	{
		return std::string(data, size);
	}
	const std::string_view
	to_string_view() const
	{
		return std::string_view(data, size);
	}

	void
	copy_out(size_t o, size_t len, byte *dest) const
	{
		ceph_assert(data != nullptr);
		ceph_assert(o + len <= size);
		pmem_memcpy(dest, data + o, len,
			    PMEMOBJ_F_MEM_NOFLUSH | PMEMOBJ_F_MEM_NODRAIN);
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

class volatile_buffer;

class pmem_kv_entry3;
using pmem_kv_entry3_ptr = dummy_ptr<pmem_kv_entry3>;

class pmem_kv_entry3 {
	int64_t allocated = 0;
	uint32_t key_size = 0;
	uint32_t val_size = 0;

	enum { HEADER_SIZE =
		       sizeof(key_size) + sizeof(val_size) + sizeof(allocated),
	       MAX_PAGE0_SIZE = PMEM_PAGE_SIZE - HEADER_SIZE };
	byte data[MAX_PAGE0_SIZE];

public:
	static pmem_kv_entry3_ptr
	allocate(PMemAllocator &alloc,
		 const volatile_buffer &k,
		 const volatile_buffer &v,
                 bool log_alloc = true);
	inline static void release(PMemAllocator &alloc, pmem_kv_entry3_ptr ptr)
	{
		alloc.release(ptr->as_byteptr(), ptr->get_allocated(), true);
	}
	inline static void
	release_volatile(PMemAllocator &alloc, pmem_kv_entry3_ptr ptr)
	{
		alloc.release(ptr->as_byteptr(), ptr->get_allocated(), false);
	}

	int64_t
	get_allocated() const
	{
		return allocated;
	}
	int64_t
	get_available() const
	{
		return allocated > HEADER_SIZE ? allocated - HEADER_SIZE: 0;
	}
	const byte *as_byteptr() const {
		return reinterpret_cast<const byte*>(this);
        }

        void assign(const volatile_buffer &k, const volatile_buffer &v);

	void
	dump(std::ostream &out) const
	{
		out << '{';
		out << "k:" << key_view() << ", v:" << value_view();
		out << '}';
	}
	inline const buffer_view
	key_view() const
	{
		return buffer_view(data, key_size);
	}
	inline const buffer_view
	value_view() const
	{
		return buffer_view(data + key_size, val_size);
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
	inline bool
	operator<(const pmem_kv_entry3 &other_kv) const
	{
		return key_view() < other_kv.key_view();
	}
};
std::ostream &operator<<(std::ostream &out, const pmem_kv_entry3 &e);

using volatile_buffer_base =
	boost::variant<void *, buffer_view, bufferlist, std::string,
		       pmem_pages_t, pmem_kv_entry3_ptr>;

class volatile_buffer : public volatile_buffer_base {
	enum { Null, BufferView, BufferList, String, PMemPages, PMemKVEntry3 };

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
	volatile_buffer(const pmem_pages_t &_pp) : volatile_buffer_base(_pp)
	{
	}
	volatile_buffer(const pmem_kv_entry3_ptr &_pp)
	    : volatile_buffer_base(_pp)
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
	get_hash() const;

        size_t length() const
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
			case PMemPages: {
				return boost::get<pmem_pages_t>(*this).first;
			}
			case PMemKVEntry3: {
				return boost::get<pmem_kv_entry3_ptr>(*this)[0]
					.get_available();
			}
			default:
				ceph_assert(false);
		}
		return 0;
	}
	volatile_buffer &
	operator=(const volatile_buffer &other)
	{
		*reinterpret_cast<volatile_buffer_base*>(this) = other;
                return *this;
	}
	void
	swap(volatile_buffer& other)
	{
		reinterpret_cast<volatile_buffer_base *>(this)->swap(other);
	}

        /*byte&
	at(size_t pos) const
	{
		switch (which()) {
			case Null: {
                                ceph_assert(false);
				return 0;
			}
			case BufferView: {
                                auto &self_bv = boost::get<buffer_view>(*this);
                                return self_bv.at(pos);
			}
			case BufferList: {
				auto &bl = boost::get<bufferlist>(*this);
                                ceph_assert(false); //FIXME: to implement
                                return 0;
			}
			case String: {
				auto &self_str = boost::get<std::string>(*this);
                                return self_str.at(pos);
			}
			case PMemPages: // FIXME minor: to implement?
			case PMemKVEntry3:
			default:
				ceph_assert(false);
		}
		return false;
	} */

	bool
	operator<(const buffer_view &other) const
	{
		switch (which()) {
			case Null: {
				return other.c_str() != nullptr;
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
						       p.length());
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
			case PMemPages: // FIXME minor: to implement
			case PMemKVEntry3:
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
				return true;
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
						       p.length());
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
			case PMemPages: // FIXME minor: to implement
			case PMemKVEntry3:
			default:
				ceph_assert(false);
		}
		return false;
	}
	bool
	operator>(const buffer_view &other) const
	{
		return !(*this <= other);
	}
	bool
	operator>=(const buffer_view &other) const
	{
		return !(*this < other);
	}
	bool
	operator==(const buffer_view &other) const
	{
		switch (which()) {
			case Null: {
				return other.c_str() == nullptr;
			}
			case BufferView: {
				return boost::get<buffer_view>(*this) == other;
			}
			case BufferList: {
				auto &bl = boost::get<bufferlist>(*this);
				if (bl.length() != other.length()) {
					return false;
				}
				auto it = bl.begin();
				size_t pos = 0;
				while (pos < other.length()) {
					auto p = it.get_current_ptr();
					int r = memcmp(p.c_str(),
						       other.c_str() + pos,
						       p.length());
					if (r != 0) {
						return false;
					}
					it += p.length();
					pos += p.length();
				}
				return true;
			}
			case String: {
				return string_to_view(boost::get<std::string>(
					       *this)) == other;
			}
			case PMemPages: // FIXME minor: to implement
			case PMemKVEntry3:
			default:
				ceph_assert(false);
		}
		return false;
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

				auto it = bl.buffers().begin();
				size_t offs = 0;
				while (it != bl.buffers().end() && len) {
					if (offs + it->length() > o) {
						const char *src = it->c_str();
						src += o - offs;
						size_t l = it->length() -
							(o - offs);
						l = std::min(l, len);
						pmem_memcpy(
							dest, src, l,
							PMEMOBJ_F_MEM_NOFLUSH |
								PMEMOBJ_F_MEM_NODRAIN);
						len -= l;
						o += l;
						dest += l;
					}
					offs += it->length();
					++it;
				}
				break;
			}
			case String: {
				auto &s = boost::get<std::string>(*this);
				pmem_memcpy(dest, s.c_str() + o, len,
					    PMEMOBJ_F_MEM_NOFLUSH |
						    PMEMOBJ_F_MEM_NODRAIN);
				break;
			}
			case PMemPages: // FIXME minor: to implement
			case PMemKVEntry3:
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
					*tail = std::string(separator + 1,
							    tail_len);
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
							tail->resize(
								bl.length() -
								pos - 1);
							it.copy(tail->length(),
								&tail->at(0));
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
				auto &in = boost::get<std::string>(*this);

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
					*tail = std::string(separator + 1,
							    tail_len);
				break;
			}
			case PMemPages: // FIXME minor: to implement?
			case PMemKVEntry3:
			default:
				ceph_assert(false);
		}
	}
	const char*
	try_get_continuous() const
	{
		switch (which()) {
			case BufferView: {
				return boost::get<pmem_kv::buffer_view>(*this)
					.c_str();
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
			case PMemPages: // FIXME minor: to implement?
			case PMemKVEntry3:
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
				return l <= bv.length()
					? memcmp(self_bv.c_str(), bv.c_str(),
						 l) == 0
					: false;
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
			case PMemPages: // FIXME minor: to implement?
			case PMemKVEntry3:
			default:
				ceph_assert(false);
		}
		return false;
	}
};

template <class T>
struct volatile_map_entry_base
{
	T kv_pair;
        std::string ss;
	boost::intrusive::any_member_hook<> _hook;
	volatile_map_entry_base(T _kv_pair) 
                : kv_pair(_kv_pair) , ss(_kv_pair[0].key_view().to_str())
	{
	}
        inline const buffer_view
        key_view() const
        {
               return string_to_view(ss);
                //return kv_pair[0].key_view();
        }
	inline bool
	operator<(const volatile_map_entry_base<T> &other_kv) const
	{
		return ss < other_kv.ss;
		// return kv_pair[0] < other_kv.kv_pair[0];
	}

	const buffer_view
	value_view() const
	{
		return kv_pair[0].value_view();
	}

	/*inline void
	persist()
	{
		kv_pair[0].persist();
	}
	inline bool
        can_assign_value(const pmem_kv::volatile_buffer &v) const
	{
		return kv_pair[0].can_assign_value(v);
	}
	inline bool
        try_assign_value(const volatile_buffer &v)
	{
		return kv_pair[0].try_assign_value(v);
	}*/
};

class DB {
public:
	enum BatchTimes {
		/*LOCK_TIME,
		START_TIME,
		END_TIME,*/

		SUBMIT_BATCH_WAIT_LOCK_TIME,
		SUBMIT_BATCH_EXEC_TIME,

		COMMIT_BATCH_TIME,

		SET_PREEXEC_TIME,
		SET_PREEXEC_LOOKUP_TIME,
		SET_PREEXEC_EXISTED0_TIME,
		SET_PREEXEC_EXISTED1_TIME,
		SET_PREEXEC_EXISTED2_TIME,
		SET_PREEXEC_INSERT_TIME,
		REMOVE_LOOKUP_TIME,
		REMOVE_EXEC_TIME,
		MERGE_LOOKUP_TIME,
		MERGE_EXEC_TIME,

		/*SET_LOOKUP_TIME,
		SET_EXEC_TIME,
		SET_EXISTED0_TIME,
		SET_EXISTED1_TIME,
		SET_EXISTED2_TIME,
		SET_EXISTED3_TIME,
		SET_MAKE_NEW_PERSISTENT_TIME,
		SET_INSERT_TIME,*/

		MAX_TIMES
	};
	class batch;

private:
	friend class iterator;

	using entry = pmem_kv_entry3;
	using entry_ptr = pmem_kv_entry3_ptr;

	using volatile_map_entry = volatile_map_entry_base<entry_ptr>;

        typedef boost::intrusive::any_to_set_hook<
	        boost::intrusive::member_hook<
                            volatile_map_entry,
                            boost::intrusive::any_member_hook<>,
		            &volatile_map_entry::_hook>>
	        kv_set_t_option;

	typedef boost::intrusive::set<
		volatile_map_entry,
                kv_set_t_option> kv_set_t;
	kv_set_t kv_set;
	enum { BUCKET_COUNT = 16386 };
	uint64_t op_seq[BUCKET_COUNT] = {0};
        PMemAllocator alloc;

	ceph::shared_mutex general_mutex = ceph::make_shared_mutex("DB");

	struct DisposeVolatile {
		void
		operator()(volatile_map_entry *e)
		{
			delete e;
		}
	};

        // to be called within a transaction
	struct Release {
                Release(PMemAllocator &_alloc) : alloc(_alloc)
		{
		}
		void
		operator()(volatile_map_entry *e)
		{
			if (e) {
				if (e->kv_pair) {
					entry::release(alloc, e->kv_pair);
				}
				delete e;
			}
		}
        private:
		PMemAllocator &alloc;
	};

	struct ReleaseVolatile {
		ReleaseVolatile(PMemAllocator &_alloc) : alloc(_alloc)
		{
		}
		void
		operator()(volatile_map_entry *e)
		{
			if (e) {
				if (e->kv_pair) {
					entry::release_volatile(alloc,
                                                                e->kv_pair);
				}
				delete e;
			}
		}

	private:
		PMemAllocator &alloc;
	};

	struct Compare {

		bool
		operator()(const buffer_view &s,
			   const volatile_map_entry &e) const
		{
			return s < e.key_view();
		}

		bool
		operator()(const volatile_map_entry &e,
			   const buffer_view &s) const
		{
			return e.key_view() < s;
		}
		bool
		operator()(const volatile_buffer &b,
			   const volatile_map_entry &e) const
		{
			return b < e.key_view();
		}

		bool
		operator()(const volatile_map_entry &e,
			   const volatile_buffer &b) const
		{
			// return e.kv_pair->key_view().cmp(string_to_view(s)) <
			// 0;
			return !(b <= e.key_view());
			// return cmp(e.kv_pair->key_view(), string_to_view(s));
		}
	};

        template <class T>
	void
        inc_op(const T &bv)
	{
		auto h = std::hash<T>{}(bv);
		++op_seq[h % BUCKET_COUNT];
	}

	protected:
	virtual volatile_buffer
	_handle_merge(const volatile_buffer &key,
		      const volatile_buffer &new_value,
		      const buffer_view &orig_value)
	{
		// default implementation performs no merge
		return new_value;
	}

	using iterator_imp = DB::kv_set_t::iterator;

public:
	class iterator {
		friend class DB;

		enum Mode { LOWER_BOUND, UPPER_BOUND, EXACT_MATCH };

		DB *kv = nullptr;
		std::string cur_key;
		size_t cur_key_hash = 0;
		iterator_imp it;
		uint64_t last_op_seqno = 0;

		// ctors are supposed to be called from within DB only.
		// hence they're declared private and lack locking.
		//
		iterator(DB &_kv, bool last = false) : kv(&_kv)
		{
			// FIXME minor: may be modify begin/end iterators in a
			// way they don't use real iterator but are dereferenced
			// when needed? no need to lock KV when init such
			// iterators in this case
			if (last) {
				it = kv->kv_set.end();
			} else {
				it = kv->kv_set.begin();
				if (it != kv->kv_set.end()) {
					cur_key = it->kv_pair[0].key();
					cur_key_hash = std::hash<std::string>{}(
						cur_key);
					last_op_seqno =
						kv->get_last_op(cur_key_hash);
				}
			}
		}
		iterator(DB &_kv, const volatile_buffer &key, Mode m)
		    : kv(&_kv)
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
				case EXACT_MATCH:
					it = kv->kv_set.find(key, Compare());
					break;
				default:
					ceph_assert(false);
			}
			if (it != kv->kv_set.end()) {
				cur_key = it->kv_pair[0].key();
				cur_key_hash =
					std::hash<std::string>{}(cur_key);
				last_op_seqno = kv->get_last_op(cur_key_hash);
			}
		}

                iterator_imp
                get_validate_iterator_impl(iterator_imp invalid_val) const
		{
			if (!kv || cur_key.empty()) {
                                return invalid_val;
			}
			ceph_assert(ceph_mutex_is_locked(
				kv->general_mutex));
			if (last_op_seqno ==
			    kv->get_last_op(cur_key_hash)) {
			    return it;
                        }
			return invalid_val;
		}

	public:
		iterator()
		{
		}
		iterator(iterator &&other)
		    : kv(other.kv),
		      cur_key_hash(other.cur_key_hash),
		      it(other.it),
		      last_op_seqno(other.last_op_seqno)
		{
			cur_key.swap(other.cur_key);
		}

		DB &
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

                const std::string &
		get_current_key() const
		{
		        return cur_key;
                }

		iterator &
		operator=(const iterator &other)
		{
			if (&other != this) {
				kv = other.kv;
				last_op_seqno = other.last_op_seqno;
				it = other.it;
				cur_key = other.cur_key;
				cur_key_hash = other.cur_key_hash;
			}
			return *this;
		}
		iterator &
		operator=(iterator &&other)
		{
			if (&other != this) {
				kv = other.kv;
				last_op_seqno = other.last_op_seqno;
				it = other.it;
				cur_key.swap(other.cur_key);
				cur_key_hash = other.cur_key_hash;
			}
			return *this;
		}
		iterator &
		operator++()
		{
			std::shared_lock l(kv->general_mutex);
			if (it != kv->kv_set.end()) {
				if (last_op_seqno ==
				    kv->get_last_op(cur_key_hash)) {
					++it;
				} else {
					it = kv->kv_set.upper_bound(
						volatile_buffer(string_to_view(
							cur_key)),
						Compare());
				}
				if (it != kv->kv_set.end()) {
					cur_key = it->kv_pair[0].key();
					cur_key_hash = std::hash<std::string>{}(
						cur_key);
					last_op_seqno =
						kv->get_last_op(cur_key_hash);
				} else {
					cur_key.clear();
					cur_key_hash = 0;
					last_op_seqno = 0;
				}
			}
			return *this;
		}
		iterator &
		operator--()
		{
			std::shared_lock l(kv->general_mutex);
			bool key_changed = false;
			if (!cur_key.empty() &&
			    last_op_seqno != kv->get_last_op(cur_key_hash)) {
				it = kv->kv_set.lower_bound(
					volatile_buffer(
						string_to_view(cur_key)),
					Compare());
				key_changed = true;
			}
			if (it != kv->kv_set.begin()) {
				--it;
				key_changed = true;
			}
			if (it != kv->kv_set.end()) {
				if (key_changed) {
					cur_key = it->kv_pair[0].key();
					cur_key_hash = std::hash<std::string>{}(
						cur_key);
					last_op_seqno =
						kv->get_last_op(cur_key_hash);
				}
			} else {
				cur_key.clear();
				cur_key_hash = 0;
				last_op_seqno = 0;
			}
			return *this;
		}

                const entry_ptr operator->()
		{
			std::shared_lock l(kv->general_mutex);
			if (!cur_key.empty() &&
			    last_op_seqno != kv->get_last_op(cur_key_hash)) {
				it = kv->kv_set.lower_bound(
					volatile_buffer(
						string_to_view(cur_key)),
					Compare());
				if (it != kv->kv_set.end()) {
					cur_key = it->kv_pair[0].key();
					cur_key_hash = std::hash<std::string>{}(
						cur_key);
					last_op_seqno =
						kv->get_last_op(cur_key_hash);
				} else {
					cur_key.clear();
					cur_key_hash = 0;
					last_op_seqno = 0;
				}
			}
			return cur_key.empty() ? entry_ptr() : it->kv_pair;
		}
		const entry_ptr operator*()
		{
			return operator->();
		}
		bool
		at_end()
		{
			if (!cur_key.empty()) {
				std::shared_lock l(kv->general_mutex);
				if (last_op_seqno !=
				    kv->get_last_op(cur_key_hash)) {
					it = kv->kv_set.lower_bound(
						volatile_buffer(string_to_view(
							cur_key)),
						Compare());
					if (it != kv->kv_set.end()) {
						cur_key = it->kv_pair[0].key();
						cur_key_hash = std::hash<
							std::string>{}(cur_key);
						last_op_seqno = kv->get_last_op(
							cur_key_hash);
					} else {
						cur_key.clear();
						cur_key_hash = 0;
						last_op_seqno = 0;
					}
				}
			}
			return cur_key.empty();
		}
	};
	class batch {
		friend class DB;
		enum Op {
			SET,
			SET_PREEXEC,
			REMOVE,
			REMOVE_PREFIX,
			REMOVE_RANGE,
			MERGE
		};
		struct preprocessed_t{
                        iterator it;
			volatile_buffer vbuf;
			volatile_map_entry* entry_ptr = nullptr;

                        typedef boost::intrusive::any_to_list_hook<
				boost::intrusive::member_hook<
					volatile_map_entry,
					boost::intrusive::any_member_hook<>,
				        &volatile_map_entry::_hook>>
				kv_list_t_option;
                        typedef boost::intrusive::list<
			        volatile_map_entry,
                                kv_list_t_option>
			kv_list_t;
                        kv_list_t kv_to_release;

                        preprocessed_t()
			{
			}
			preprocessed_t(preprocessed_t &&from)
			    : it(std::move(from.it)), vbuf(std::move(from.vbuf)),
			      entry_ptr(from.entry_ptr)
			{
			}
			preprocessed_t(const volatile_buffer& val)
			    : vbuf(val)
			{
			}
			preprocessed_t(volatile_buffer &&val)
			    : vbuf(std::move(val))
			{
			}
			preprocessed_t(iterator &&it, volatile_map_entry* ptr)
			    : it(std::move(it)),
			      entry_ptr(ptr)
			{
			}
			void
			release(volatile_map_entry* e)
			{
				if (e) {
					kv_to_release.push_back(*e);
				}
			}
		};

		std::vector<std::tuple<Op, volatile_buffer, preprocessed_t>>
			ops;
		DB  *kv = nullptr;
		pmem::obj::pool_base *pool = nullptr;
		bool preexec_mode = false;
		bool prelookup_mode = false;

	public:
		batch()
		{
		}
		batch(DB &_kv,
		      pmem::obj::pool_base &_pool,
                      bool _preexec_mode = false,
                      bool _prelookup_mode = false)
		    : kv(&_kv),
		      pool(&_pool),
		      preexec_mode(_preexec_mode),
                      prelookup_mode(_prelookup_mode)
		{
		}
		~batch()
		{
			ceph_assert(ops.empty());
		}
		void
		swap(batch &other)
		{
			std::swap(kv, other.kv);
			std::swap(pool, other.pool);
			std::swap(preexec_mode, other.preexec_mode);
			std::swap(prelookup_mode, other.prelookup_mode);
			ops.swap(other.ops);
		}

		void
		set(const volatile_buffer &key, const volatile_buffer &val)
		{
			/*std::cout << __func__ << "1 " << key.length() << " "
				  << val.length() << std::endl;*/
			if (preexec_mode) {
				ceph_assert(kv);
				ceph_assert(pool);
				auto pp = entry::allocate(kv->get_allocator(),
                                                          key, val, false);
                                iterator it;
				if (prelookup_mode) {
					it = kv->lower_bound(key);
				}
				ops.emplace_back(SET_PREEXEC, key,
                                        preprocessed_t(std::move(it),
						new volatile_map_entry(pp)));
			} else {
				ops.emplace_back(SET, key, val);
			}
		}
		void
		set(volatile_buffer &&key, volatile_buffer &&val)
		{
			/*std::cout << __func__ << "2 " << key.length() << " "
				  << val.length() << std::endl;*/
			if (preexec_mode) {
				ceph_assert(kv);
				ceph_assert(pool);
				auto pp = entry::allocate(kv->get_allocator(),
							  key, val, false);
				iterator it;
				if (prelookup_mode) {
					it = kv->lower_bound(key);
				}
				ops.emplace_back(SET_PREEXEC, key,
					preprocessed_t(std::move(it),
                                                new volatile_map_entry(pp)));
			} else {
				ops.emplace_back(SET, key, val);
			}
		}
		void
		set(volatile_buffer &&key, const volatile_buffer &val)
		{
			/*std::cout << __func__ << "3 " << key.length() << " "
				  << val.length() << std::endl;*/
			if (preexec_mode) {
				ceph_assert(kv);
				ceph_assert(pool);
				auto pp = entry::allocate(kv->get_allocator(),
							  key, val, false);
				iterator it;
				if (prelookup_mode) {
					it = kv->lower_bound(key);
				}
				ops.emplace_back(SET_PREEXEC, key,
					preprocessed_t(std::move(it),
                                                new volatile_map_entry(pp)));

			} else {
				ops.emplace_back(SET, key, val);
			}
		}
		void
		set(const volatile_buffer &key, volatile_buffer &&val)
		{
			/*std::cout << __func__ << "4 " << key.length() << " "
				  << val.length() << std::endl;*/
			if (preexec_mode) {
				ceph_assert(kv);
				ceph_assert(pool);
				auto pp = entry::allocate(kv->get_allocator(),
							  key, val, false);
				iterator it;
				if (prelookup_mode) {
					it = kv->lower_bound(key);
				}
				ops.emplace_back(SET_PREEXEC, key,
					preprocessed_t(std::move(it),
						new volatile_map_entry(pp)));
			} else {
				ops.emplace_back(SET, key, val);
			}
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
                //
                // disposes ops in unsubmitted(!) batch
                //
		void
		dispose_volatile()
		{
			for (auto &o : ops) {
				auto& pp = std::get<2>(o);
				ReleaseVolatile(kv->get_allocator())(pp.entry_ptr);
				pp.kv_to_release.clear_and_dispose(
					ReleaseVolatile(kv->get_allocator()));
			}
			ops.clear();
		}
                size_t
		get_ops_count() const
		{
			return ops.size();
		}
	};

	DB()
	{
	}
	virtual ~DB()
	{
		shutdown();
	}

	uint64_t
	get_last_op(size_t hash) const
	{
		return op_seq[hash % BUCKET_COUNT];
	}
	PMemAllocator &
	get_allocator() {
                return alloc;
        }

        void
	create(pmem::obj::pool<pmem_kv::root> &pool,
	       uint64_t log_size)
	{
		alloc.create(pool, log_size);
	}

	void
	load_from_pool(pmem::obj::pool<pmem_kv::root> &pool)
	{
		ceph_assert(kv_set.empty());
		alloc.load(pool,
                        [&](byte* ptr) {
			        ceph_assert(ptr != nullptr);
			        entry_ptr kv_ptr(reinterpret_cast<entry*>(ptr));
				kv_set.insert(*new volatile_map_entry(
					kv_ptr));
                        });
	}

        void
	shutdown()
	{
		alloc.shutdown();
		kv_set.clear_and_dispose(DisposeVolatile());
	}

        void
	insert(pmem::obj::pool<pmem_kv::root> &pool,
               const volatile_buffer &k,
	       const volatile_buffer &v,
               bool persist = true)
	{
		ceph_assert(k.length() != 0);
		ceph_assert(!v.is_null());
		entry_ptr kv_ptr = entry::allocate(alloc, k, v, true);
		ceph_assert(kv_ptr);
		{
		        std::lock_guard l(general_mutex);

			kv_set_t::insert_commit_data commit_data;
			auto ip = kv_set.insert_check(k, Compare(),
						      commit_data);
			if (!ip.second) {
				std::swap(ip.first->kv_pair, kv_ptr);
			} else {
				kv_set.insert_commit(
					*new volatile_map_entry(kv_ptr),
					commit_data);
				kv_ptr = nullptr;
			}
                }
		if (kv_ptr) {
			entry::release(alloc, kv_ptr);
		}
		if (persist) {
			flush();
		}
	}
	void
	erase(pmem::obj::pool<pmem_kv::root> &pool,
              const volatile_buffer &k,
	      bool persist = true)
	{
		ceph_assert(k.length() != 0);
		entry_ptr kv_ptr = nullptr;
		{
	              std::lock_guard l(general_mutex);
	              auto it = kv_set.find(k, Compare());
	              if (it != kv_set.end()) {
		              kv_ptr = it->kv_pair;
		              kv_set.erase_and_dispose(it,
                                      DisposeVolatile());
	              }
                }
		if (kv_ptr != nullptr) {
		        entry::release(alloc, kv_ptr);
		        if (persist) {
			        flush();
                        }
                }
	}
	void
	flush()
	{
		alloc.commit();
	}


	void
	submit_batch(batch &b,
		    std::function<void(BatchTimes, const ceph::timespan &)>
			    f = nullptr);

        void
	commit_batch_set(pmem::obj::pool<pmem_kv::root> &pool, batch *bp, size_t batch_count,
		    std::function<void(BatchTimes, const ceph::timespan &)> f =
			    nullptr);
	void
	commit_batch(pmem::obj::pool<pmem_kv::root> &pool, batch &b,
		    std::function<void(BatchTimes, const ceph::timespan &)> f =
			    nullptr)
	{
		commit_batch_set(pool, &b, 1, f);
	}
	void
	apply_batch(pmem::obj::pool<pmem_kv::root> &pool, batch &b,
		     std::function<void(BatchTimes, const ceph::timespan &)> f =
			     nullptr)
	{
		submit_batch(b, f);
		commit_batch(pool, b, f);
	}

	bool
	empty()
	{
		std::shared_lock l(general_mutex);
		return kv_set.empty();
	}
	const entry_ptr &
	first()
	{
		std::shared_lock l(general_mutex);
		ceph_assert(!kv_set.empty());
		return kv_set.begin()->kv_pair;
	}
	const entry_ptr &
	last()
	{
		std::shared_lock l(general_mutex);
		ceph_assert(!kv_set.empty());
		return (--kv_set.end())->kv_pair;
	}
	const entry_ptr
	get(const volatile_buffer &key)
	{
		std::shared_lock l(general_mutex);
		entry_ptr ret = nullptr;
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

        inline size_t
	get_free() const
	{
	        return alloc.get_free();
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
		return iterator(*this, key, iterator::EXACT_MATCH);
	}

	void test(pmem::obj::pool<pmem_kv::root> &pool, bool remove,
		  uint64_t base, uint64_t count);
	void test2(pmem::obj::pool<pmem_kv::root> &pool);
	void test3(pmem::obj::pool<pmem_kv::root> &pool);
	void test3_1(pmem::obj::pool<pmem_kv::root> &pool);
	void test3_2(pmem::obj::pool<pmem_kv::root> &pool);
	void test3_3(pmem::obj::pool<pmem_kv::root> &pool);

private:
	void
	_commit_batch(batch &b,
		      std::function<void(BatchTimes, const ceph::timespan &)> f);

}; // class DB
}; // namespace pmem_kv

template <>
struct std::hash<pmem_kv::buffer_view> {
	size_t
	operator()(const pmem_kv::buffer_view &bv) const
	{
		return std::hash<std::string_view>{}(bv.to_string_view());
	}
};

template <>
struct std::hash<pmem_kv::volatile_buffer> {
	size_t
	operator()(const pmem_kv::volatile_buffer &bv) const
	{
		return bv.get_hash();
	}
};
