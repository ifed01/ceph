#pragma once
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "include/buffer.h"
#include "include/ceph_assert.h"
#include "include/intarith.h"
#include <algorithm>
#include <boost/intrusive/set.hpp>
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

const size_t PMEM_UNIT_SIZE = 0x100;
const size_t PMEM_PAGE_SIZE = 0x100;

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
		// memcpy(dest, data + o, len);
		// pmem_memcpy_nodrain(dest, data + o, len);
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

class pmem_kv_entry2;
using pmem_kv_entry2_ptr = pmem::obj::persistent_ptr<pmem_kv_entry2[]>;

class volatile_buffer;
class pmem_kv_entry2 {
	pmem::obj::p<int32_t> allocated = 0;
	pmem::obj::p<uint32_t> key_size = 0;
	pmem::obj::p<uint32_t> val_size = 0;

	enum { HEADER_SIZE =
		       sizeof(key_size) + sizeof(val_size) + sizeof(allocated),
	       MAX_PAGE0_SIZE = PMEM_PAGE_SIZE - HEADER_SIZE };
	byte data[MAX_PAGE0_SIZE];

public:
	static pmem_kv_entry2_ptr allocate(const volatile_buffer &k,
					   const volatile_buffer &v);

	static pmem_kv_entry2_ptr
	allocate_atomic_volatile(pmem::obj::pool_base &pool,
				 const volatile_buffer &k,
				 const volatile_buffer &v);

	static void
	release(pmem_kv_entry2_ptr p)
	{
		pmem::obj::delete_persistent<pmem_kv_entry2[]>(
			p.get(),
			p2roundup<uint32_t>(p[0].allocated, PMEM_PAGE_SIZE));
	}
	static void
	release_atomic_volatile(pmem_kv_entry2_ptr p)
	{
		pmem::obj::delete_persistent_atomic<pmem_kv_entry2[]>(
			p, p2roundup<uint32_t>(p[0].allocated, PMEM_PAGE_SIZE));
	}
	uint32_t
	get_allocated() const
	{
		return (uint32_t)std::abs(allocated);
	}
	bool
	is_persistent() const
	{
		return allocated >= 0;
	}
	inline void
	persist()
	{
		if (allocated < 0) {
			allocated = -allocated;
		}
	}

	void assign(const volatile_buffer &k, const volatile_buffer &v,
		    bool need_snapshot = true);
	bool try_assign_value(const volatile_buffer &v);
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
	operator<(const pmem_kv_entry2 &other_kv) const
	{
		return key_view() < other_kv.key_view();
	}
};
std::ostream &operator<<(std::ostream &out, const pmem_kv_entry2 &e);

using volatile_buffer_base =
	boost::variant<void *, buffer_view, bufferlist, std::string,
		       pmem_pages_t, pmem_kv_entry2_ptr>;

class volatile_buffer : public volatile_buffer_base {
	enum { Null, BufferView, BufferList, String, PMemPages, PMemKVEntry2 };

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
	volatile_buffer(const pmem_kv_entry2_ptr &_pp)
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
			case PMemKVEntry2: {
				return boost::get<pmem_kv_entry2_ptr>(*this)[0]
					.get_allocated();
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
			case PMemKVEntry2:
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
			case PMemKVEntry2:
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
			case PMemKVEntry2:
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
			case PMemKVEntry2:
			default:
				ceph_assert(false);
		}
	}
	pmem_pages_t
	take_pmem_pages_away()
	{
		switch (which()) {
			case PMemPages: {
				auto &p = boost::get<pmem_pages_t>(*this);
				pmem_pages_t res(0, NULL);
				p.swap(res);
				return res;
			}
			case Null:
			case BufferView:
			case BufferList:
			case String:
			default:
				break;
		}
		return pmem_pages_t(0, nullptr);
	}
	pmem_kv_entry2_ptr
	take_pmem_kv_entry_away()
	{
		switch (which()) {
			case PMemKVEntry2: {
				auto &p = boost::get<pmem_kv_entry2_ptr>(*this);
				pmem_kv_entry2_ptr res = p;
				p = nullptr;
				return res;
			}
			case Null:
			case BufferView:
			case BufferList:
			case String:
			case PMemPages:
			default:
				break;
		}
		return nullptr;
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
			case PMemKVEntry2:
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
			case PMemKVEntry2:
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
			case PMemKVEntry2:
			default:
				ceph_assert(false);
		}
		return false;
	}
};

template <class T>
struct volatile_map_entry_base {
	T kv_pair;
	std::string ss;
	boost::intrusive::set_member_hook<> set_hook;
	volatile_map_entry_base(T _kv_pair) 
		: kv_pair(_kv_pair), ss(_kv_pair[0].key_view().to_str())
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
		//return kv_pair[0] < other_kv.kv_pair[0];
	}
};

class DB {
	friend class iterator;

	using entry = pmem_kv_entry2;
	using entry_ptr = pmem_kv_entry2_ptr;

	using volatile_map_entry = volatile_map_entry_base<entry_ptr>;

	typedef boost::intrusive::set<
		volatile_map_entry,
		boost::intrusive::member_hook<
			volatile_map_entry, boost::intrusive::set_member_hook<>,
			&volatile_map_entry::set_hook>>
		kv_set_t;
	kv_set_t kv_set;
	enum { BUCKET_COUNT = 16386 };
	uint64_t op_seq[BUCKET_COUNT] = {0};

	ceph::shared_mutex general_mutex = ceph::make_shared_mutex("DB");

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
			//return s < e.kv_pair[0].key_view();
			return s < e.key_view();
		}

		bool
		operator()(const volatile_map_entry &e,
			   const buffer_view &s) const
		{
			//return e.kv_pair[0].key_view() < s;
			return e.key_view() < s;
		}
		bool
		operator()(const volatile_buffer &b,
			   const volatile_map_entry &e) const
		{
			//return b < e.kv_pair[0].key_view();
			return b < e.key_view();
		}

		bool
		operator()(const volatile_map_entry &e,
			   const volatile_buffer &b) const
		{
			//return !(b <= e.kv_pair[0].key_view());
			return !(b <= e.key_view());
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
                        if (!kv) {
                                return invalid_val;
			}
			ceph_assert(ceph_mutex_is_locked(kv->general_mutex));
			iterator_imp res = invalid_val;
			if (!cur_key.empty()) {
				if (last_op_seqno ==
				    kv->get_last_op(cur_key_hash)) {
				    res = it;
                                }
			}
                        return res;
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
			pmem_kv_entry2_ptr entry_ptr = nullptr;

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
			preprocessed_t(iterator &&it, pmem_kv_entry2_ptr ptr)
			    : it(std::move(it)),
			      entry_ptr(ptr)
			{
			}
		};

		std::vector<std::tuple<Op, volatile_buffer, preprocessed_t>>
			ops;
		DB  &kv;
		pmem::obj::pool_base &pool;
		bool preexec_mode = false;
		bool prelookup_mode = false;

		static pmem_pages_t
		allocate_pmem_pages_and_copyin(pmem::obj::pool_base &pool,
					       const volatile_buffer &v)
		{
			pmem_pages_t res;
			auto sz = v.length();
			sz = p2roundup(sz, PMEM_PAGE_SIZE);
			res.first = sz;
			pmem::obj::make_persistent_atomic<pmem_page[]>(
				pool, res.second, sz / PMEM_PAGE_SIZE,
				pmem::obj::allocation_flag_atomic(
					POBJ_XALLOC_ZERO));
			// FIXME minor: check for alloc errors
			ceph_assert(res.second);
			v.copy_out(
				0, v.length(),
				res.second[0]
					.data()); // FIXME: adjust by one byte
						  // to mark uncommitted pages
			return res;
		}
		static pmem_kv_entry2_ptr
		allocate_pmem_kv_and_copyin(pmem::obj::pool_base &pool,
					    const volatile_buffer &k,
					    const volatile_buffer &v)
		{
			pmem_kv_entry2_ptr res;
			res = pmem_kv_entry2::allocate_atomic_volatile(pool, k,
								       v);
			return res;
		}

	public:
		batch(DB &_kv,
		      pmem::obj::pool_base &_pool,
                      bool _preexec_mode = false,
                      bool _prelookup_mode = false)
		    : kv(_kv),
		      pool(_pool),
		      preexec_mode(_preexec_mode),
                      prelookup_mode(_prelookup_mode)
		{
		}
		~batch()
		{
			reset();
		}
		void
		set(const volatile_buffer &key, const volatile_buffer &val)
		{
			if (preexec_mode) {
				auto pp = allocate_pmem_kv_and_copyin(pool, key,
								      val);
                                iterator it;
				if (prelookup_mode) {
					it = kv.lower_bound(key);
				}
				ops.emplace_back(SET_PREEXEC, key,
                                        preprocessed_t(std::move(it), pp));
			} else {
				ops.emplace_back(SET, key, val);
			}
		}
		void
		set(volatile_buffer &&key, volatile_buffer &&val)
		{
			if (preexec_mode) {
				auto pp = allocate_pmem_kv_and_copyin(pool, key,
								      val);
				iterator it;
				if (prelookup_mode) {
					it = kv.lower_bound(key);
				}
				ops.emplace_back(SET_PREEXEC, key,
					preprocessed_t(std::move(it), pp));
			} else {
				ops.emplace_back(SET, key, val);
			}
		}
		void
		set(volatile_buffer &&key, const volatile_buffer &val)
		{
			if (preexec_mode) {
				auto pp = allocate_pmem_kv_and_copyin(pool, key,
								      val);
				iterator it;
				if (prelookup_mode) {
					it = kv.lower_bound(key);
				}
				ops.emplace_back(SET_PREEXEC, key,
					preprocessed_t(std::move(it), pp));
			} else {
				ops.emplace_back(SET, key, val);
			}
		}
		void
		set(const volatile_buffer &key, volatile_buffer &&val)
		{
			if (preexec_mode) {
				auto pp = allocate_pmem_kv_and_copyin(pool, key,
								      val);
				iterator it;
				if (prelookup_mode) {
					it = kv.lower_bound(key);
				}
				ops.emplace_back(SET_PREEXEC, key,
					preprocessed_t(std::move(it), pp));
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
		void
		reset()
		{
			for (auto &o : ops) {
				if (std::get<0>(o) == SET_PREEXEC) {
					pmem_kv_entry2_ptr pp =
						std::get<2>(o).entry_ptr;
					if (pp) {
						// delete_persistent_atomic does
						// exactly the same call to
						// pmemobj_free. Hence using it
						// directly
						pmemobj_free(pp.raw_ptr());
					}
				}
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
		kv_set.clear_and_dispose(Dispose());
	}

	uint64_t
	get_last_op(size_t hash) const
	{
		return op_seq[hash % BUCKET_COUNT];
	}
	template <class T>
	size_t
	for_each(pmem::obj::pool_base &pool,
		 std::function<void(pmemoid &)> fn = nullptr)
	{
		ceph_assert(kv_set.empty());
		std::lock_guard l(general_mutex);
		size_t entries = 0;
		auto type_num = pmem::detail::type_num<T>();
		auto o = POBJ_FIRST_TYPE_NUM(pool.handle(), type_num);
		while (!OID_IS_NULL(o)) {
			++entries;
			if ((entries % 100000) == 0) {
				std::cout << "Loading units " << entries
					  << std::endl;
			}

			auto _o = o;
			o = POBJ_NEXT_TYPE_NUM(o);
			if (fn) {
				fn(_o);
			}
		}
		return entries;
	}

	void
	load_from_pool(pmem::obj::pool_base &pool)
	{
		ceph_assert(kv_set.empty());
		size_t entries = 0;
		for_each<pmem_kv_entry2>(pool, [&](pmemoid &o) {
			++entries;
			if ((entries % 100000) == 0) {
				std::cout << "Loading " << entries << std::endl;
			}
			entry_ptr kv_ptr((entry *)pmemobj_direct(
				o)); // FIXME minor: in fact we just
				     // need the ability to create
				     // persistent_ptr from PMEMoid not
				     // from void*
			if (!kv_ptr[0].is_persistent()) {
				entry::release_atomic_volatile(kv_ptr);
			} else {
				kv_set.insert(*new volatile_map_entry(kv_ptr));
			}
		});
	}

	void
	insert(pmem::obj::pool_base &pool, const volatile_buffer &k,
	       const volatile_buffer &v)
	{
		ceph_assert(k.length() != 0);
		ceph_assert(!v.is_null());
		std::lock_guard l(general_mutex);
		pmem::obj::transaction::run(pool, [&] {
			auto it = kv_set.find(k, Compare());
			if (it == kv_set.end()) {
				auto kv_ptr = entry::allocate(k, v);
				kv_set.insert(*new volatile_map_entry(kv_ptr));
			} else {
				it->kv_pair[0].assign(k, v);
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
				entry::release(it->kv_pair);
				kv_set.erase_and_dispose(it, Dispose());
			}
		});
	}

/*#define LOG_TIME_START ;
#define LOG_TIME(idx) ;
  */

#define LOG_TIME_START \
        t0 = mono_clock::now();
#define LOG_TIME(idx)                                                          \
	if (f)                                                                 \
		f(idx, mono_clock::now() - t0);

	enum ApplyBatchTimes {
		LOCK_TIME,
		START_TIME,
		END_TIME,
		SET_LOOKUP_TIME,
		SET_EXEC_TIME,
		SET_EXISTED0_TIME,
		SET_EXISTED1_TIME,
		SET_EXISTED2_TIME,
		SET_EXISTED3_TIME,
		SET_MAKE_NEW_PERSISTENT_TIME,
		SET_INSERT_TIME,
		REMOVE_LOOKUP_TIME,
		REMOVE_EXEC_TIME,
		MERGE_LOOKUP_TIME,
		MERGE_EXEC_TIME,
		MAX_TIMES
	};

	void
	apply_batch(pmem::obj::pool_base &pool, batch &b,
		    std::function<void(ApplyBatchTimes, const ceph::timespan &)>
			    f = nullptr)
	{
		if (b.ops.empty())
			return;
                mono_clock::time_point t0;
                LOG_TIME_START;
		pmem::obj::transaction::
			run(
				pool, [&] {
					LOG_TIME(START_TIME);
                                        LOG_TIME_START;
					std::lock_guard l(general_mutex);
					LOG_TIME(LOCK_TIME);

					for (auto &p : b.ops) {
						auto &key = std::get<1>(p);
						/*std::cerr << key << " " <<
						   std::get<0>(p)
							  << std::endl;*/
						switch (std::get<0>(p)) {
							case batch::SET: {
                                                                LOG_TIME_START;
								kv_set_t::insert_commit_data
									commit_data;
								auto ip = kv_set.insert_check(
									key,
									Compare(),
									commit_data);
								LOG_TIME(
									SET_LOOKUP_TIME);
								LOG_TIME_START;
								auto &val = std::
									get<2>(p).vbuf;
								if (!ip.second) {
									int path =
										0;
									if (!ip.first
										     ->kv_pair
											     [0]
										     .try_assign_value(
											     val)) {
										entry::release(
											ip.first->kv_pair);
										ip.first->kv_pair =
											entry::allocate(
												key,
												val);
										path = 1;
									}

									LOG_TIME(ApplyBatchTimes(
										SET_EXISTED0_TIME +
										path));
								} else {
									auto kv = entry::allocate(
										key,
										val);
									LOG_TIME(
										SET_MAKE_NEW_PERSISTENT_TIME);
                                                                        mono_clock::time_point t0;
                                                                        LOG_TIME_START;
									kv_set.insert_commit(
										*new volatile_map_entry(
											kv),
										commit_data);
									inc_op(key);
									LOG_TIME(
										SET_INSERT_TIME);
								}
								LOG_TIME(
									SET_EXEC_TIME);
								break;
							}
							case batch:: SET_PREEXEC: {
                                                                LOG_TIME_START;
								auto &preproc_val =
									std::get<2>(p);

								pmem_kv_entry2_ptr kv;
								*(kv.raw_ptr()) = preproc_val.entry_ptr.raw();
								*(preproc_val.entry_ptr.raw_ptr()) = OID_NULL;
								ceph_assert(
									kv !=
									nullptr);
								iterator_imp preexec_it =
									preproc_val.it.get_validate_iterator_impl(
                                                                                kv_set.end());
								if (preexec_it != kv_set.end()) {
									kv[0].persist();
									if (key == string_to_view(preproc_val.it.get_current_key())) {
										entry::release(
											preexec_it->kv_pair);
                                                                                *(preexec_it->kv_pair.raw_ptr()) =
                                                                                        kv.raw();
									        LOG_TIME(
										        SET_EXISTED1_TIME);
									} else {
										kv_set.insert(
                                                                                        preexec_it,
											*new volatile_map_entry(
												kv));
									        LOG_TIME(
										        SET_EXISTED2_TIME);
									}
								} else {
                                                                        kv_set_t::insert_commit_data
									        commit_data;
								        auto ip = kv_set.insert_check(
									        key,
									        Compare(),
									        commit_data);

								        LOG_TIME(
									        SET_LOOKUP_TIME);
                                                                        LOG_TIME_START;
									kv[0].persist();
									if (!ip.second) {
									        entry::release(
										        ip.first->kv_pair);

									        *(ip.first->kv_pair.raw_ptr()) =
										        kv.raw();
									        LOG_TIME(ApplyBatchTimes(
										        SET_EXISTED0_TIME));
								        } else {
									        kv_set.insert_commit(
										        *new volatile_map_entry(
											        kv),
										        commit_data);
                                                                                LOG_TIME(
                                                                                  SET_MAKE_NEW_PERSISTENT_TIME);
                                                                                mono_clock::time_point t0;
                                                                                LOG_TIME_START;
                                                                                inc_op(key);
									        LOG_TIME(
										        SET_INSERT_TIME);
								        }
                                                                }
								LOG_TIME(
									SET_EXEC_TIME);
								break;
							}
							case batch::REMOVE: {
                                                                LOG_TIME_START;
								auto it = kv_set.find(
									key,
									Compare());
								LOG_TIME(
									REMOVE_LOOKUP_TIME);
                                                                LOG_TIME_START;
								if (it !=
								    kv_set.end()) {
									inc_op(key);
									entry::release(
										it->kv_pair);
									kv_set.erase_and_dispose(
										it,
										Dispose());
								}
								LOG_TIME(
									REMOVE_EXEC_TIME);
								break;
							}
							case batch::
								REMOVE_PREFIX: {
                                                                LOG_TIME_START;
								auto it = kv_set.lower_bound(
									key,
									Compare());

								LOG_TIME(
									REMOVE_LOOKUP_TIME);
                                                                LOG_TIME_START;
								while (it != kv_set.end() &&
								       key.is_prefix_for(
									       it->kv_pair[0]
										       .key_view())) {
									inc_op(it->kv_pair[0]
										       .key_view());
									entry::release(
										it->kv_pair);
									it = kv_set.erase_and_dispose(
										it,
										Dispose());
								}
								LOG_TIME(
									REMOVE_EXEC_TIME);
								break;
							}
							case batch::
								REMOVE_RANGE: {
								LOG_TIME_START;
								auto &key_end = std::
									get<2>(p).vbuf;
								auto it = kv_set.lower_bound(
									key,
									Compare());
								LOG_TIME(
									REMOVE_LOOKUP_TIME);
                                                                LOG_TIME_START;
								while (it != kv_set.end() &&
								       key_end >
									       it->kv_pair[0]
										       .key_view()) {
									inc_op(it->kv_pair[0]
										       .key_view());
									entry::release(
										it->kv_pair);
									it = kv_set.erase_and_dispose(
										it,
										Dispose());
								}
								LOG_TIME(
									REMOVE_EXEC_TIME);
								break;
							}
							case batch::MERGE: {
								LOG_TIME_START;
								kv_set_t::insert_commit_data
									commit_data;
								auto ip = kv_set.insert_check(
									key,
									Compare(),
									commit_data);
								LOG_TIME(
									MERGE_LOOKUP_TIME);
                                                                LOG_TIME_START;
								auto &val = std::
									get<2>(p).vbuf;
								bool present =
									!ip.second;
								buffer_view
									orig_bv;
								if (present) {
									orig_bv =
										ip.first
											->kv_pair
												[0]
											.value_view();
								}
								volatile_buffer new_v = _handle_merge(
									key,
									val,
									orig_bv);

								if (!present) {
									auto v = entry::allocate(
										key,
										new_v);
									kv_set.insert_commit(
										*new volatile_map_entry(
											v),
										commit_data);
									inc_op(key);
								} else if (
									!ip.first
										 ->kv_pair
											 [0]
										 .try_assign_value(
											 new_v)) {
									entry::release(
										ip.first->kv_pair);
									ip.first->kv_pair =
										entry::allocate(
											key,
											new_v);
								}
								LOG_TIME(
									MERGE_EXEC_TIME);

								break;
							}
						}
					}
					t0 = mono_clock::now();
				});
		LOG_TIME(END_TIME);
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
		entry_ptr ret;
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
		return iterator(*this, key, iterator::EXACT_MATCH);
	}

	void test(pmem::obj::pool_base &pool, bool remove = true);
	void test2(pmem::obj::pool_base &pool);

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
