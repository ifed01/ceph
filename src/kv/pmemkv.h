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
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/allocation_flag.hpp>
  #include <libpmemobj.h>
#include <shared_mutex>
#include <stdexcept>
#include <string>

const size_t PMEM_UNIT_SIZE = 0x100;
const size_t PMEM_PAGE_SIZE = 0x100;

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
/*class pmem_pages_t : public pmem_pages_base {
public:
	pmem_pages_t(uint32_t sz = 0, pmem_page *ptr = nullptr)
	    : pmem_pages_base(sz, ptr)
	{
	}
	pmem_pages_t(pmem_pages_t &&from) : pmem_pages_base(from)
	{
		from.second = nullptr;
	}
	~pmem_pages_t()
	{
		if (second != nullptr) {
		}
	}
};*/

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
	static pmem_kv_entry2_ptr
	allocate(const volatile_buffer &k, const volatile_buffer &v);

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
			p,
			p2roundup<uint32_t>(p[0].allocated,
					PMEM_PAGE_SIZE));
	}
	uint32_t
	get_allocated() const
	{
                return (uint32_t)std::abs(allocated);
	}
        bool is_persistent() const {
		return allocated >= 0;
	}
	void persist()
	{
		if (allocated < 0) {
			allocated = -allocated;
		}
	}
	void
	assign(const volatile_buffer &k, const volatile_buffer &v,
	       bool need_snapshot = true);
	bool
	try_assign_value(const volatile_buffer &v);
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
		res.append(data, key_size);
		return res;
	}
	const buffer_view
	value_view() const
	{
		buffer_view res;
		res.append(data + key_size, val_size);
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
	operator<(const pmem_kv_entry2 &other_kv) const
	{
		return key_view() < other_kv.key_view();
	}
};
std::ostream &operator<<(std::ostream &out, const pmem_kv_entry2 &e);

using volatile_buffer_base = boost::variant<void *, buffer_view, bufferlist,
					    std::string, pmem_pages_t, pmem_kv_entry2_ptr>;

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
	volatile_buffer(const pmem_kv_entry2_ptr &_pp) : volatile_buffer_base(_pp)
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
						// TX_MEMCPY(dest, src, l);
						len -= l;
						o += l;
						dest += l;
					}
					offs += it->length();
					++it;
				}
				/*auto it = bl.begin();
				it += o;
				it.copy(len, dest);*/
				break;
			}
			case String: {
				/*boost::get<std::string>(*this).copy(dest, len,
								    o);*/
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
	const char *
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

const buffer_view string_to_view(const std::string &s);
const buffer_view string_to_view(const char *s);

class pmem_kv_entry;
using pmem_kv_entry_ptr = pmem::obj::persistent_ptr<pmem_kv_entry>;

class pmem_kv_entry {
	// FIXME minor: merge into a single 64bit field to reduce logging
	// overhead
	pmem::obj::p<uint32_t> key_size = 0;
	pmem::obj::p<uint32_t> val_size = 0;

	// byte_array_ptr extern_data = nullptr;
	pmem_page_ptr extern_data = nullptr;
	pmem::obj::p<uint32_t> extern_data_size = 0;

	enum { MAX_EMBED_SIZE = PMEM_UNIT_SIZE - sizeof(key_size) -
		       sizeof(val_size) - sizeof(extern_data) -
		       sizeof(extern_data_size) };
	// pmem::obj::array<byte, MAX_EMBED_SIZE> embed_data;
	byte embed_data[MAX_EMBED_SIZE];

	void
	dispose(pmem_page_ptr &data, uint32_t size)
	{
		if (data != nullptr) {
			// pmemobj_tx_free(data.raw());
			uint32_t sz = p2roundup<uint32_t>(sz, PMEM_PAGE_SIZE) /
				PMEM_PAGE_SIZE;
			pmem::obj::delete_persistent<pmem_page[]>(data.get(),
								  size_t(sz));
			data = nullptr;
		}
	}
	void
	dispose()
	{
		if (extern_data != nullptr) {
			dispose(extern_data, extern_data_size);
			extern_data_size = 0;
		}
	}

	void
	alloc_and_assign_extern(const volatile_buffer &s)
	{
		ceph_assert(extern_data == nullptr);
		ceph_assert(s.length() != 0);
		/*
		 * We need to cache pmemobj_tx_alloc return value and only after
		 * that assign it to _data, because when pmemobj_tx_alloc fails,
		 * it aborts transaction.
		 */
		size_t new_len = p2roundup(s.length(), PMEM_PAGE_SIZE);
		// std::cout << "??? " << sizeof(pmem_page) << " " << new_len /
		// PMEM_PAGE_SIZE << std::endl;
		pmem_page_ptr res = pmem::obj::make_persistent<pmem_page[]>(
			new_len / PMEM_PAGE_SIZE);
		/*pmemobj_tx_alloc(new_len / PMEM_PAGE_SIZE,
				 pmem::detail::type_num<pmem_page>());*/
		// std::cout << "!!! " << new_len / PMEM_PAGE_SIZE << std::endl;
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
		// FIXME minor: remove
		// sanity development phase check
		// std::cout << "vvv?vvv" << std::endl;
		if (new_len > PMEM_PAGE_SIZE) {
			ceph_assert(res[0].data() + PMEM_PAGE_SIZE ==
				    res[1].data());
		}
		/*std::cout << "vvvvvv" << std::endl;
		std::cout << "------" << s.length() << " " << std::hex <<
		(void*)res[0].data() << std::dec
			  << std::endl;
		std::cout << "------" << s.length() << " " << std::hex
			  << (void *)res[1].data() << std::dec << std::endl;*/
		s.copy_out(0, s.length(), res[0].data());
		// std::cout << "^^^^" << std::endl;
		extern_data = res;
		extern_data_size = new_len;
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
		size_t new_len =
			p2roundup(s1.length() + s2.length(), PMEM_PAGE_SIZE);
		// std::cout << "??? " << new_len / PMEM_PAGE_SIZE << std::endl;
		pmem_page_ptr res = pmem::obj::make_persistent<pmem_page[]>(
			new_len / PMEM_PAGE_SIZE);
		/*pmemobj_tx_alloc(new_len / PMEM_PAGE_SIZE,
			 pmem::detail::type_num<pmem_page>());*/
		// std::cout << "!!! " << new_len / PMEM_PAGE_SIZE << std::endl;
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
		// FIXME minor: remove
		// sanity development phase check
		// std::cout << "vvv???vvv" << std::endl;
		if (new_len > PMEM_PAGE_SIZE) {
			ceph_assert(res[0].data() + PMEM_PAGE_SIZE ==
				    res[1].data());
		}

		// std::cout << "vvvvvv" << std::endl;
		s1.copy_out(0, s1.length(), res[0].data());
		/*std::cout << "------" << std::hex << res[0].data() << std::dec
		<< std::endl; std::cout << "------" << std::hex << res[0].data()
		+ s1.length()
			  << std::dec << std::endl;*/
		s2.copy_out(0, s2.length(), res[0].data() + s1.length());
		// std::cout << "^^^^" << std::endl;
		extern_data = res;
		extern_data_size = new_len;
	}

public:
	static pmem_kv_entry_ptr
	allocate(const volatile_buffer &k, const volatile_buffer &v)
	{
		pmem_kv_entry_ptr res;
		res = pmem::obj::make_persistent<pmem_kv_entry>();
		ceph_assert(res[0].key_size == 0);
		ceph_assert(res[0].val_size == 0);
		res->assign(k, v, false);
		return res;
	}
	static void
        release(pmem_kv_entry_ptr p)
        {
              pmem::obj::delete_persistent<pmem_kv_entry>(p);
        }

	pmem_kv_entry()
	{
	}
	pmem_kv_entry(const volatile_buffer &k, const volatile_buffer &v)
	{
		assign(k, v, false);
	}

	~pmem_kv_entry()
	{
		dispose();
	}
	bool
	try_assign_value(const volatile_buffer &v)
	{
		ceph_assert(key_size != 0);
		val_size = v.length();
		size_t k_v_size = key_size + val_size;
		if (k_v_size <= MAX_EMBED_SIZE) {
			dispose();
			if (val_size) {
				/*auto slice =
					embed_data.range(key_size, val_size);
				v.copy_out(0, val_size, &slice.at(0));*/
				pmemobj_tx_add_range_direct(
					embed_data + key_size, val_size);
				v.copy_out(0, val_size, embed_data + key_size);
			}
		} else if (key_size <= MAX_EMBED_SIZE) {
			if (extern_data_size >= val_size) {
				ceph_assert(extern_data != nullptr);
				pmemobj_tx_add_range_direct(
					extern_data[0].data() + 0, val_size);

				v.copy_out(0, val_size,
				        extern_data[0].data());
			} else {
				dispose();
				alloc_and_assign_extern(v);
			}
		} else if (val_size <= MAX_EMBED_SIZE) {
			if (val_size) {
				/*auto slice = embed_data.range(0, val_size);
				v.copy_out(0, val_size, &slice.at(0));*/
				pmemobj_tx_add_range_direct(embed_data,
							    val_size);
				v.copy_out(0, val_size, embed_data);
			}
		} else {
			// FIXME minor: can be done in a bit more effective
			// manner
			// by reusing existing extern_data if possible
			auto old_extern_data = extern_data;
			auto old_size = extern_data_size;
			volatile_buffer k(key_view());
			extern_data = nullptr;
			extern_data_size = 0;
			alloc_and_assign_extern(k, v);
			dispose(old_extern_data, old_size);
		}
		return true;
	}
	void
	assign(const volatile_buffer &k, const volatile_buffer &v,
	       bool need_snapshot = true)
	{
		dispose();
		key_size = k.length();
		val_size = v.length();
		size_t k_v_size = key_size + val_size;
		if (k_v_size <= MAX_EMBED_SIZE) {
			/*			auto slice = embed_data.range(0,
			   k_v_size, need_snapshot ? k_v_size : 0);
						k.copy_out(0, key_size,
			   &slice.at(0)); if (val_size) { v.copy_out(0,
			   val_size, &slice.at(key_size));
						}*/
			if (need_snapshot) {
				pmemobj_tx_add_range_direct(embed_data,
							    k_v_size);
			}
			k.copy_out(0, key_size, embed_data);
			if (val_size) {
				v.copy_out(0, val_size, embed_data + key_size);
			}

		} else if (key_size <= MAX_EMBED_SIZE) {
			/*auto slice = embed_data.range(0, key_size,
				need_snapshot ? k_v_size : 0);
			k.copy_out(0, key_size, &slice.at(0));
			*/
			if (need_snapshot) {
				pmemobj_tx_add_range_direct(embed_data,
							    key_size);
			}
			k.copy_out(0, key_size, embed_data);
			alloc_and_assign_extern(v);
		} else if (val_size <= MAX_EMBED_SIZE) {
			if (val_size) {
				/*auto slice = embed_data.range(0, val_size);
				v.copy_out(0, val_size, &slice.at(0));*/
				if (need_snapshot) {
					pmemobj_tx_add_range_direct(embed_data,
								    val_size);
				}
				v.copy_out(0, val_size, embed_data);
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
			res.append(embed_data, key_size);
		} else if (key_size <= MAX_EMBED_SIZE) {
			res.append(embed_data, key_size);
		} else if (val_size <= MAX_EMBED_SIZE) {
			res.append(extern_data[0].data(), key_size);
		} else {
			res.append(extern_data[0].data(), key_size);
		}
		return res;
	}
	const buffer_view
	value_view() const
	{
		buffer_view res;
		size_t k_v_size = key_size + val_size;
		if (k_v_size <= MAX_EMBED_SIZE) {
			res.append(embed_data + key_size, val_size);
		} else if (key_size <= MAX_EMBED_SIZE) {
			res.append(extern_data[0].data(), val_size);
		} else if (val_size <= MAX_EMBED_SIZE) {
			res.append(embed_data, val_size);
		} else {
			res.append(extern_data[0].data() + key_size, val_size);
		}
		return res;
	}
	/*const buffer_view
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
	}*/

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
	}
};
std::ostream &operator<<(std::ostream &out, const pmem_kv_entry &e);

template <class T>
struct volatile_map_entry_base {
	T kv_pair;
	boost::intrusive::set_member_hook<> set_hook;
	volatile_map_entry_base(T _kv_pair) : kv_pair(_kv_pair)
	{
	}
	bool
	operator<(const volatile_map_entry_base<T> &other_kv) const
	{
		return kv_pair[0] < other_kv.kv_pair[0];
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
	uint64_t rm_seq = 0;
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
			return s < e.kv_pair[0].key_view();
		}

		bool
		operator()(const volatile_map_entry &e,
			   const buffer_view &s) const
		{
			return e.kv_pair[0].key_view() < s;
		}
		bool
		operator()(const volatile_buffer &b,
			   const volatile_map_entry &e) const
		{
			return b < e.kv_pair[0].key_view();
		}

		bool
		operator()(const volatile_map_entry &e,
			   const volatile_buffer &b) const
		{
			// return e.kv_pair->key_view().cmp(string_to_view(s)) <
			// 0;
			return !(b <= e.kv_pair[0].key_view());
			// return cmp(e.kv_pair->key_view(), string_to_view(s));
		}
	};

protected:
	virtual volatile_buffer
	_handle_merge(const volatile_buffer &key,
		      const volatile_buffer &new_value,
		      const buffer_view &orig_value)
	{
		// default implementation performs no merge
		return new_value;
	}

public:
	class batch {
		friend class DB;
		enum Op { SET, SET_PMEM, REMOVE, REMOVE_PREFIX, REMOVE_RANGE, MERGE };
		std::vector<std::tuple<Op, volatile_buffer, volatile_buffer>>
			ops;

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
                        //FIXME minor: check for alloc errors
			ceph_assert(res.second);
			v.copy_out(0, v.length(), res.second[0].data()); //FIXME: adjust by one byte to mark uncommitted pages
                        return res;
		}
		static pmem_kv_entry2_ptr
		allocate_pmem_kv_and_copyin(pmem::obj::pool_base &pool,
			                 const volatile_buffer &k,
					 const volatile_buffer &v)
		{
                        pmem_kv_entry2_ptr res;
			res = pmem_kv_entry2::allocate_atomic_volatile(pool, k, v);
			return res;
		}

	public:
		~batch()
		{
                        reset();
		}
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
		set_allocate_pmem(pmem::obj::pool_base &pool,
                                  const volatile_buffer &key,
				  const volatile_buffer &val)
		{
			auto pp = allocate_pmem_kv_and_copyin(pool, key, val);
			ops.emplace_back(SET_PMEM, key, pp);
		}
		void
		set_allocate_pmem(pmem::obj::pool_base &pool,
				  volatile_buffer &&key,
				  const volatile_buffer &val)
		{
			auto pp = allocate_pmem_kv_and_copyin(pool, key, val);
			ops.emplace_back(SET_PMEM, key, pp);
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
				if (std::get<0>(o) == SET_PMEM) {
					pmem_kv_entry2_ptr pp =
						std::get<2>(o)
							.take_pmem_kv_entry_away();
					if (pp) {
                                                // delete_persistent_atomic does 
                                                // exactly the same call to pmemobj_free.
                                                // Hence using it directly
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
		iterator(DB &_kv, bool last = false) : kv(&_kv), seq(_kv.rm_seq)
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
				}
			}
		}
		iterator(DB &_kv, const volatile_buffer &key, Mode m)
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
				cur_key = it->kv_pair[0].key();
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
					cur_key = it->kv_pair[0].key();
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
					volatile_buffer(
						string_to_view(cur_key)),
					Compare());
				seq = kv->rm_seq;
			}
			if (it != kv->kv_set.begin()) {
				--it;
			}
			if (it != kv->kv_set.end()) {
				cur_key = it->kv_pair[0].key();
			} else {
				cur_key.clear();
			}
			return *this;
		}
		const entry_ptr operator->()
		{
			std::shared_lock l(kv->general_mutex);
			if (seq != kv->rm_seq && !cur_key.empty()) {
				it = kv->kv_set.lower_bound(
					volatile_buffer(
						string_to_view(cur_key)),
					Compare());
				seq = kv->rm_seq;
				if (it != kv->kv_set.end()) {
					cur_key = it->kv_pair[0].key();
				} else {
					cur_key.clear();
				}
			}
			return cur_key.empty() ? entry_ptr()
					       : it->kv_pair;
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
				if (seq != kv->rm_seq) {
					it = kv->kv_set.lower_bound(
						volatile_buffer(string_to_view(
							cur_key)),
						Compare());
					seq = kv->rm_seq;
					if (it != kv->kv_set.end()) {
						cur_key = it->kv_pair[0].key();
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

	template <class T>
	size_t
	for_each(pmem::obj::pool_base &pool,
		 std::function<void(pmemoid&)> fn = nullptr)
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
		//std::lock_guard l(general_mutex);
		size_t entries = 0;
		for_each<pmem_kv_entry2>(pool, [&](pmemoid& o) {
			++entries;
			if ((entries % 100000) == 0) {
				std::cout << "Loading " << entries << std::endl;
			}
			entry_ptr kv_ptr(
				(entry *)pmemobj_direct(
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
/*		auto entry_type_num = pmem::detail::type_num<entry>();
		auto o = POBJ_FIRST_TYPE_NUM(pool.handle(), entry_type_num);
		while (!OID_IS_NULL(o)) {
			++entries;
			if ((entries % 100000) == 0) {
				std::cout << "Loading " << entries << std::endl;
			}

			entry_ptr kv_ptr(
				(entry *)pmemobj_direct(
					o)); // FIXME minor: in fact we just
					     // need the ability to create
					     // persistent_ptr from PMEMoid not
					     // from void*
			kv_set.insert(*new volatile_map_entry(kv_ptr));

			o = POBJ_NEXT_TYPE_NUM(o);
		}*/
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
		auto t0 = mono_clock::now();
		std::lock_guard l(general_mutex);
		LOG_TIME(LOCK_TIME);
		t0 = mono_clock::now();
		pmem::obj::
			transaction::
				run(pool,
				    [&] {
					    LOG_TIME(START_TIME);
					    for (auto &p : b.ops) {
						    auto &key = std::get<1>(p);
						    /*std::cerr << key << " " <<
						       std::get<0>(p)
							      << std::endl;*/
						    switch (std::get<0>(p)) {
							    case batch::SET: {
								    t0 = mono_clock::
									    now();
								    kv_set_t::insert_commit_data
									    commit_data;
								    auto ip = kv_set.insert_check(
									    key,
									    Compare(),
									    commit_data);
								    LOG_TIME(
									    SET_LOOKUP_TIME);
								    t0 = mono_clock::
									    now();
								    auto &val = std::get<
									    2>(
									    p);
								    if (!ip.second) {
                                                                            int path = 0;
									    if (!ip.first->kv_pair[0]
											    .try_assign_value(
												 val)) {
										    entry::release(
											    ip.first->kv_pair);
										    ip.first->kv_pair =
											    entry::allocate(key, val);
                                                                                    path = 1;
									    }

									    LOG_TIME(ApplyBatchTimes(
										    SET_EXISTED0_TIME +
										    path));
								    } else {
									    auto kv = entry::allocate(
                                                                                    key, val);
									    LOG_TIME(SET_MAKE_NEW_PERSISTENT_TIME);
									    auto t0 = mono_clock::
										    now();
									    kv_set.insert_commit(
										    *new volatile_map_entry(
											    kv),
										    commit_data);
									    LOG_TIME(SET_INSERT_TIME);
								    }
								    LOG_TIME(SET_EXEC_TIME);
								    break;
							    }
							    case batch::SET_PMEM: {
								    t0 = mono_clock::
									    now();
								    kv_set_t::insert_commit_data
									    commit_data;
								    auto ip = kv_set.insert_check(
									    key,
									    Compare(),
									    commit_data);
								    LOG_TIME(
									    SET_LOOKUP_TIME);
								    t0 = mono_clock::
									    now();
								    auto &val = std::get<
									    2>(
									    p);
								    pmem_kv_entry2_ptr kv =
									    val.take_pmem_kv_entry_away();
								    ceph_assert(kv != nullptr);
								    kv[0].persist();
								    if (!ip.second) {
									    entry::release(
										    ip.first->kv_pair);
									    ip.first->kv_pair = kv;

									    LOG_TIME(ApplyBatchTimes(
										    SET_EXISTED0_TIME));
								    } else {
									    LOG_TIME(SET_MAKE_NEW_PERSISTENT_TIME);
									    auto t0 = mono_clock::
										    now();
									    kv_set.insert_commit(
										    *new volatile_map_entry(
											    kv),
										    commit_data);
									    LOG_TIME(SET_INSERT_TIME);
								    }
								    LOG_TIME(SET_EXEC_TIME);
								    break;
							    }
							    case batch::
								    REMOVE: {
								    t0 = mono_clock::
									    now();
								    auto it = kv_set.find(
									    key,
									    Compare());
								    LOG_TIME(
									    REMOVE_LOOKUP_TIME);
								    t0 = mono_clock::
									    now();
								    if (it !=
									kv_set.end()) {
									    entry::release(it->kv_pair);
									    kv_set.erase_and_dispose(
										    it,
										    Dispose());
									    ++rm_seq;
								    }
								    LOG_TIME(
									    REMOVE_EXEC_TIME);
								    break;
							    }
							    case batch::
								    REMOVE_PREFIX: {
								    t0 = mono_clock::
									    now();
								    auto it = kv_set.lower_bound(
									    key,
									    Compare());

								    LOG_TIME(
									    REMOVE_LOOKUP_TIME);
								    t0 = mono_clock::
									    now();
								    bool any_remove =
									    false;
								    while (it != kv_set.end() &&
									   key.is_prefix_for(
										   it->kv_pair[0]
											   .key_view())) {
									    entry::release(it->kv_pair);
									    it = kv_set.erase_and_dispose(
										    it,
										    Dispose());
									    any_remove =
										    true;
								    }
								    if (any_remove) {
									    ++rm_seq;
								    }
								    LOG_TIME(
									    REMOVE_EXEC_TIME);
								    break;
							    }
							    case batch::
								    REMOVE_RANGE: {
								    t0 = mono_clock::
									    now();
								    auto &key_end = std::
									    get<2>(p);
								    auto it = kv_set.lower_bound(
									    key,
									    Compare());
								    LOG_TIME(
									    REMOVE_LOOKUP_TIME);
								    t0 = mono_clock::
									    now();
								    bool any_remove =
									    false;
								    while (it != kv_set.end() &&
									   key_end >
										   it->kv_pair[0]
											   .key_view()) {
									    entry::release(
										    it->kv_pair);
									    it = kv_set.erase_and_dispose(
										    it,
										    Dispose());
									    any_remove =
										    true;
								    }
								    if (any_remove) {
									    ++rm_seq;
								    }
								    LOG_TIME(
									    REMOVE_EXEC_TIME);
								    break;
							    }
							    case batch::MERGE: {
								    t0 = mono_clock::
									    now();
								    kv_set_t::insert_commit_data
									    commit_data;
								    auto ip = kv_set.insert_check(
									    key,
									    Compare(),
									    commit_data);
								    LOG_TIME(
									    MERGE_LOOKUP_TIME);
								    t0 = mono_clock::
									    now();
								    auto &val = std::get<
									    2>(
									    p);
								    bool present =
									    !ip.second;
								    buffer_view
									    orig_bv;
								    if (present) {
									    orig_bv =
										    ip.first->kv_pair[0]
											    .value_view();
								    }
								    volatile_buffer new_v = _handle_merge(
									    key,
									    val,
									    orig_bv);

								    if (!present) {
									    auto v = entry::allocate(key, new_v);
									    kv_set.insert_commit(
										    *new volatile_map_entry(
											    v),
										    commit_data);
								    } else if (!ip.first->kv_pair[0]
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
		return iterator(*this, key, iterator::FIND);
	}

	void test(pmem::obj::pool_base &pool, bool remove = true);
	void test2(pmem::obj::pool_base &pool);

}; // class DB
}; // namespace pmem_kv
