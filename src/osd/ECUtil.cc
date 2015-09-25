// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <errno.h>
#include "include/encoding.h"
#include "ECUtil.h"

int ECUtil::decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  bufferlist *out) {

  uint64_t total_chunk_size = to_decode.begin()->second.length();

  assert(to_decode.size());
  assert(total_chunk_size % sinfo.get_chunk_size() == 0);
  assert(out);
  assert(out->length() == 0);

  for (map<int, bufferlist>::iterator i = to_decode.begin();
       i != to_decode.end();
       ++i) {
    assert(i->second.length() == total_chunk_size);
  }

  if (total_chunk_size == 0)
    return 0;

  for (uint64_t i = 0; i < total_chunk_size; i += sinfo.get_chunk_size()) {
    map<int, bufferlist> chunks;
    for (map<int, bufferlist>::iterator j = to_decode.begin();
	 j != to_decode.end();
	 ++j) {
      chunks[j->first].substr_of(j->second, i, sinfo.get_chunk_size());
    }
    bufferlist bl;
    int r = ec_impl->decode_concat(chunks, &bl);
    assert(bl.length() == sinfo.get_stripe_width());
    assert(r == 0);
    out->claim_append(bl);
  }
  return 0;
}

int ECUtil::decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  map<int, bufferlist*> &out) {

  uint64_t total_chunk_size = to_decode.begin()->second.length();

  assert(to_decode.size());
  assert(total_chunk_size % sinfo.get_chunk_size() == 0);

  for (map<int, bufferlist>::iterator i = to_decode.begin();
       i != to_decode.end();
       ++i) {
    assert(i->second.length() == total_chunk_size);
  }

  if (total_chunk_size == 0)
    return 0;

  set<int> need;
  for (map<int, bufferlist*>::iterator i = out.begin();
       i != out.end();
       ++i) {
    assert(i->second);
    assert(i->second->length() == 0);
    need.insert(i->first);
  }

  for (uint64_t i = 0; i < total_chunk_size; i += sinfo.get_chunk_size()) {
    map<int, bufferlist> chunks;
    for (map<int, bufferlist>::iterator j = to_decode.begin();
	 j != to_decode.end();
	 ++j) {
      chunks[j->first].substr_of(j->second, i, sinfo.get_chunk_size());
    }
    map<int, bufferlist> out_bls;
    int r = ec_impl->decode(need, chunks, &out_bls);
    assert(r == 0);
    for (map<int, bufferlist*>::iterator j = out.begin();
	 j != out.end();
	 ++j) {
      assert(out_bls.count(j->first));
      assert(out_bls[j->first].length() == sinfo.get_chunk_size());
      j->second->claim_append(out_bls[j->first]);
    }
  }
  for (map<int, bufferlist*>::iterator i = out.begin();
       i != out.end();
       ++i) {
    assert(i->second->length() == total_chunk_size);
  }
  return 0;
}


int ECUtil::decompress(
  CompressionInterfaceRef &cs_impl,
  int original_size,
  bufferlist &in,
  bufferlist *out)
{
  int r = cs_impl->decode(in, original_size, out);
  assert(r == 0);
  return 0;
}

int ECUtil::compress(
  CompressionInterfaceRef &cs_impl,
  bufferlist &in,
  bufferlist *out)
{
  int r = cs_impl->encode(in, out);
  assert(r == 0);
  return 0;
}

int ECUtil::encode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  bufferlist &in,
  const set<int> &want,
  map<int, bufferlist> *out) {

  uint64_t logical_size = in.length();

  assert(logical_size % sinfo.get_stripe_width() == 0);
  assert(out);
  assert(out->empty());

  if (logical_size == 0)
    return 0;

  for (uint64_t i = 0; i < logical_size; i += sinfo.get_stripe_width()) {
    map<int, bufferlist> encoded;
    bufferlist buf;
    buf.substr_of(in, i, sinfo.get_stripe_width());
    int r = ec_impl->encode(want, buf, &encoded);
    assert(r == 0);
    for (map<int, bufferlist>::iterator i = encoded.begin();
	 i != encoded.end();
	 ++i) {
      assert(i->second.length() == sinfo.get_chunk_size());
      (*out)[i->first].claim_append(i->second);
    }
  }

  for (map<int, bufferlist>::iterator i = out->begin();
       i != out->end();
       ++i) {
    assert(i->second.length() % sinfo.get_chunk_size() == 0);
    assert(
      sinfo.aligned_chunk_offset_to_logical_offset(i->second.length()) ==
      logical_size);
  }
  return 0;
}

void ECUtil::HashInfo::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(total_chunk_size, bl);
  ::encode(cumulative_shard_hashes, bl);
  ENCODE_FINISH(bl);
}

void ECUtil::HashInfo::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(total_chunk_size, bl);
  ::decode(cumulative_shard_hashes, bl);
  DECODE_FINISH(bl);
}

void ECUtil::HashInfo::dump(Formatter *f) const
{
  f->dump_unsigned("total_chunk_size", total_chunk_size);
  f->open_object_section("cumulative_shard_hashes");
  for (unsigned i = 0; i != cumulative_shard_hashes.size(); ++i) {
    f->open_object_section("hash");
    f->dump_unsigned("shard", i);
    f->dump_unsigned("hash", cumulative_shard_hashes[i]);
    f->close_section();
  }
  f->close_section();
}

void ECUtil::HashInfo::generate_test_instances(list<HashInfo*>& o)
{
  o.push_back(new HashInfo(3));
  {
    bufferlist bl;
    bl.append_zero(20);
    map<int, bufferlist> buffers;
    buffers[0] = bl;
    buffers[1] = bl;
    buffers[2] = bl;
    o.back()->append(0, buffers);
    o.back()->append(20, buffers);
  }
  o.push_back(new HashInfo(4));
}

void ECUtil::CompressInfo::BlockInfo::encode(bufferlist &bl) const
{
        ENCODE_START(1, 1, bl);
        ::encode(method, bl);
        ::encode(original_size, bl);
        ::encode(target_offset, bl);
        ENCODE_FINISH(bl);
}

void ECUtil::CompressInfo::BlockInfo::decode(bufferlist::iterator &bl)
{
        DECODE_START(1, bl);
        ::decode(method, bl);
        ::decode(original_size, bl);
        ::decode(target_offset, bl);
        DECODE_FINISH(bl);
}

int ECUtil::CompressInfo::setup(map<string, bufferlist>& attrset)
{
        int ret = 0;
        clear();
        try{
                bool root_key_found = false, some_compression_keys_found = false;
                string key_prefix = ECUtil::get_cinfo_key();
                key_prefix += '_';
                map<string, bufferlist>::iterator it = attrset.begin();
                while (it != attrset.end())
                {
                        int pos = it->first.find(key_prefix);
                        if (pos == 0)
                        {
                                string str_offs = it->first;
                                stringstream ss0(it->first);
                                ss0.seekg(key_prefix.size());
                                uint64_t original_offset;
                                ss0 >> std::hex >> original_offset;

                                bufferlist::iterator bp=it->second.begin();
                                ::decode(
                                        blocks[original_offset],
                                        bp);
                                some_compression_keys_found = true;
                        }
                        else if (it->first == ECUtil::get_cinfo_key())
                        {
                                ::decode(
                                        next_target_offset,
                                        it->second);
                                root_key_found = true;
                        }
                        ++it;
                }
                if (!root_key_found && some_compression_keys_found)
                        ret = -1;
        }
        catch (...){
                ret = -1;
        }
        if (ret != 0)
                clear();
	return ret;
}

void ECUtil::CompressInfo::flush( map<string, boost::optional<bufferlist> > & attrset) const
{
        for (ECUtil::CompressInfo::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
                stringstream ss_key;
                ss_key << ECUtil::get_cinfo_key() << "_" << std::hex << it->first;

                ::encode(
                        it->second,
                        attrset[ss_key.str()].get());
        }

        ::encode(
                next_target_offset,
                attrset[ECUtil::get_cinfo_key()].get());
}

void ECUtil::CompressInfo::dump(Formatter *f) const
{
        f->dump_unsigned("next_target_offset", next_target_offset);
        f->open_object_section("blocks");
        for (CompressInfo::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
                f->open_object_section("block");
                f->dump_unsigned("offset", it->first);
                f->dump_string("method", it->second.method);
                f->dump_unsigned("original_size", it->second.original_size);
                f->dump_unsigned("target_offset", it->second.target_offset);
                f->close_section();
        }
        f->close_section();
}

void ECUtil::CompressInfo::append_block(uint64_t original_offset,
                  uint64_t original_size, 
                  const string& method, 
                  uint64_t new_block_size, 
                  map<string, 
                  bufferlist>& attrset)
{
        BlockInfo bi(method, original_size, next_target_offset);
        blocks[original_offset] = bi;

        //saving compression info for the appended block
        stringstream ss_key;
        ss_key << ECUtil::get_cinfo_key() << "_" << std::hex << original_offset;

        ::encode(
                bi,
                attrset[ss_key.str()]);
        
     
        //updating compressed data block length
        next_target_offset += new_block_size;
        ::encode(
                next_target_offset,
                attrset[ECUtil::get_cinfo_key()]);
}

const string HINFO_KEY = "hinfo_key";
const string CINFO_KEY = "cinfo_key";

bool ECUtil::is_internal_key_string(const string &key)
{
  return key == HINFO_KEY || key.find(CINFO_KEY)==0;
}

const string &ECUtil::get_hinfo_key()
{
  return HINFO_KEY;
}

const string &ECUtil::get_cinfo_key()
{
  return CINFO_KEY;
}
