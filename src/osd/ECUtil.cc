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

void ECUtil::CompressContext::BlockInfo::encode(bufferlist &bl) const
{
        ENCODE_START(1, 1, bl);
        ::encode(method, bl);
        //::encode(original_size, bl);
        ::encode(target_offset, bl);
        ENCODE_FINISH(bl);
}

void ECUtil::CompressContext::BlockInfo::decode(bufferlist::iterator &bl)
{
        DECODE_START(1, bl);
        ::decode(method, bl);
        //::decode(original_size, bl);
        ::decode(target_offset, bl);
        DECODE_FINISH(bl);
}

void ECUtil::CompressContext::setup_for_append(const bufferlist& master_record_val)
{
        clear();
        ::decode(
                current_original_pos,
                master_record_val);
        ::decode(
                current_compressed_pos,
                master_record_val);
}

void ECUtil::CompressContext::setup_for_read(const map<string, bufferlist>& attr, uint64_t start_offset, uint64_t end_offset)
{
        clear();
        string key_prefix = ECUtil::get_cinfo_key();
        map<string, bufferlist>::const_iterator it = attrset.begin();
        map<string, bufferlist>::const_iterator end = attrset.end()

        if (it != attrset.end()) {
                ::decode(
                        current_original_pos,
                        it->second);
                ::decode(
                        current_compressed_pos,
                        it->second);
                ++it;
                bool done = false;

                uint64_t start_found_offset;
                map<string, bufferlist>::iterator start_it=end;
                while ( it < end && it->first.size() > key_prefix.size() && it->first.compare(0, key_prefix.size(), key_prefix) == 0 ) {
                        uint64_t rec_offset = strtoull(it->first.c_str() + key_prefix.size(), NULL, 16);
                        if (rec_offset <= start_offset) {
                                start_found_offset = rec_offset;
                                start_it = it;
                        }
                        if (rec_offset <= end_offset) {
                                if (rec_offset > start_offset) {
                                        bufferlist::iterator bp = it->second.begin();
                                        ::decode(
                                                blocks[rec_offset],
                                                bp);
                                }
                                ++it;
                        }
                        else
                                it = end;
                }

                if (start_it != end){
                        bufferlist::iterator bp = start_it->second.begin();
                        ::decode(
                                blocks[start_found_offset],
                                bp);
                }
        }
}

void ECUtil::CompressContext::flush( map<string, boost::optional<bufferlist> > & attrset)
{
        for (ECUtil::CompressContext::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
                stringstream ss_key;
                ss_key << ECUtil::get_cinfo_key() << std::hex << it->first;

                ::encode(
                        it->second,
                        attrset[ss_key.str()].get());
        }
        blocks.clear();

        ::encode(
                current_original_pos,
                attrset[ECUtil::get_cinfo_key()].get());

        ::encode(
                current_compressed_pos,
                attrset[ECUtil::get_cinfo_key()].get());
}

void ECUtil::CompressContext::dump(Formatter *f) const
{
        f->dump_unsigned("current_original_pos", current_original_pos);
        f->dump_unsigned("current_compressed_pos", current_compressed_pos);
        f->open_object_section("blocks");
        for (CompressContext::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
                f->open_object_section("block");
                f->dump_unsigned("offset", it->first);
                f->dump_string("method", it->second.method);
                f->dump_unsigned("target_offset", it->second.target_offset);
                f->close_section();
        }
        f->close_section();
}

void ECUtil::CompressContext::append_block(uint64_t original_offset,
                  uint64_t original_size, 
                  const string& method, 
                  uint64_t new_block_size )
{
        blocks[original_offset] = BlockInfo(method, current_compressed_pos);

        current_original_pos += original_size;

        current_compressed_pos += new_block_size;
}

pair<uint64_t, uint64_t> ECUtil::CompressContext::offset_len_to_compressed_block(const pair<uint64_t, uint64_t> offs_len_pair) const
{
        uint64_t start_offs = offs_len_pair.first;
        uint64_t end_offs = offs_len_pair.first + offs_len_pair.second-1;
        assert(current_original_pos == 0 || start_offs < current_offset_pos);
        assert(current_original_pos == 0 || end_offs < current_offset_pos);

        uint64_t res_start_offs = current_original_pos != 0 ? map_offset(start_offs, false).second : start_offs;
        uint64_t res_end_offs = current_original_pos != 0 ? map_offset(end_offs, true).second : end_offs;

        return make_pair<uint64_t, uint64_t>(res_start_offs, res_end_offs - res_start_offs);
}

pair<uint64_t, uint64_t> ECUtil::CompressionInfo::map_offset(uint64_t offs, bool next_block_flag) const
{
        pair<uint64_t, uint64_t> res; //original block offset, compressed block offset
        BlockMap::iterator it;
        if (next_block_flag){
                it = std::upper_end(blocks.begin(), blocks.end(), offs);
                if (it != blocks.end())
                        res = make_pair(it->first, it->second.target_offset);
                else
                        res = make_pair(current_original_pos, current_compressed_pos);
        }
        else {
                it = std::lower_end(blocks.begin(), blocks.end(), offs);
                if (it == blocks.end() || *it != offs){ //lower_end returns iterator to the specified value if present or the next entry
                        assert(it != blocks.begin()); 
                        --it; 
                }
                res = make_pair(it->first, it->second.target_offset);
        }
        return res;
}

int CompressContext::try_decompress(CompressionInterfaceRef cs_impl, const bufferlist& cs_bl, uint64_t orig_offs, uint64_t len, bufferlist& res_bl) const
{
        int res = 0;
        uint64_t appended = 0;
        if (current_original_pos == 0) //no compression applied was to the object
        {
                dout(10) << __func__ ::"ifed: bypass due to no compression: (" << orig_offs << ", " << len << ")" << dendl;
                res_bl.append(cs_bl);
                appended += cs_bl.length();
        }
        else
        {
                dout(10) << __func__ ::"ifed: (" << orig_offs << ", " << len << ")" << dendl;
                assert(current_original_pos >= orig_offs + len);
                assert(blocks.size() > 0);

                uint64_t cur_block_offs, cur_block_len, cur_block_coffs, cur_block_clen, cur_block_method;
                BlockMap iterator it = blocks.begin();

                uint64_t cs_bl_pos = 0;

                uint64_t cur_compressed_pos;
                do{
                        cur_block_offs = it->first;
                        cur_block_coffs = it->second.target_offset;
                        cur_block_method = it->second.method;
                        ++it;
                        if (it == blocks.end())
                        {
                                cur_block_len = current_original_pos - cur_block_offs;
                                cur_block_clen = current_compressed_pos - cur_block_coffs;
                        }
                        else
                        {
                                cur_block_len = it->first - cur_block_offs;
                                cur_block_clen = it->second.target - cur_block_coffs;
                        }
                        if (cur_block_offs + len >= orig_offs) //skipping map entries for data ranges prior to the desired one.
                        {
                                bufferlist cs_bl1, tmp_bl;
                                if (cur_block_method.empty()) //block not compressed, doing data copy directly
                                {
                                        uint64_t offs2splice = cur_block_offs < orig_offs ? orig_offs - cur_block_offs : 0;
                                        offs2splice += cs_bl_pos;

                                        uint64_t len2splice = cur_block_offs + cur_block_len > orig_offs + len ? cur_block_offs + cur_block_len - orig_offs - len : cur_block_len;
                                        len2splice = MIN(len2splice, cur_block_len);

                                        dout(10) << __func__ ::"ifed: decompressed: (" << cur_block_offs << "," << cur_block_len << "," << offs2splice << "," << len2splice << "," << cs_bl.length() << ")" << dendl;
                                        if (off2splice == 0 && len2splice == cs_bl.length()) //current block is completely within the requested range
                                                res_bl.append(cs_bl);
                                        else
                                                cs_bl.splice(offs2splice, len2splice, &res_bl);
                                        appended += len2splice;
                                }
                                else {
                                        cs_bl1.substr_of(cs_bl, cs_bl_pos, MIN(cs_bl.length() - cs_bl_pos, cur_block_clen));
                                        int r = ECUtil::decompress(
                                                cur_block_len,
                                                cs_impl,
                                                cs_bl1,
                                                &tmp_bl)) //FIXME: apply decompression method depending on the indicated one
                                        if (r< 0)
                                        {
                                                dout(0) << __func__ << ": decompress(" << cur_block_method << ")"
                                                        << " returned an error: "<< r << dendl;
                                                res = -1;
                                        }
                                        else{
                                                uint64_t offs2splice = cur_block_offs < orig_offs ? orig_offs - cur_block_offs : 0;
                                                uint64_t len2splice = cur_block_offs + cur_block_len > orig_offs + len ? cur_block_offs + cur_block_len - orig_offs - len : tmp_bl.length();
                                                len2splice = MIN(len2splice, tmp_bl.length());

                                                dout(10) << __func__ ::"ifed: decompressed: (" << cur_block_offs << "," << cur_block_len << "," << offs2splice << "," << len2splice << "," << tmp_bl.length() << ")" << dendl;

                                                if (off2splice == 0 && len2splice == tmp_bl.length()) //decompressed block is completely within the requested range
                                                        res_bl.append(tmp_bl);
                                                else
                                                        tmp_bl.splice(offs2splice, len2splice, &res_bl);
                                                appended += len2splice;
                                        }
                                }
                        }

                        cs_bl_pos += cur_block_clen;
                } while (it != blocks.end() && appended < len && res == 0);
        }
        assert(r != 0 || (r == 0 && appended == len));
        return res;
}


int CompressContext::do_compress(CompressionInterfaceRef cs_impl, const ECUtil::stripe_info_t& sinfo, const bufferlist& block2compress, stdstring& res_method, bufferlist& result_bl)
{
        bufferlist tmp_bl;
        int r = ECUtil::compress(cs_impl, block2compress, &tmp_bl);
        if (r == 0) {
                if (sinfo.pad_to_stripe_width(tmp_bl.length()) < sinfo.pad_to_stripe_width(block2compress.length())) {
                        // align
                        if (tmp_bl.length() % sinfo.get_stripe_width())
                                tmp_bl.append_zero(
                                sinfo.get_stripe_width() -
                                tmp_bl.length() % sinfo.get_stripe_width());

                        res_method = cs_impl->get_profile().at("plugin"); //FIXME: add a method to access compression method directly
                        result_bl.append(tmp_bl);
                }
                else {
                        res_method.clear();
                        result_bl.append(block2compress);
                }
        }
        return res;
}

void CompressContext::try_compress(const hobject_t& oid, uint64_t off, const bufferlist& bl, CompressionInterfaceRef cs_impl, const ECUtil::stripe_info_t& sinfo, bufferlist& res_bl)
{
        bufferlist bl_cs(bl);

        bool compressed = false, failure = false;
        //apply compression if that's a first block or it's been already applied to previous blocks
        if (can_compress(off) && cs_impl!=NULL) {
                bufferlist bl;
                ECUtil::CompressContext new_cinfo(*this);
                if (bl_cs.length() > sinfo.get_stripe_width()) {

                        bufferlist::iterator it = bl_cs.begin();

                        uint64_t cur_offs = off;
                        while (it != bl_cs.end() && !failure){
                                uint64_t block_size = cs_impl->get_block_size(sinfo.get_stripe_width());
                                bufferlist block2compress, compressed_block;
                                uint64_t prev_len = bl.length();
                                it.copy(block_size, block2compress); //FIXME: check for other options instead of copy!!!!

                                int r0 = do_compress(block2compress, cmethod, bl);
                                if (r0 != 0) {
                                        dout(0) << "block compression failed, left uncompressed, oid=" << oid << dendl;
                                        failure = true;
                                }
                                else{
                                        new_cinfo.append_block(cur_offs, block2compress.length(), cmethod, bl.length() - prev_length());
                                }
                                cur_offs += block2compress.size();
                        }
                        if (!failure && sinfo.pad_to_stripe_width(bl.length()) < sinfo.pad_to_stripe_width(bl_cs.length())) {
                                //There is some benefit from compression after data alignment
                                compressed = true;
                                dout(10) << "block compressed, oid=" << oid << ", ratio = " << (float)bl_cs.length() / bl.length() << dendl;
                        }
                        else if (failure)
                                dout(10) << "block compression bypassed due to failure, oid=" << oid << dendl;
                        else
                                dout(10) << "block compression bypassed, oid=" << oid << dendl;

                }
                else
                        dout(10) << "block compression bypassed, block is too small, oid=" << oid << dendl;

        }
        if (!compressed) //the whole block wasn't compressed or there is no benefit in compression
        {
                res_bl = bl;
                append_block(off, res_bl.length(), "", res_bl.length());
        }
        else
        {
                bl.swap(res_bl);
                swap(new_cinfo);
        }
}

const string HINFO_KEY = "hinfo_key";
const string CINFO_KEY = "@ci@";

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
