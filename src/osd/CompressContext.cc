// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <errno.h>
#include "include/encoding.h"
#include "ECUtil.h"
#include "CompressContext.h"
#include "compressor/Compressor.h"
#include "compressor/CompressionPlugin.h"
#include "common/debug.h"


#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix *_dout

void CompressContext::BlockInfoRecord::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(method_idx, bl);
  ::encode(original_length, bl);
  ::encode(compressed_length, bl);
  ENCODE_FINISH(bl);
}

void CompressContext::BlockInfoRecord::decode(bufferlist::iterator& bl) {
  DECODE_START(1, bl);
  ::decode(method_idx, bl);
  ::decode(original_length, bl);
  ::decode(compressed_length, bl);
  DECODE_FINISH(bl);
}

void CompressContext::BlockInfoRecordSetHeader::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(start_offset, bl);
  ::encode(compressed_offset, bl);
  ENCODE_FINISH(bl);
}

void CompressContext::BlockInfoRecordSetHeader::decode(bufferlist::iterator& bl) {
  DECODE_START(1, bl);
  ::decode(start_offset, bl);
  ::decode(compressed_offset, bl);
  DECODE_FINISH(bl);
}

uint8_t CompressContext::MasterRecord::add_get_method(const string& method) {
  uint8_t res = 0;
  if (!method.empty()) {
    for (size_t i = 0; i < methods.size() && res == 0; i++) {
      if (methods[i] == method) {
	res = i + 1;
      }
    }
    if (res == 0) {
      methods.push_back(method);
      res = methods.size();
    }
  }
  return res;
}

string CompressContext::MasterRecord::get_method_name(uint8_t index) const {
  string res;
  if (index > 0) {
    if (index <= methods.size()) {
      res = methods[index - 1];
    } else {
      res = "???";
    }
  }
  return res;
}

void CompressContext::MasterRecord::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(current_original_pos, bl);
  ::encode(current_compressed_pos, bl);
  ::encode(block_info_record_length, bl);
  ::encode(block_info_recordset_header_length, bl);
  ::encode(methods, bl);
  ENCODE_FINISH(bl);
}


void CompressContext::MasterRecord::decode(bufferlist::iterator& bl) {
  DECODE_START(1, bl);
  ::decode(current_original_pos, bl);
  ::decode(current_compressed_pos, bl);
  ::decode(block_info_record_length, bl);
  ::decode(block_info_recordset_header_length, bl);
  ::decode(methods, bl);
  DECODE_FINISH(bl);
}

CompressContext::~CompressContext() {
}

bool  CompressContext::can_compress(uint64_t offs) const {
  return (offs == 0 && masterRec.current_compressed_pos == 0) || masterRec.current_compressed_pos != 0;
}

uint64_t CompressContext::get_block_size(uint64_t stripe_width) const {
  return MAX_STRIPES_PER_BLOCK * stripe_width;
}

void CompressContext::setup_for_append_or_recovery(const map<string, bufferlist>& attrset) {
  clear();

  map<string, bufferlist>::const_iterator it_attrs = attrset.find(ECUtil::get_cinfo_master_key());
  if (it_attrs != attrset.end()) {
    bufferlist::iterator it = const_cast<bufferlist&>(it_attrs->second).begin();
    ::decode(masterRec, it);
    prevMasterRec = masterRec;
  }

  it_attrs = attrset.find(ECUtil::get_cinfo_key());
  if (it_attrs != attrset.end()) {
    prev_blocks_encoded = it_attrs->second;
  }
}

void CompressContext::setup_for_read(const map<string, bufferlist>& attrset, uint64_t start_offset, uint64_t end_offset) {
  assert(end_offset >= start_offset);

  clear();
  map<string, bufferlist>::const_iterator it = attrset.find(ECUtil::get_cinfo_master_key());
  map<string, bufferlist>::const_iterator end = attrset.end();
  if (it != end) {
    bufferlist::iterator it_val = const_cast<bufferlist&>(it->second).begin();
    ::decode(masterRec, it_val);

    assert(end_offset <= masterRec.current_original_pos);
    dout(1) << __func__ << masterRec.current_original_pos << "," << masterRec.current_compressed_pos << "," << start_offset << "," << end_offset << dendl;

    it = attrset.find(ECUtil::get_cinfo_key());
    if (it != end) {
      size_t record_set_size = masterRec.block_info_record_length * RECS_PER_RECORDSET + masterRec.block_info_recordset_header_length;
      assert(record_set_size != 0);

      bufferlist::iterator it_bl = const_cast<bufferlist&>(it->second).begin();
      BlockInfoRecordSetHeader recset_header(0, 0);
      {
	//searching for the record set that contains desired data range
	::decode(recset_header, it_bl);

	BlockInfoRecordSetHeader recset_header_next(0, 0);
	bufferlist::iterator it_bl_next = it_bl;

	bool found = false;
	do {
	  if (it_bl_next.get_remaining() >= record_set_size) {
	    it_bl_next.advance(record_set_size - masterRec.block_info_recordset_header_length);
	    ::decode(recset_header_next, it_bl_next);
	    found = recset_header_next.start_offset > start_offset;
	    if (!found) {
	      it_bl = it_bl_next;
	      recset_header = recset_header_next;
	    }
	  } else {
	    found = true;
	  }
	} while (!found);
      }

      uint64_t cur_pos = recset_header.start_offset;
      uint64_t cur_cpos = recset_header.compressed_offset;
      dout(1) << __func__ << " ifed:found recordset:" << cur_pos << "." << cur_cpos << dendl;
      uint64_t found_start_offset = 0;
      BlockInfo found_bi;
      bool start_found = false;
      bool stop = false;

      while (!stop && !it_bl.end()) {
	if ((it_bl.get_off() % record_set_size) == 0) {
	  ::decode(recset_header, it_bl);
	}
	BlockInfoRecord rec;
	::decode(rec, it_bl);

	if (cur_pos <= start_offset && (!start_found || cur_pos > found_start_offset)) {
	  found_start_offset = cur_pos;
	  found_bi = BlockInfo(rec.method_idx, cur_cpos);
	  start_found = true;
	}
	if (cur_pos > start_offset) {
	  blocks[cur_pos] = BlockInfo(rec.method_idx, cur_cpos);
	  if (cur_pos >= end_offset) {
	    stop = true;
	  }
	  dout(1) << __func__ << " ifed:" << cur_pos << "=(" << (int)rec.method_idx << "," << cur_cpos << ")" << dendl;
	}

	cur_pos += rec.original_length;
	cur_cpos += rec.compressed_length;
	assert(cur_pos <= masterRec.current_original_pos);
	assert(cur_cpos <= masterRec.current_compressed_pos);
      }

      if (start_found) {
	blocks[found_start_offset] = found_bi;
	dout(1) << __func__ << " ifed:" << found_start_offset << "==(" << (int)found_bi.method_idx << "," << found_bi.target_offset << ")" << dendl;
      }
    }
  }
}

void CompressContext::flush(map<string, bufferlist>* attrset) {
  assert(attrset);
  if (need_flush()) { //some changes have been made
    bufferlist bl(prev_blocks_encoded);

    size_t record_set_size = masterRec.block_info_record_length * RECS_PER_RECORDSET + masterRec.block_info_recordset_header_length;
    assert((bl.length() == 0 && record_set_size == 0) || (bl.length() != 0 && record_set_size != 0));

    BlockInfoRecord rec;
    for (CompressContext::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
      CompressContext::BlockMap::const_iterator it_next = it;
      ++it_next;
      if (it_next == blocks.end()) {
	rec = BlockInfoRecord(it->second.method_idx, masterRec.current_original_pos - it->first, masterRec.current_compressed_pos - it->second.target_offset);
      } else {
	rec = BlockInfoRecord(it->second.method_idx, it_next->first - it->first, it_next->second.target_offset - it->second.target_offset);
      }

      if (record_set_size == 0) { //first record to be added - need to measure rec sizes
	BlockInfoRecordSetHeader header(it->first, it->second.target_offset);
	size_t old_len = bl.length();
	::encode(header, bl);
	masterRec.block_info_recordset_header_length = bl.length() - old_len;
	old_len = bl.length();
	::encode(rec, bl);
	masterRec.block_info_record_length = bl.length() - old_len;

	record_set_size = masterRec.block_info_record_length * RECS_PER_RECORDSET + masterRec.block_info_recordset_header_length;
	assert(record_set_size != 0);
      } else {
	if ((bl.length() % record_set_size) == 0) {
	  BlockInfoRecordSetHeader header(it->first, it->second.target_offset);
	  ::encode(header, bl);
	}
	::encode(rec, bl);
      }
    }

    (*attrset)[ECUtil::get_cinfo_key()] = bl;

    bufferlist master_bl;
    ::encode(masterRec, master_bl );
     (*attrset)[ECUtil::get_cinfo_master_key()] = master_bl;

    dout(1) << __func__ << " ifed: cinfo:" << (*attrset)[ECUtil::get_cinfo_master_key()].length() << "," <<
	    bl.length() << "," << masterRec.block_info_record_length << "," << masterRec.block_info_recordset_header_length << "," << masterRec.current_original_pos << "," << masterRec.current_compressed_pos <<
	    "," << prevMasterRec.current_original_pos << "," << prevMasterRec.current_compressed_pos << dendl;

    prev_blocks_encoded.swap(bl);
    prevMasterRec = masterRec;
    blocks.clear();
  }
}

void CompressContext::flush_for_rollback(map<string, boost::optional<bufferlist> >* attrset) const {
  assert(attrset);
  if (prev_blocks_encoded.length() > 0) {
    (*attrset)[ECUtil::get_cinfo_key()] = prev_blocks_encoded;
  } else {
    (*attrset)[ECUtil::get_cinfo_key()] = boost::optional<bufferlist>();    //to trigger record removal on rollback
  }

  if (prevMasterRec.current_original_pos != 0) {
    bufferlist bl;
    ::encode(prevMasterRec, bl);
    (*attrset)[ECUtil::get_cinfo_master_key()] = bl;
  } else {
    (*attrset)[ECUtil::get_cinfo_master_key()] = boost::optional<bufferlist>();    //to trigger record removal on rollback
  }
}


void CompressContext::dump(Formatter* f) const {
  f->dump_unsigned("current_original_pos", masterRec.current_original_pos);
  f->dump_unsigned("current_compressed_pos", masterRec.current_compressed_pos);
  f->open_object_section("blocks");
  for (CompressContext::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
    f->open_object_section("block");
    f->dump_unsigned("offset", it->first);
    f->dump_string("method", masterRec.get_method_name(it->second.method_idx));
    f->dump_unsigned("target_offset", it->second.target_offset);
    f->close_section();
  }
  f->close_section();
}

void CompressContext::append_block(uint64_t original_offset,
				   uint64_t original_size,
				   const string& method,
				   uint64_t new_block_size) {
  assert(original_size != 0);
  assert(original_offset == masterRec.current_original_pos);

  uint8_t method_idx = masterRec.add_get_method(method);

  blocks[original_offset] = BlockInfo(method_idx, masterRec.current_compressed_pos);

  masterRec.current_original_pos += original_size;
  masterRec.current_compressed_pos += new_block_size;
}

pair<uint64_t, uint64_t> CompressContext::offset_len_to_compressed_block(const pair<uint64_t, uint64_t> offs_len_pair) const {
  uint64_t start_offs = offs_len_pair.first;
  uint64_t end_offs = offs_len_pair.first + offs_len_pair.second - 1;
  assert(masterRec.current_original_pos == 0 || start_offs < masterRec.current_original_pos);
  assert(masterRec.current_original_pos == 0 || end_offs < masterRec.current_original_pos);

  uint64_t res_start_offs = masterRec.current_original_pos != 0 ? map_offset(start_offs, false).second : start_offs;
  uint64_t res_end_offs = masterRec.current_original_pos != 0 ? map_offset(end_offs, true).second : end_offs + 1;

  dout(1) << __func__ << masterRec.current_original_pos << "," << masterRec.current_compressed_pos << "," << blocks.size() << dendl;
  return std::pair<uint64_t, uint64_t>(res_start_offs, res_end_offs - res_start_offs);
}

bool CompressContext::less_upper(const uint64_t& val1, const BlockMap::value_type& val2) {
  return val1 < val2.first;
}

bool CompressContext::less_lower(const BlockMap::value_type& val1, const uint64_t& val2) {
  return val1.first < val2;
}

pair<uint64_t, uint64_t> CompressContext::map_offset(uint64_t offs, bool next_block_flag) const {
  pair<uint64_t, uint64_t> res; //original block offset, compressed block offset
  BlockMap::const_iterator it;
  if (next_block_flag) {
    it = std::upper_bound(blocks.begin(), blocks.end(), offs, less_upper);
    if (it != blocks.end()) {
      res = std::pair<uint64_t, uint64_t>(it->first, it->second.target_offset);
    } else {
      res = std::pair<uint64_t, uint64_t>(masterRec.current_original_pos, masterRec.current_compressed_pos);
    }
  } else {
    it = std::lower_bound(blocks.begin(), blocks.end(), offs, less_lower);
    if (it == blocks.end() || it->first != offs) { //lower_end returns iterator to the specified value if present or the next entry
      assert(it != blocks.begin());
      --it;
    }
    res = std::pair<uint64_t, uint64_t>(it->first, it->second.target_offset);
  }
  return res;
}

const int CompressContextDebugLevel = 10;
int CompressContext::try_decompress(const hobject_t& oid, uint64_t orig_offs, uint64_t len, const bufferlist& cs_bl, bufferlist* res_bl) const {
  assert(res_bl);
  int res = 0;
  uint64_t appended = 0;
  if (masterRec.current_original_pos == 0) { //no compression applied was to the object
    dout(CompressContextDebugLevel) << __func__ << "ifed: bypass due to no compression were applied: oid=" << oid << " (" << orig_offs << ", " << len << "," << cs_bl.length() << ")" << dendl;
    res_bl->append(cs_bl);
    appended += cs_bl.length();
  } else {
    dout(CompressContextDebugLevel) << __func__ << "ifed: decompressing oid=" << oid << " (" << orig_offs << ", " << len << ")" << dendl;
    assert(masterRec.current_original_pos >= orig_offs + len);
    assert(blocks.size() > 0);

    uint64_t cur_block_offs, cur_block_len, cur_block_coffs, cur_block_clen;
    string cur_block_method;
    BlockMap::const_iterator it = blocks.begin();

    bufferlist::iterator cs_bl_pos = const_cast<bufferlist&>(cs_bl).begin();
    do {
      cur_block_offs = it->first;
      cur_block_coffs = it->second.target_offset;
      cur_block_method = masterRec.get_method_name(it->second.method_idx);
      ++it;
      if (it == blocks.end()) {
	cur_block_len = masterRec.current_original_pos - cur_block_offs;
	cur_block_clen = masterRec.current_compressed_pos - cur_block_coffs;
      } else {
	cur_block_len = it->first - cur_block_offs;
	cur_block_clen = it->second.target_offset - cur_block_coffs;
      }
      if (cur_block_offs + cur_block_len >= orig_offs) { //skipping map entries for data ranges prior to the desired one.
	bufferlist cs_bl1, tmp_bl;
	CompressorRef cs_impl;
	stringstream ss;
	int r = cur_block_method.empty() ? 0 :
		CompressionPluginRegistry::instance().factory(
		  cur_block_method,
		  g_conf->compression_dir,
		  &cs_impl,
		  &ss);
	if (r != 0) {
	  derr << "Failed to create decompression engine for " << cur_block_method << ":" << ss.str() << dendl;
	  res = -1;
	} else if (cs_impl == NULL) {
	  uint64_t offs2splice = cur_block_offs < orig_offs ? orig_offs - cur_block_offs : 0;

	  uint64_t len2splice = cur_block_len - offs2splice;
	  len2splice -= cur_block_offs + cur_block_len > orig_offs + len ? cur_block_offs + cur_block_len - orig_offs - len : 0 ;

	  offs2splice += cs_bl_pos.get_off();
	  dout(CompressContextDebugLevel) << __func__ << "ifed: uncompressed: oid=" << oid << " (" << cur_block_offs << "," << cur_block_len << "," << offs2splice << "," << len2splice << "," << cs_bl.length() << ")" << dendl;
	  if (offs2splice == 0 && len2splice == cs_bl.length()) { //current block is completely within the requested range
	    res_bl->append(cs_bl);
	  } else {
	    tmp_bl.substr_of(cs_bl, offs2splice, len2splice);
	    res_bl->append(tmp_bl);
	  }
	  appended += len2splice;
	} else {
	  dout(CompressContextDebugLevel) << __func__ << ": prepare decompress:" << cs_bl_pos.get_off() << ","
					  << cs_bl.length() << "," << cur_block_clen << "," << cur_block_method << dendl;

	  uint32_t real_len = 0;
	  bufferlist::iterator it = cs_bl_pos;
	  ::decode(real_len, it);
	  assert(cur_block_clen >= real_len);
	  assert(it.get_remaining() >= real_len);
	  cs_bl1.substr_of(cs_bl, it.get_off(), real_len);

	  int r = cs_impl->decompress(
		    cs_bl1,
		    tmp_bl);
	  if (r < 0) {
	    derr << __func__ << ": decompress(" << cur_block_method << ")"
		 << " returned an error: " << r << dendl;
	    res = -1;
	  } else {
	    uint64_t offs2splice = cur_block_offs < orig_offs ? orig_offs - cur_block_offs : 0;
	    uint64_t len2splice = tmp_bl.length() - offs2splice;
	    len2splice -= cur_block_offs + cur_block_len > orig_offs + len ? cur_block_offs + cur_block_len - orig_offs - len : 0;

	    dout(CompressContextDebugLevel) << __func__ << "ifed: decompressed: oid=" << oid << " (" << r << "," << cur_block_offs << "," << cur_block_len << "," << offs2splice << "," << len2splice << "," << tmp_bl.length() << ")" << dendl;

	    if (offs2splice == 0 && len2splice == tmp_bl.length()) { //decompressed block is completely within the requested range
	      res_bl->append(tmp_bl);
	    } else {
	      tmp_bl.splice(offs2splice, len2splice, res_bl);
	    }
	    appended += len2splice;
	  }
	}
      }

      cs_bl_pos.advance(cur_block_clen);
    } while (it != blocks.end() && appended < len && res == 0);
  }
  assert(res != 0 || (res == 0 && appended == len));
  return res;
}

int CompressContext::do_compress(CompressorRef cs_impl, const bufferlist& block2compress, bufferlist* result_bl) const {
  int res = -1;
  bufferlist tmp_bl;
  assert(result_bl);
  int r = cs_impl->compress(const_cast<bufferlist&>(block2compress), tmp_bl);
  if (r == 0) {
    bufferlist tmp_bl0;
    uint32_t real_len = tmp_bl.length();
    ::encode(real_len, tmp_bl0);
    unsigned block_len = tmp_bl.length() + tmp_bl0.length();
    if (block_len < block2compress.length()) {
      result_bl->append(tmp_bl0);
      result_bl->append(tmp_bl);
      res = 1;
    } else {
      result_bl->append(block2compress);
      res = 0;
    }
  }
  return res;
}

int CompressContext::try_compress(const std::string& compression_method, const hobject_t& oid, const bufferlist& bl0, const ECUtil::stripe_info_t& sinfo, uint64_t* off, bufferlist* res_bl) {
  assert(off);
  assert(res_bl);
  bufferlist bl_cs(bl0);
  bufferlist bl;
  CompressContext new_cinfo(*this);

  dout(CompressContextDebugLevel) << __func__ << "ifed: compressing oid=" << oid << " (" << off << ", " << bl0.length() << ", " << compression_method << ")" << dendl;

  bool compressed = false, failure = false;
  uint64_t prev_compressed_pos = masterRec.current_compressed_pos;
  CompressorRef cs_impl;
  stringstream ss;
  int r = compression_method.empty() ? 0 :
	  CompressionPluginRegistry::instance().factory(
	    compression_method,
	    g_conf->compression_dir,
	    &cs_impl,
	    &ss);
  if (r != 0) {
    derr << "Failed to create compression engine:" << ss.str() << dendl;
  }

  //apply compression if that's a first block or it's been already applied to previous blocks
  if (can_compress(*off) && cs_impl != NULL) {
    if (bl_cs.length() > sinfo.get_stripe_width()) {

      bufferlist::iterator it = bl_cs.begin();

      uint64_t cur_offs = *off;
      while (!it.end() && !failure) {
	uint64_t block_size = MIN(it.get_remaining(), get_block_size(sinfo.get_stripe_width()));
	bufferlist block2compress, compressed_block;
	uint64_t prev_len = bl.length();
	it.copy(block_size, block2compress);

	int r0 = do_compress(cs_impl, block2compress, &bl);
	if (r0 < 0) {
	  dout(CompressContextDebugLevel) << "ifed: block compression failed, left uncompressed, oid=" << oid << dendl;
	  failure = true;
	} else {
	  if (it.end()) {
	    unsigned padded_size = sinfo.pad_to_stripe_width(bl.length());
	    if (padded_size > bl.length()) {
	      bl.append_zero(padded_size - bl.length());
	    }
	  }
	  new_cinfo.append_block(cur_offs, block2compress.length(), r0 ? compression_method : "", bl.length() - prev_len);
	}
	cur_offs += block2compress.length();
      }
      if (!failure && bl.length() < sinfo.pad_to_stripe_width(bl_cs.length())) {
	compressed = true;

	dout(CompressContextDebugLevel) << "ifed: block compressed, oid=" << oid << ", ratio = " << (float)bl_cs.length() / bl.length() << dendl;
      } else if (failure) {
	dout(CompressContextDebugLevel) << "ifed: block compression bypassed due to failure, oid=" << oid << dendl;
      } else {
	dout(CompressContextDebugLevel) << "ifed: block compression bypassed, oid=" << oid << dendl;
      }

    } else {
      dout(CompressContextDebugLevel) << "ifed: block compression bypassed, block is too small, oid=" << oid << dendl;
    }

  }
  if (!compressed) { //the whole block wasn't compressed or there is no benefit in compression
    *res_bl = bl0;
    append_block(*off, res_bl->length(), "", res_bl->length());
  } else {
    bl.swap(*res_bl);
    swap(new_cinfo);
  }
  *off = prev_compressed_pos;
  return 0;
}
