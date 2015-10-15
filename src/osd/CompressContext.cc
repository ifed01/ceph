// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <errno.h>
#include "include/encoding.h"
#include "ECUtil.h"
#include "CompressContext.h"
#include "common/debug.h"


#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix *_dout

void CompressContext::BlockInfoRecord::encode(bufferlist &bl) const
{
        ENCODE_START(1, 1, bl);
        ::encode(method_idx, bl);
        ::encode(original_length, bl);
        ::encode(compressed_length, bl);
        ENCODE_FINISH(bl);
}

void CompressContext::BlockInfoRecord::decode(bufferlist::iterator &bl)
{
        DECODE_START(1, bl);
        ::decode(method_idx, bl);
        ::decode(original_length, bl);
        ::decode(compressed_length, bl);
        DECODE_FINISH(bl);
}

void CompressContext::BlockInfoRecordSetHeader::encode(bufferlist &bl) const
{
        ENCODE_START(1, 1, bl);
        ::encode(start_offset, bl);
        ::encode(compressed_offset, bl);
        ENCODE_FINISH(bl);
}

void CompressContext::BlockInfoRecordSetHeader::decode(bufferlist::iterator &bl)
{
        DECODE_START(1, bl);
        ::decode(start_offset, bl);
        ::decode(compressed_offset, bl);
        DECODE_FINISH(bl);
}

uint8_t CompressContext::MasterRecord::add_get_method(const string& method)
{
        uint8_t res = 0;
        if (!method.empty()){
                for (size_t i = 0; i < methods.size() && res == 0; i++){
                        if (methods[i] == method)
                                res = i + 1;
                }
                if (res == 0){
                        methods.push_back(method);
                        res = methods.size();
                }
        }
        return res;
}

string CompressContext::MasterRecord::get_method_name(uint8_t index) const
{
        string res;
        if (index > 0)
        {
                if (index <= methods.size())
                        res = methods[index - 1];
                else
                        res = "???";
        }
        return res;
}

void CompressContext::MasterRecord::encode(bufferlist &bl) const
{
        ENCODE_START(1, 1, bl);
        ::encode(current_original_pos, bl);
        ::encode(current_compressed_pos, bl);
        ::encode(block_info_record_length, bl);
        ::encode(block_info_recordset_header_length, bl);
        ::encode(methods, bl);
        ENCODE_FINISH(bl);
}


void CompressContext::MasterRecord::decode(bufferlist::iterator &bl)
{
        DECODE_START(1, bl);
        ::decode(current_original_pos, bl);
        ::decode(current_compressed_pos, bl);
        ::decode(block_info_record_length, bl);
        ::decode(block_info_recordset_header_length, bl);
        ::decode(methods, bl);

        DECODE_FINISH(bl);
}

bool  CompressContext::can_compress( uint64_t offs ) const
{
        return (offs == 0 && masterRec.current_compressed_pos == 0) || masterRec.current_compressed_pos != 0;
}

void CompressContext::setup_for_append(map<string, bufferlist>& attrset)
{
        clear();
        
        map<string, bufferlist>::iterator it_attrs = attrset.find(ECUtil::get_cinfo_master_key());
        if (it_attrs != attrset.end()){
                bufferlist::iterator it = it_attrs->second.begin();
                ::decode(masterRec, it);
                prev_original_pos = masterRec.current_original_pos;
                prev_compressed_pos = masterRec.current_compressed_pos;
        }

        it_attrs = attrset.find(ECUtil::get_cinfo_key());
        if (it_attrs != attrset.end()){
dout(0)<<__func__<<" ifed:found"<<dendl;
                prev_blocks_encoded = it_attrs->second;
        }
dout(0)<<__func__<<" ifed:"<<prev_blocks_encoded.length()<<dendl;
}

void CompressContext::setup_for_read(map<string, bufferlist>& attrset, uint64_t start_offset, uint64_t end_offset)
{
        clear();
        map<string, bufferlist>::iterator it = attrset.find(ECUtil::get_cinfo_master_key());
        map<string, bufferlist>::iterator end = attrset.end();
        if (it != end) {
                bufferlist::iterator it_val = it->second.begin();
                ::decode(masterRec, it_val);
                assert(end_offset >= start_offset);
                assert(end_offset <= masterRec.current_original_pos);


                it = attrset.find(ECUtil::get_cinfo_key());
                if (it != end) {
                        size_t record_set_size = masterRec.block_info_record_length*RECS_PER_RECORDSET + masterRec.block_info_recordset_header_length;
                        assert(record_set_size != 0);

                        //bufferlist& bl = it->second;
                        //
                        bufferlist::iterator it_bl = it->second.begin();
                        BlockInfoRecordSetHeader recset_header;
                        { //searching for the record set that contains desired data range
                                BlockInfoRecordSetHeader recset_header_next;
                                bufferlist::iterator it_bl_next = it_bl_next;
                                ::decode(recset_header, it_bl);

                                bool found = false;
                                do{
                                        if (it_bl_next.get_remaining() >= record_set_size)
                                        {
                                                it_bl_next.advance(record_set_size);
                                                ::decode(recset_header_next, it_bl_next);

                                                found = recset_header_next.start_offset > start_offset;
                                                if (!found)
                                                {
                                                        it_bl = it_bl_next;
                                                        recset_header = recset_header_next;
                                                }
                                        }
                                        else
                                                found = true;
                                } while (!found);
                        }

                        uint64_t cur_pos = recset_header.start_offset;
                        uint64_t cur_cpos = recset_header.compressed_offset;
dout(1)<<__func__<<cur_pos<<dendl;
                        uint64_t found_start_offset = 0;
                        BlockInfo found_bi;
                        bool start_found = false;
                        bool stop=false;

                        while (!stop && !it_bl.end()){
                                if ((it_bl.get_off() % record_set_size) == 0){
                                        ::decode(recset_header, it_bl);
                                }
                                BlockInfoRecord rec;
                                ::decode(rec, it_bl);

                                if (cur_pos <= start_offset && (!start_found || cur_pos > found_start_offset)) {
                                        found_start_offset = cur_pos;
                                        found_bi = BlockInfo( rec.method_idx, cur_cpos);
                                        start_found = true;
                                }
                                if (cur_pos > start_offset) {
                                        blocks[cur_pos] = BlockInfo(rec.method_idx, cur_cpos);
                                        if( cur_pos >= end_offset )
                                            stop=true;
dout(0)<<__func__<<" ifed:"<<cur_pos<<"=("<<(int)rec.method_idx<<","<<cur_cpos<<")"<<dendl;
                                }

                                cur_pos += rec.original_length;
                                cur_cpos += rec.compressed_length;
                        }

                        if ( start_found){
                                blocks[found_start_offset] = found_bi;
dout(0)<<__func__<<" ifed:"<<found_start_offset<<"==("<<(int)found_bi.method_idx<<","<<found_bi.target_offset<<")"<<dendl;
                        }
                }
        }
}

/*void CompressContext::setup_for_read(map<string, bufferlist>& attrset, uint64_t start_offset, uint64_t end_offset)
{
        clear();
        string key_prefix = ECUtil::get_cinfo_key();
        map<string, bufferlist>::iterator it = attrset.find(key_prefix);
        map<string, bufferlist>::iterator end = attrset.end();

        if (it != end) {
                bufferlist::iterator it_val = it->second.begin();
                ::decode(masterRec, it_val);
                ++it;

                uint64_t start_found_offset=0;
                map<string, bufferlist>::iterator start_it=end;
                while ( it != end && it->first.size() > key_prefix.size() && it->first.compare(0, key_prefix.size(), key_prefix) == 0 ) {
                        uint64_t rec_offset = strtoull(it->first.c_str() + key_prefix.size(), NULL, 16);
                        if (rec_offset <= start_offset && (start_it==end || rec_offset>start_found_offset)) {
                                start_found_offset = rec_offset;
                                start_it = it;
                        }
                        if (rec_offset <= end_offset) {
                                if (rec_offset > start_offset) {
                                        bufferlist::iterator bp = it->second.begin();
                                        ::decode(
                                                blocks[rec_offset],
                                                bp);
//dout(1)<<__func__<<" add: ifed: "<<rec_offset<<dendl;
                                }
                        }
                        ++it;
                }

                if (start_it != end){
                        bufferlist::iterator bp = start_it->second.begin();
                        ::decode(
                                blocks[start_found_offset],
                                bp);
//dout(1)<<__func__<<" add0: ifed: "<<start_found_offset<<","<<start_it->first<<dendl;
                }
        }
}
*/

void CompressContext::flush( map<string, bufferlist> & attrset)
{
        if (prev_original_pos != masterRec.current_original_pos){ //some changes have been made
                bufferlist bl(prev_blocks_encoded);

                size_t record_set_size = masterRec.block_info_record_length*RECS_PER_RECORDSET + masterRec.block_info_recordset_header_length;
                assert((bl.length() == 0 && record_set_size == 0) || (bl.length() != 0 && record_set_size != 0));

                uint64_t offs = prev_original_pos;
                uint64_t coffs = prev_compressed_pos;

                for (CompressContext::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
                        BlockInfoRecordSetHeader header(offs, coffs);
                        BlockInfoRecord rec;
                        CompressContext::BlockMap::const_iterator it_next = it;
                        ++it_next;
                        if (it_next == blocks.end())
                                rec = BlockInfoRecord(it->second.method_idx, masterRec.current_original_pos - it->first, masterRec.current_compressed_pos - it->second.target_offset);
                        else
                                rec = BlockInfoRecord(it->second.method_idx, it_next->first - it->first, it_next->second.target_offset - it->second.target_offset);

                        if (record_set_size == 0){ //first record to be added - need to measure rec sizes
                                size_t old_len = bl.length();
                                ::encode(header, bl);
                                masterRec.block_info_recordset_header_length = bl.length() - old_len;
                                old_len = bl.length();
                                ::encode(rec, bl);
                                masterRec.block_info_record_length = bl.length() - old_len;

                                record_set_size = masterRec.block_info_record_length * RECS_PER_RECORDSET + masterRec.block_info_recordset_header_length;
                                assert(record_set_size != 0);
                        }
                        else{
                                if ((bl.length() % record_set_size) == 0)
                                    ::encode(header, bl);
                                ::encode(rec, bl);
                        }
                }

                attrset[ECUtil::get_cinfo_key()]=bl;

                ::encode(masterRec,
                        attrset[ECUtil::get_cinfo_master_key()]);
dout(1)<<__func__<<" ifed: cinfo:"<<attrset[ECUtil::get_cinfo_master_key()].length()<<","<<
    bl.length()<<","<<masterRec.block_info_record_length<<","<<masterRec.block_info_recordset_header_length<<dendl;
        }
}

void CompressContext::flush_for_rollback(map<string, bufferlist>& attrset) const
{
        attrset[ECUtil::get_cinfo_key()] = prev_blocks_encoded;

        ::encode(masterRec,
                attrset[ECUtil::get_cinfo_master_key()]);
}


void CompressContext::dump(Formatter *f) const
{
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
                  uint64_t new_block_size )
{
        uint8_t method_idx = masterRec.add_get_method(method);

        blocks[original_offset] = BlockInfo(method_idx, masterRec.current_compressed_pos);

        masterRec.current_original_pos += original_size;

        masterRec.current_compressed_pos += new_block_size;
}

pair<uint64_t, uint64_t> CompressContext::offset_len_to_compressed_block(const pair<uint64_t, uint64_t> offs_len_pair) const
{
        uint64_t start_offs = offs_len_pair.first;
        uint64_t end_offs = offs_len_pair.first + offs_len_pair.second-1;
        assert(masterRec.current_original_pos == 0 || start_offs < masterRec.current_original_pos);
        assert(masterRec.current_original_pos == 0 || end_offs < masterRec.current_original_pos);

        uint64_t res_start_offs = masterRec.current_original_pos != 0 ? map_offset(start_offs, false).second : start_offs;
        uint64_t res_end_offs = masterRec.current_original_pos != 0 ? map_offset(end_offs, true).second : end_offs + 1;

//dout(1)<<__func__<<current_original_pos<<","<<current_compressed_pos<<","<<blocks.size()<<dendl;
        return std::pair<uint64_t, uint64_t>(res_start_offs, res_end_offs - res_start_offs);
}

bool CompressContext::less_upper(const uint64_t& val1, const BlockMap::value_type& val2 )
{
    return val1<val2.first;
}

bool CompressContext::less_lower( const BlockMap::value_type& val1, const uint64_t& val2 )
{
    return val1.first < val2;
}

pair<uint64_t, uint64_t> CompressContext::map_offset(uint64_t offs, bool next_block_flag) const
{
        pair<uint64_t, uint64_t> res; //original block offset, compressed block offset
        BlockMap::const_iterator it;
        if (next_block_flag){
                it = std::upper_bound(blocks.begin(), blocks.end(), offs, less_upper);
                if (it != blocks.end()){
                        res = std::pair<uint64_t, uint64_t>(it->first, it->second.target_offset);
                }
                else {
                        res = std::pair<uint64_t, uint64_t>(masterRec.current_original_pos, masterRec.current_compressed_pos);
                }
        }
        else {
                it = std::lower_bound(blocks.begin(), blocks.end(), offs, less_lower);
                if (it == blocks.end() || it->first != offs){ //lower_end returns iterator to the specified value if present or the next entry
dout(0)<<__func__<<" ifed:"<<offs<<","<<blocks.size()<<dendl;

                        assert(it != blocks.begin()); 
                        --it; 
                }
                res = std::pair<uint64_t, uint64_t>(it->first, it->second.target_offset);
        }
dout(1)<<__func__<<res.first<<","<<res.second<<dendl;
        return res;
}

const int CompressContextDebugLevel = 1;
int CompressContext::try_decompress(CompressionInterfaceRef cs_impl, const hobject_t& oid, uint64_t orig_offs, uint64_t len, bufferlist& cs_bl, bufferlist& res_bl) const
{
        int res = 0;
        uint64_t appended = 0;
        if (masterRec.current_original_pos == 0) //no compression applied was to the object
        {
                dout(CompressContextDebugLevel) << __func__ <<"ifed: bypass due to no compression were applied: oid="<<oid<<" (" << orig_offs << ", " << len << ","<< cs_bl.length() << ")" << dendl;
                res_bl.append(cs_bl);
                appended += cs_bl.length();
        }
        else
        {
                dout(CompressContextDebugLevel) << __func__ <<"ifed: decompressing oid="<<oid<<" (" << orig_offs << ", " << len << ")" << dendl;
                assert(masterRec.current_original_pos >= orig_offs + len);
                assert(blocks.size() > 0);

                uint64_t cur_block_offs, cur_block_len, cur_block_coffs, cur_block_clen;
                string cur_block_method;
                BlockMap::const_iterator it = blocks.begin();

                uint64_t cs_bl_pos = 0;

                do{
                        cur_block_offs = it->first;
                        cur_block_coffs = it->second.target_offset;
                        cur_block_method = masterRec.get_method_name(it->second.method_idx);
                        ++it;
                        if (it == blocks.end())
                        {
                                cur_block_len = masterRec.current_original_pos - cur_block_offs;
                                cur_block_clen = masterRec.current_compressed_pos - cur_block_coffs;
                        }
                        else
                        {
                                cur_block_len = it->first - cur_block_offs;
                                cur_block_clen = it->second.target_offset - cur_block_coffs;
                        }
                        if (cur_block_offs+cur_block_len >= orig_offs) //skipping map entries for data ranges prior to the desired one.
                        {
                                bufferlist cs_bl1, tmp_bl;
                                if (cur_block_method.empty()) //block not compressed, doing data copy directly
                                {
                                        uint64_t offs2splice = cur_block_offs < orig_offs ? orig_offs - cur_block_offs : 0;

                                        uint64_t len2splice = cur_block_len-offs2splice;
                                        len2splice -= cur_block_offs + cur_block_len > orig_offs + len ? cur_block_offs + cur_block_len - orig_offs - len :0 ;
                                        //len2splice = MIN(len2splice, cur_block_len-offs2splice);

                                        offs2splice += cs_bl_pos;
                                        dout(CompressContextDebugLevel) << __func__ <<"ifed: uncompressed: oid="<<oid<< " (" << cur_block_offs << "," << cur_block_len << "," << offs2splice << "," << len2splice << "," << cs_bl.length() << ")" << dendl;
                                        if (offs2splice == 0 && len2splice == cs_bl.length()) //current block is completely within the requested range
                                                res_bl.append(cs_bl);
                                        else{
                                                tmp_bl.substr_of( cs_bl, offs2splice, len2splice);
                                                res_bl.append(tmp_bl);
                                                //cs_bl.splice(offs2splice, len2splice, &res_bl);
                                        }
                                        appended += len2splice;
                                }
                                else {
                                        cs_bl1.substr_of(cs_bl, cs_bl_pos, MIN(cs_bl.length() - cs_bl_pos, cur_block_clen));
                                        int r = ECUtil::decompress(
                                                cs_impl,
                                                cur_block_len,
                                                cs_bl1,
                                                &tmp_bl); //FIXME: apply decompression method depending on the indicated one
                                        if (r< 0)
                                        {
                                                dout(0) << __func__ << ": decompress(" << cur_block_method << ")"
                                                        << " returned an error: "<< r << dendl;
                                                res = -1;
                                        }
                                        else{
                                                uint64_t offs2splice = cur_block_offs < orig_offs ? orig_offs - cur_block_offs : 0;
                                                uint64_t len2splice = tmp_bl.length()-offs2splice;
                                                len2splice -= cur_block_offs + cur_block_len > orig_offs + len ? cur_block_offs + cur_block_len - orig_offs - len : 0;
                                                //len2splice = MIN(len2splice, tmp_bl.length()-offs2splice);

                                                dout(CompressContextDebugLevel) << __func__ <<"ifed: decompressed: oid="<<oid <<" (" << cur_block_offs << "," << cur_block_len << "," << offs2splice << "," << len2splice << "," << tmp_bl.length() << ")" << dendl;

                                                if (offs2splice == 0 && len2splice == tmp_bl.length()) //decompressed block is completely within the requested range
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
        assert(res != 0 || (res == 0 && appended == len));
        return res;
}


int CompressContext::do_compress(CompressionInterfaceRef cs_impl, const ECUtil::stripe_info_t& sinfo, bufferlist& block2compress, std::string& res_method, bufferlist& result_bl) const
{
        int res = -1;
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
                res = 0;
        }
        return res;
}

int CompressContext::try_compress(CompressionInterfaceRef cs_impl, const hobject_t& oid, uint64_t& off, const bufferlist& bl0, const ECUtil::stripe_info_t& sinfo, bufferlist& res_bl)
{
        bufferlist bl_cs(bl0);
        bufferlist bl;
        CompressContext new_cinfo(*this);

        dout(CompressContextDebugLevel) << __func__ <<"ifed: compressing oid="<<oid<<" (" << off << ", " << bl0.length() << ")" << dendl;

        bool compressed = false, failure = false;
        uint64_t prev_compressed_pos = masterRec.current_compressed_pos;

        //apply compression if that's a first block or it's been already applied to previous blocks
        if (can_compress(off) && cs_impl!=NULL) {
                if (bl_cs.length() > sinfo.get_stripe_width()) {

                        bufferlist::iterator it = bl_cs.begin();

                        uint64_t cur_offs = off;
                        while (!it.end() && !failure){
                                uint64_t block_size = MIN( it.get_remaining(), cs_impl->get_block_size(sinfo.get_stripe_width()));
                                bufferlist block2compress, compressed_block;
                                uint64_t prev_len = bl.length();
                                dout(0) << "ifed: do compression " << block_size << dendl;
                                it.copy(block_size, block2compress);

                                dout(0) << "ifed: do compression " << block2compress.length() << dendl;

                                std::string cmethod;
                                int r0 = do_compress(cs_impl, sinfo, block2compress, cmethod, bl);
                                if (r0 != 0) {
                                        dout(0) << "ifed: block compression failed, left uncompressed, oid=" << oid << dendl;
                                        failure = true;
                                }
                                else{
                                        new_cinfo.append_block(cur_offs, block2compress.length(), cmethod, bl.length() - prev_len);
                                }
                                cur_offs += block2compress.length();
                        }
                        if (!failure && sinfo.pad_to_stripe_width(bl.length()) < sinfo.pad_to_stripe_width(bl_cs.length())) {
                                //There is some benefit from compression after data alignment
                                compressed = true;
                                dout(CompressContextDebugLevel) << "ifed: block compressed, oid=" << oid << ", ratio = " << (float)bl_cs.length() / bl.length() << dendl;
                        }
                        else if (failure)
                                dout(CompressContextDebugLevel) << "ifed: block compression bypassed due to failure, oid=" << oid << dendl;
                        else
                                dout(CompressContextDebugLevel) << "ifed: block compression bypassed, oid=" << oid << dendl;

                }
                else
                        dout(CompressContextDebugLevel) << "ifed: block compression bypassed, block is too small, oid=" << oid << dendl;

        }
        if (!compressed) //the whole block wasn't compressed or there is no benefit in compression
        {
                res_bl = bl0;
                if( off != 0 ) //we can omit adding dummy compression attrs if this is the first append
                   append_block(off, res_bl.length(), "", res_bl.length());
        }
        else
        {
                bl.swap(res_bl);
                swap(new_cinfo);
        }
        off = prev_compressed_pos;
        return 0;
}
