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

void CompressContext::BlockInfo::encode(bufferlist &bl) const
{
        ENCODE_START(1, 1, bl);
        ::encode(method, bl);
        ::encode(target_offset, bl);
        ENCODE_FINISH(bl);
}

void CompressContext::BlockInfo::decode(bufferlist::iterator &bl)
{
        DECODE_START(1, bl);
        ::decode(method, bl);
        ::decode(target_offset, bl);
        DECODE_FINISH(bl);
}

bool  CompressContext::can_compress( uint64_t offs ) const
{
    return (offs == 0 && current_compressed_pos == 0) || current_compressed_pos != 0; 
}

void CompressContext::setup_for_append(bufferlist& master_record_val)
{
        clear();
        bufferlist::iterator it = master_record_val.begin();
        ::decode(
                current_original_pos,
                it);
        ::decode(
                current_compressed_pos,
                it);
}

void CompressContext::setup_for_read(map<string, bufferlist>& attrset, uint64_t start_offset, uint64_t end_offset)
{
        clear();
        string key_prefix = ECUtil::get_cinfo_key();
        map<string, bufferlist>::iterator it = attrset.find(key_prefix);
        map<string, bufferlist>::iterator end = attrset.end();

        if (it != end) {
                bufferlist::iterator it_val = it->second.begin();
                ::decode(
                        current_original_pos,
                        it_val);
                ::decode(
                        current_compressed_pos,
                        it_val);
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

void CompressContext::flush( map<string, bufferlist> & attrset)
{
        for (CompressContext::BlockMap::const_iterator it = blocks.begin(); it != blocks.end(); it++) {
                stringstream ss_key;
                ss_key << ECUtil::get_cinfo_key() << std::hex << it->first;

                ::encode(
                        it->second,
                        attrset[ss_key.str()]);
        }
        blocks.clear();

        ::encode(
                current_original_pos,
                attrset[ECUtil::get_cinfo_key()]);

        ::encode(
                current_compressed_pos,
                attrset[ECUtil::get_cinfo_key()]);
}

void CompressContext::dump(Formatter *f) const
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

void CompressContext::append_block(uint64_t original_offset,
                  uint64_t original_size, 
                  const string& method, 
                  uint64_t new_block_size )
{
        blocks[original_offset] = BlockInfo(method, current_compressed_pos);

        current_original_pos += original_size;

        current_compressed_pos += new_block_size;
}

pair<uint64_t, uint64_t> CompressContext::offset_len_to_compressed_block(const pair<uint64_t, uint64_t> offs_len_pair) const
{
        uint64_t start_offs = offs_len_pair.first;
        uint64_t end_offs = offs_len_pair.first + offs_len_pair.second-1;
        assert(current_original_pos == 0 || start_offs < current_original_pos);
        assert(current_original_pos == 0 || end_offs < current_original_pos);

        uint64_t res_start_offs = current_original_pos != 0 ? map_offset(start_offs, false).second : start_offs;
        uint64_t res_end_offs = current_original_pos != 0 ? map_offset(end_offs, true).second : end_offs+1;

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
                        res = std::pair<uint64_t, uint64_t>(current_original_pos, current_compressed_pos);
                }
        }
        else {
                it = std::lower_bound(blocks.begin(), blocks.end(), offs, less_lower);
                if (it == blocks.end() || it->first != offs){ //lower_end returns iterator to the specified value if present or the next entry
                        assert(it != blocks.begin()); 
                        --it; 
                }
                res = std::pair<uint64_t, uint64_t>(it->first, it->second.target_offset);
        }
//dout(1)<<__func__<<res.first<<","<<res.second<<dendl;
        return res;
}

const int CompressContextDebugLevel = 1;
int CompressContext::try_decompress(CompressionInterfaceRef cs_impl, const hobject_t& oid, uint64_t orig_offs, uint64_t len, bufferlist& cs_bl, bufferlist& res_bl) const
{
        int res = 0;
        uint64_t appended = 0;
        if (current_original_pos == 0) //no compression applied was to the object
        {
                dout(CompressContextDebugLevel) << __func__ <<"ifed: bypass due to no compression were applied: oid="<<oid<<" (" << orig_offs << ", " << len << ","<< cs_bl.length() << ")" << dendl;
                res_bl.append(cs_bl);
                appended += cs_bl.length();
        }
        else
        {
                dout(CompressContextDebugLevel) << __func__ <<"ifed: decompressing oid="<<oid<<" (" << orig_offs << ", " << len << ")" << dendl;
                assert(current_original_pos >= orig_offs + len);
                assert(blocks.size() > 0);

                uint64_t cur_block_offs, cur_block_len, cur_block_coffs, cur_block_clen;
                string cur_block_method;
                BlockMap::const_iterator it = blocks.begin();

                uint64_t cs_bl_pos = 0;

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
                                        else
                                                cs_bl.splice(offs2splice, len2splice, &res_bl);
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

        bool compressed = false, failure = false;
        uint64_t prev_compressed_pos = current_compressed_pos;

        //apply compression if that's a first block or it's been already applied to previous blocks
        if (can_compress(off) && cs_impl!=NULL) {
                if (bl_cs.length() > sinfo.get_stripe_width()) {

                        bufferlist::iterator it = bl_cs.begin();

                        uint64_t cur_offs = off;
                        while (!it.end() && !failure){
                                uint64_t block_size = cs_impl->get_block_size(sinfo.get_stripe_width());
                                bufferlist block2compress, compressed_block;
                                uint64_t prev_len = bl.length();
                                it.copy(block_size, block2compress);

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
