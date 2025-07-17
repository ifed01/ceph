#include <ostream>
#include <string>
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/debug.h"
#include "MxlCompressor.h"

#define dout_context cct
#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix *_dout << "mxl plugin:"

const uint32_t DST_BUF_SIZE = CEPH_PAGE_SIZE * 1024;
const uint32_t SRC_NUM_DESC = 32;

using pair_type = std::pair<const std::string, DRE_compAlgo>;
  inline static const std::initializer_list<pair_type> compression_algorithms {
    { "lzs",	 DRE_LZS },
    { "elzs",	 DRE_ELZS },
    { "gzip",	 DRE_GZIP },
    { "deflate", DRE_DEFLATE },
    { "zlib",	 DRE_ZLIB },
    { "xp10",	 DRE_XP10 },
  };

MxlCompressorSession::MxlCompressorSession(CephContext *cct)
  : cct(cct)
{
  memset(&compAlgoParam, 0, sizeof(compAlgoParam));
  compAlgoParam.reserved = nullptr;

  auto p = std::find_if(std::cbegin(compression_algorithms),
                        std::cend(compression_algorithms),
	   [cct](const auto& kv) { return kv.first == cct->_conf->compressor_mxl_alg; });

  ceph_assert(std::cend(compression_algorithms) != p);
  comp.compAlgo = p->second;
  switch (comp.compAlgo) {
    case DRE_DEFLATE:
      compAlgoParam.deflate.winSize = DRE_deflateWinSize(cct->_conf->compressor_mxl_param);
      break;
    case DRE_XP10:
      compAlgoParam.xp10.level = DRE_xp10Level(cct->_conf->compressor_mxl_param);
      break;
    default:
      break;
  };
  comp.compAlgoParam = &compAlgoParam;
  comp.optFlags = 0;
  comp.customDataFormat = 0;
  comp.compThreshold = 0;
  comp.edhMode = DRE_EDH_MODE_DROP_DATA_WITH_ERR;
}

MxlCompressorSession::~MxlCompressorSession()
{
  close();
}

int MxlCompressorSession::open(std::ostream* ss, bool _compress)
{
  if (!opened) {

    std::unique_ptr<byte[]> buf(new byte[DST_BUF_SIZE]);

    std::unique_ptr<DRE_dataDesc[]> src(new DRE_dataDesc[SRC_NUM_DESC]);

    const size_t num_dst = 1;
    std::unique_ptr<DRE_dataDesc[]> dst(new DRE_dataDesc[num_dst]);

    DRE_COSType cos = DRE_COS_TYPE_ASSURED_FWD;
    DRE_BOOL useDevMem = DRE_TRUE; /* User device memory */
    DRE_u32b sessFlag = 0;
    auto r = DRE_rawSessOpen(&handle,
      cos,
      _compress ? DRE_TRUE : DRE_FALSE,
      useDevMem,
      sessFlag,
      nullptr,
      &comp,
      nullptr, //pad
      nullptr, //enc
      nullptr, //hash
      nullptr, //ncme
      0);
    if(DRE_IS_RESULT_ERR(r))
    {
      *ss << "error opening compression session: " << r;
      return -1;
    }
    dst_buf.swap(buf);
    dst_buf_size = DST_BUF_SIZE;

    src_desc.swap(src);
    num_src_desc = SRC_NUM_DESC;
    memset(src_desc.get(), 0, sizeof(DRE_dataDesc) * num_src_desc);

    ceph_assert(num_dst == 1);
    dst_desc.swap(dst);
    num_dst_desc = num_dst;
    memset(dst_desc.get(), 0, sizeof(DRE_dataDesc) * num_dst_desc);

    opened = true;
    compress_mode = _compress;
  }
  return 0;
}

void MxlCompressorSession::close()
{
  if (opened) {
    DRE_rawSessClose(handle);
    src_desc.reset();
    num_src_desc = 0;
    dst_desc.reset();
    num_dst_desc = 0;
    dst_buf.reset();
    dst_buf_size = 0;
    opened = false;
  }
}

int MxlCompressorSession::compress(const ceph::buffer::list &sbl,
                            ceph::buffer::list &dbl,
                            std::optional<int32_t> &compressor_message)
{
  if (!opened || !compress_mode) {
    return -EBADFD;
  }
  auto it = sbl.buffers().begin();
  if (it == sbl.buffers().end() || sbl.length() == 0) {
    return -EINVAL;
  }

  DRE_rawSyncOpData opData; /* Operation parameters - filled by user */
  DRE_u32b dstBufferLength = 0;

  memset(&opData, 0, sizeof(DRE_rawSyncOpData));

  //initialize user proviced buffers
  opData.nextHeader = nullptr;   // pointer to new header
  opData.dstLen = &dstBufferLength;
  opData.consumedByteCnt = nullptr; // pointer to consumed bytes counter

  opData.compCount.countMode = 0;
  opData.compCount.headCount = 0;
  opData.compCount.sourceCount = 0;

  size_t src_idx = 0;
  size_t src_buf_pos = 0;
  size_t prefix_len = sizeof(uint32_t) * 2;
  size_t extra_len = 64;  // We expect destination buffer size not
                          // to exceed source length but for very
                          // short input this could be not the case.
                          // This primarily happens for existng compression
                          // UTs, hence let's have some spare space.

  size_t src_len = prefix_len + extra_len;

  ceph_assert(num_dst_desc == 1);

  while(it != sbl.buffers().end()) {
    auto l = std::min<size_t>(DST_BUF_SIZE - src_len, (*it).length() - src_buf_pos);
    if (l) {
      src_desc[src_idx].ptr = const_cast<char*>((*it).c_str()) + src_buf_pos;
      src_desc[src_idx].len = l;
      ++src_idx;
      src_buf_pos += l;
      src_len += l;
    }
    if (src_buf_pos == (*it).length()) {
      ++it;
      src_buf_pos = 0;
    }
    if (it == sbl.buffers().end() ||
        src_idx == num_src_desc ||
        src_len >= DST_BUF_SIZE) {
      if (src_len != 0) {
        auto p = dbl.get_contiguous_appender(src_len);
        dst_desc[0].ptr = p.get_pos() + prefix_len;
        dst_desc[0].len = src_len - prefix_len;

        auto r = DRE_rawSessSubmitSync(handle,
          &opData,
          src_desc.get(),
          src_idx,
          dst_desc.get(),
          num_dst_desc,
          nullptr);

        if(DRE_IS_RESULT_ERR(r)) {
          dout(1) << __func__ << " error: " << std::hex << r << std::dec << dendl;
          return -EFAULT;
        }
        ceph_assert(dstBufferLength > 0);
        ceph_assert(dstBufferLength <= dst_desc[0].len);

        // prefix with both full and compressed length
        denc((uint32_t)(src_len - prefix_len - extra_len), p);
        denc((uint32_t)dstBufferLength, p);
        p.get_pos_add(dstBufferLength);
      }
      src_len = prefix_len + extra_len;
      src_idx = 0;
    }
  }
  return 0;
}

int MxlCompressorSession::decompress(ceph::buffer::list::const_iterator &p,
                              size_t compressed_len,
                              ceph::buffer::list &dbl,
                              std::optional<int32_t> compressor_message)
{
  if (!opened || compress_mode) {
    return -EBADFD;
  }
  DRE_rawSyncOpData opData; /* Operation parameters - filled by user */
  DRE_u32b dstBufferLength = 0;

  memset(&opData, 0, sizeof(DRE_rawSyncOpData));

  //initialize user proviced buffers
  opData.nextHeader = nullptr;      // pointer to new header
  opData.dstLen = &dstBufferLength;
  opData.consumedByteCnt = nullptr; // pointer to consumed bytes counter

  opData.compCount.countMode = 0;
  opData.compCount.headCount = 0;
  opData.compCount.sourceCount = 0;

  uint32_t uncompressed_frame_len;
  uint32_t compressed_frame_len;
  size_t remaining = std::min<size_t>(p.get_remaining(), compressed_len);

  while (remaining >= sizeof(uint32_t) * 2) {
    ceph::decode(uncompressed_frame_len, p);
    ceph::decode(compressed_frame_len, p);
    remaining -= sizeof(uint32_t) * 2;
    if (!uncompressed_frame_len || !compressed_frame_len || !remaining)
      continue;
    if (compressed_frame_len > remaining) {
      return -ENODATA;
    }
    size_t src_idx = 0;

    while(compressed_frame_len && src_idx < num_src_desc) {
      const char* c_in;
      auto len = p.get_ptr_and_advance(compressed_frame_len, &c_in);
      remaining -= len;
      compressed_frame_len -= len;

      src_desc[src_idx].ptr = const_cast<char*>(c_in);
      src_desc[src_idx].len = len;
      ++src_idx;
    }
    ceph_assert(num_dst_desc == 1);

    dst_desc[0].len = std::min<uint32_t>(DST_BUF_SIZE, uncompressed_frame_len);
    bufferptr buf = buffer::create_page_aligned(dst_desc[0].len);
    dst_desc[0].ptr = buf.c_str();
    auto r = DRE_rawSessSubmitSync(handle,
      &opData,
      src_desc.get(),
      src_idx,
      dst_desc.get(),
      num_dst_desc,
      nullptr);

    if(DRE_IS_RESULT_ERR(r)) {
      dout(1) << __func__ << " error: " << std::hex << r << std::dec << dendl;
      return -EFAULT;
    }
    if (dstBufferLength > 0) {
      ceph_assert(dstBufferLength <= dst_desc[0].len);
      dbl.append(buf, 0, dstBufferLength);
    }
  }
  return 0;
}

MxlCompressor::MxlCompressor(CephContext *cct)
  : Compressor(COMP_ALG_MXL, "mxl"), cct(cct)
{
}

MxlCompressor::~MxlCompressor()
{
  close();
}

int MxlCompressor::open(std::ostream* ss)
{
  int r = 0;
  if (opened)
    return r;
  compress_sess_avail = new SessionStack(MAX_SESSIONS);
  decompress_sess_avail = new SessionStack(MAX_SESSIONS);
  for (size_t i = 0; i < MAX_SESSIONS; i++) {
    compress_sess[i].reset(new MxlCompressorSession(cct));
    r = compress_sess[i]->open(ss, true);
    if (r != 0) {
      close();
      return r;
    }
    compress_sess_avail->push(compress_sess[i].get());

    decompress_sess[i].reset(new MxlCompressorSession(cct));
    r = decompress_sess[i]->open(ss, false);
    if(r != 0) {
      close();
      return r;
    }
    decompress_sess_avail->push(decompress_sess[i].get());
  }
  opened = true;
  return 0;
}

void MxlCompressor::close()
{
  if (!opened)
    return;
  delete compress_sess_avail;
  compress_sess_avail = nullptr;

  delete decompress_sess_avail;
  decompress_sess_avail = nullptr;

  for (size_t i = 0; i < MAX_SESSIONS; i++) {
    compress_sess[i].reset();
    decompress_sess[i].reset();
  }
  opened = false;
}

int MxlCompressor::compress(const ceph::buffer::list &sbl,
                            ceph::buffer::list &dbl,
                            std::optional<int32_t> &compressor_message)
{
  MxlCompressorSession* sess = nullptr;
  ceph_assert(opened);

  while(!compress_sess_avail->pop(sess));

  ceph_assert(sess);
  int r =  sess->compress(sbl, dbl, compressor_message);
  compress_sess_avail->push(sess);
  return r;
}

int MxlCompressor::decompress(ceph::buffer::list::const_iterator &p,
                              size_t compressed_len,
                              ceph::buffer::list &dbl,
                              std::optional<int32_t> compressor_message)
{
  MxlCompressorSession* sess = nullptr;
  ceph_assert(opened);

  while(!decompress_sess_avail->pop(sess));

  ceph_assert(sess);
  int r = sess->decompress(p, compressed_len, dbl, compressor_message);
  decompress_sess_avail->push(sess);
  return r;
}
