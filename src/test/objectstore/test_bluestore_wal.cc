// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <string.h>
#include <string>
#include <vector>

#include "global/global_init.h"
#include "global/global_context.h"
#include "common/ceph_argparse.h"
#include "include/uuid.h"
#include "include/buffer.h"
#include <gtest/gtest.h>

#include "kv/KeyValueDB.h"
#include "os/bluestore/BlueStore.h"
#include "os/bluestore/BlueStoreWAL.h"

class BlueWALTestContext;
class BluestoreWALTester : public BluestoreWAL {
protected:
  struct TestTransactionImpl : public KeyValueDB::TransactionImpl {
    std::string bytes;
    using KeyValueDB::TransactionImpl::set;
    void set(
      const std::string&,
      const std::string&,
      const ceph::buffer::list&) override {}
    using KeyValueDB::TransactionImpl::rmkey;
    void rmkey(
      const std::string&,
      const std::string&) override {}
    void rmkeys_by_prefix(
      const std::string&) override {}
    void rm_range_keys(
      const std::string&,
      const std::string&,
      const std::string&) override {}
    const std::string& get_as_bytes() override {
      return bytes;
    }
    void set_from_bytes(const std::string& v) override {
      bytes = v;
    }
  };

  // following funcs made virtual to be able to make UT stubs for them
  void aio_write(uint64_t off,
    bufferlist& bl,
    IOContext* ioc,
    bool buffered) override {
    ceph_assert(0 == (off % get_block_size()));
    ceph_assert(0 == (bl.length() % get_block_size()));
    writes.emplace_back(std::make_pair(bl, ioc));

    auto off0 = off;
    auto off_end = off + bl.length();
    while (off < off_end) {
      bufferlist bl0;
      bl0.substr_of(bl, off - off0, get_block_size());
      content[off] = bl0;
      off += get_block_size();
    }
  }
  int read(uint64_t off,
           uint64_t len,
           bufferlist* bl,
           IOContext* ioc,
           bool buffered) override {
    ceph_assert(0 == (off % get_block_size()));
    ceph_assert(bl);
    reads.emplace_back(std::make_pair(off, len));
    auto off_end = off + len;
    auto it = content.find(off);
    while (off < off_end) {
      if (it != content.end() && it->first == off) {
        bl->append(it->second);
      } else {
        bl->append_zero(get_block_size());
      }
      off += get_block_size();
      if (it != content.end()) {
        it++;
      }
    }
    return 0;
  }
  void aio_submit(IOContext* ioc) override {
    ++aio_submits;
  }

public:
  size_t aio_submits = 0;
  std::map<uint64_t, bufferlist> content;
  std::vector<std::pair<uint64_t, uint64_t>> reads;
  std::vector<std::pair<bufferlist, IOContext*>> writes;
  std::vector<std::pair<uint64_t, void*>> completed_writes;

  BluestoreWALTester(CephContext* _cct,
		      BlockDevice* _bdev,
		      const uuid_d& _uuid,
		      uint64_t psize,
		      uint64_t bsize,
		      uint64_t fsize)
    : BluestoreWAL(_cct, _bdev, _uuid, psize, bsize, fsize) {
  }
  auto make_transaction(const std::string& bytes) {
    KeyValueDB::Transaction t(std::make_shared<TestTransactionImpl>());
    t->set_from_bytes(bytes);
    return t;
  }
  void reset_all() {
    content.clear();
    reads.clear();
    writes.clear();
    completed_writes.clear();
    aio_submits = 0;
  }
  void clone_content(BluestoreWALTester& target) {
    target.content = content;
  }

  uint64_t get_page_seqno() const {
    return page_seqno;
  }
  uint64_t get_transact_seqno() const {
    return cur_txc_seqno;
  }
  uint64_t get_last_submitted_page_seqno() const {
    return last_submitted_page_seqno;
  }
  uint64_t get_last_committed_page_seqno() const {
    return last_committed_page_seqno;
  }
  uint64_t get_last_wiping_page_seqno() const {
    return last_wiping_page_seqno;
  }
  uint64_t get_last_wiped_page_seqno() const {
    return last_wiped_page_seqno;
  }
};

class BlueWALTestContext : public BlueWALContext {
  IOContext ioc;
  BluestoreWALTester* tester = nullptr;
  void wal_aio_finish() override {
    ceph_assert(tester);
    tester->completed_writes.emplace_back(get_wal_seq(), this);
  }
  IOContext* get_ioc() override {
    return &ioc;
  }
  const std::string& get_payload() {
    return t->get_as_bytes();
  }
public:
  BlueWALTestContext() : ioc(nullptr, nullptr) {
  }

  KeyValueDB::Transaction t;
  void on_write_done(BluestoreWALTester* _tester) {
    tester = _tester;
    tester->aio_finish(this);
  }
};

TEST(BlueStoreWAL, basic) {
  uuid_d uuid;
  uuid.generate_random();

  BlueWALTestContext txc;
  BlueWALTestContext txc2;
  BlueWALTestContext txc3;
  BlueWALTestContext txc4;
  BlueWALTestContext txc5;
  BlueWALTestContext txc6;
  BlueWALTestContext txc7;
  BlueWALTestContext txc8;
  BlueWALTestContext txc9;
  BlueWALTestContext txc10;

  std::vector<std::string> transactions;

  uint64_t psize = 512;
  uint64_t bsize = 128;
  size_t page_count = 16;
  BluestoreWALTester t(g_ceph_context,
    nullptr,
    uuid,
    psize,
    bsize,
    psize * 2); // flush when 2 pages are ready
  t.init_add_pages(0, t.get_page_size() * 4);
  t.init_add_pages(1ull << 32, t.get_page_size() * (page_count - 4));

  ASSERT_EQ(psize, t.get_page_size());
  ASSERT_EQ(psize * 16, t.get_total());
  ASSERT_EQ(psize * 16, t.get_avail());

  // just to make sure we operate headers small enough for block/page sizes
  // used in this test suite
  auto head_size = t.get_header_size();
  ASSERT_EQ(50, head_size);

  size_t flush_cnt = 0;
  /////////////////////////////////////////////////
  std::cout << "Write 1, short transact" << std::endl;
  transactions.push_back("some transact");
  auto expected_w1 =
    round_up_to(head_size + transactions[0].length(), bsize);
  txc.t = t.make_transaction(transactions[0]);
  t.log(&txc);

  ASSERT_EQ(1, t.writes.size());
  ASSERT_EQ(expected_w1, t.writes[0].first.length());
  ASSERT_EQ(1, t.aio_submits);
  ASSERT_EQ(0, t.completed_writes.size());
  ASSERT_EQ(
    expected_w1,
    t.get_total() - t.get_avail());

  txc.on_write_done(&t);
  ASSERT_EQ(1, t.completed_writes.size());
  ASSERT_EQ(1, t.completed_writes[0].first); // cur_page_seqno = 1
  ASSERT_EQ(&txc, t.completed_writes[0].second);
  // indicate txc has been submitted to DB
  t.submitted(&txc,
    [&]{
      ASSERT_TRUE(false); // we shouldn't get here at this point
    });
  ASSERT_GE(t.get_total(), t.get_avail());

  /////////////////////////////////////////////////
  // another op which consumes the same page
  std::cout << "Write 2, another short transact" << std::endl;
  transactions.emplace_back(std::string("another transact"));

  auto expected_w2 =
    round_up_to(head_size + transactions.back().length(), bsize);
  txc2.t = t.make_transaction(transactions.back());
  t.log(&txc2);

  ASSERT_EQ(2, t.writes.size());
  ASSERT_EQ(expected_w2, t.writes[1].first.length());
  ASSERT_EQ(2, t.aio_submits);
  ASSERT_EQ(1, t.completed_writes.size());
  ASSERT_EQ(
    expected_w1 + expected_w2,
    t.get_total() - t.get_avail());

  txc2.on_write_done(&t);
  ASSERT_EQ(2, t.completed_writes.size());
  ASSERT_EQ(1, t.completed_writes[1].first); // still cur_page_seqno = 1
  ASSERT_EQ(&txc2, t.completed_writes[1].second);

  //no-op
  t.submitted(&txc2,
    [&]{
      ASSERT_TRUE(false); // we shouldn't get here at this point
    });

  /////////////////////////////////////////////////
  // another op which consumes the next page = 2
  transactions.push_back(std::string(psize - head_size, 'a'));
  auto expected_w3 =
    round_up_to(head_size + transactions.back().length(), bsize);
  std::cout << "Write 3, len = " << expected_w3 << std::endl;
  txc3.t = t.make_transaction(transactions.back());
  t.log(&txc3);

  ASSERT_EQ(3, t.writes.size());
  ASSERT_EQ(expected_w3, t.writes[2].first.length());
  ASSERT_EQ(3, t.aio_submits);
  ASSERT_EQ(2, t.completed_writes.size());
  ASSERT_EQ(
    psize // the first page is completely consumed although
	  // the tail is unused
      + expected_w3,
    t.get_total() - t.get_avail());

  txc3.on_write_done(&t);
  ASSERT_EQ(3, t.completed_writes.size());
  ASSERT_EQ(2, t.completed_writes[2].first); // cur page seqno = 2
  ASSERT_EQ(&txc3, t.completed_writes[2].second);

  // indicate everything prior to page 2 is committed
  t.submitted(&txc3,
    [&]{
      ASSERT_TRUE(false); // we shouldn't get here at this point
    });
  //avail is unchanged
  ASSERT_EQ(
    psize // the first page is completely consumed although
	  // the tail is unused
      + expected_w3,
    t.get_total() - t.get_avail());

  /////////////////////////////////////////////////
  // forth op which fully consumes two following pages but the last block.
  // pages prior to seq 3 to be indicated as exausted
  transactions.push_back(std::string(2 * (psize - head_size) - bsize, 'b'));
  auto expected_w4 =
    round_up_to(2 * head_size + transactions.back().length(), psize);
  std::cout << "Write 4, len = " << expected_w4 << std::endl;
  txc4.t = t.make_transaction(transactions.back());
  t.log(&txc4);

  ASSERT_EQ(5, t.writes.size()); // +2 page writes
  ASSERT_EQ(psize, t.writes[3].first.length());
  ASSERT_EQ(psize - bsize, t.writes[4].first.length());
  ASSERT_EQ(t.writes[3].second, t.writes[4].second); // IOCs are equal

  ASSERT_EQ(4, t.aio_submits);
  ASSERT_EQ(3, t.completed_writes.size());
  ASSERT_EQ(
    psize
      + expected_w3 + expected_w4,
    t.get_total() - t.get_avail());

  txc4.on_write_done(&t);
  ASSERT_EQ(4, t.completed_writes.size());

  // cur page = 5 as huge write issues "self-submission"
  // indications
  ASSERT_EQ(5, t.completed_writes[3].first);
  ASSERT_EQ(&txc4, t.completed_writes[3].second);
  flush_cnt = 0;
  t.submitted(&txc4, // indicate everything prior to page 5
                     // this includes txc4 itself
    [&]{
      flush_cnt++;
    });
  ASSERT_EQ(flush_cnt, 1);

  //page 1-4 aren not wiped but available
  ASSERT_EQ(
    0,
    t.get_total() - t.get_avail());

  /////////////////////////////////////////////////
  // fifth op just regularly uses current page
  transactions.push_back(std::string(bsize, 'c'));

  auto expected_w5 =
    round_up_to(head_size + transactions[4].length(), bsize);
  std::cout << "Write 5, len = " << expected_w5 << std::endl;
  txc5.t = t.make_transaction(transactions.back());
  t.log(&txc5);

  ASSERT_EQ(5 + 1 + 4, t.writes.size()); //4 page wipings + 1 block write
  ASSERT_EQ(bsize, t.writes[5].first.length());
  ASSERT_EQ(bsize, t.writes[6].first.length());
  ASSERT_EQ(bsize, t.writes[7].first.length());
  ASSERT_EQ(bsize, t.writes[8].first.length());
  ASSERT_EQ(expected_w5, t.writes[9].first.length());
  ASSERT_EQ(t.writes[5].second, t.writes[6].second); // the same iocs
  ASSERT_EQ(t.writes[5].second, t.writes[7].second); // the same iocs
  ASSERT_EQ(t.writes[5].second, t.writes[8].second); // the same iocs
  ASSERT_EQ(t.writes[5].second, t.writes[9].second); // the same iocs
  ASSERT_EQ(5, t.aio_submits);
  ASSERT_EQ(4, t.completed_writes.size()); // no change
  ASSERT_EQ(
    psize * 4     // page 1-4 are being wiped
    + expected_w5,
    t.get_total() - t.get_avail());

  txc5.on_write_done(&t);
  ASSERT_EQ(5, t.completed_writes.size());
  ASSERT_EQ(&txc5, t.completed_writes[4].second);
  ASSERT_EQ(5, t.completed_writes[4].first); // cur page = 5

  //pages 1-2 are now wiped
  ASSERT_EQ(
    + expected_w5,
    t.get_total() - t.get_avail());

  
  /////////////////////////////////////////////////
  //3 more transactions - last one starts new page 6
  // txc 5 is not submitted yet!!!!
  //
  t.reset_all();
  transactions.push_back(std::string(8, 'd'));
  auto expected_w6 =
    round_up_to(head_size + transactions.back().length(), bsize);
  std::cout << "Write 6-8, len = 3x" << expected_w6 << std::endl;
  txc6.t = t.make_transaction(transactions.back());
  t.log(&txc6);

  transactions.push_back(std::string(8, 'd'));
  auto expected_w7 = expected_w6;
  txc7.t = t.make_transaction(transactions.back());
  t.log(&txc7);

  transactions.push_back(std::string(8, 'd'));
  auto expected_w8 = expected_w6;
  txc8.t = t.make_transaction(transactions.back());
  t.log(&txc8);

  ASSERT_EQ(3, t.writes.size());
  ASSERT_EQ(expected_w6, t.writes[0].first.length());
  ASSERT_EQ(expected_w7, t.writes[1].first.length());
  ASSERT_EQ(expected_w8, t.writes[2].first.length());

  ASSERT_EQ(3, t.aio_submits);
  ASSERT_EQ(0, t.completed_writes.size()); // no change
  ASSERT_EQ(
    + psize       // ops 5-7 consume this page
    + expected_w8,
    t.get_total() - t.get_avail());

  txc8.on_write_done(&t);
  ASSERT_EQ(0, t.completed_writes.size()); // still wait for prior write to complete
  txc7.on_write_done(&t);
  ASSERT_EQ(0, t.completed_writes.size()); // still wait for prior write to complete
  txc6.on_write_done(&t);
  ASSERT_EQ(3, t.completed_writes.size()); // 3 pending writes are completed

  ASSERT_EQ(5, t.completed_writes[0].first);
  ASSERT_EQ(&txc6, t.completed_writes[0].second); // cur page = 5
  ASSERT_EQ(5, t.completed_writes[1].first);
  ASSERT_EQ(&txc7, t.completed_writes[1].second); // cur page = 5
  ASSERT_EQ(6, t.completed_writes[2].first);
  ASSERT_EQ(&txc8, t.completed_writes[2].second); // cur page = 6

  // ops 4-8 are not submitted yet!!!!
  ASSERT_EQ(
    + psize       // ops 5-7 consume this page
    + expected_w8,
    t.get_total() - t.get_avail());

  flush_cnt = 0;
  t.submitted(&txc5, // no-op
    [&]{
      flush_cnt++;
    });
  ASSERT_EQ(flush_cnt, 0);

  ASSERT_EQ(
    + psize       // ops 5-7 consume this page
    + expected_w8,
    t.get_total() - t.get_avail());

  // current *seqno validation
  ASSERT_EQ(t.get_page_seqno(), 6);
  ASSERT_EQ(t.get_transact_seqno(), 8);

  ASSERT_EQ(t.get_last_submitted_page_seqno(), 4);
  ASSERT_EQ(t.get_last_committed_page_seqno(), 4);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 4);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 4);

  flush_cnt = 0;
  t.submitted(&txc6, // no-op, page 5 again
    [&]{
      ++flush_cnt;
    });
  ASSERT_EQ(flush_cnt, 0);

  flush_cnt = 0;
  t.submitted(&txc8, // page 6, not enough for a flush, no pages released
    [&]{
      ++flush_cnt;
    });
  ASSERT_EQ(flush_cnt, 0);

  ASSERT_EQ(
    + psize
    + expected_w8,
    t.get_total() - t.get_avail());
  ASSERT_EQ(t.get_last_submitted_page_seqno(), 5);
  ASSERT_EQ(t.get_last_committed_page_seqno(), 4);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 4);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 4);

  /////////////////////////////////////////////////
  //1 huge transaction to take all the space but two pages
  // (including the current page one)  and a few(5) bytes
  //
  t.reset_all();
  auto num_pages_w9 = page_count - 2; // = 14
  auto page_seqno_before_w9 = t.get_page_seqno();
  transactions.push_back(
    std::string(num_pages_w9 * (psize - head_size) - 5, 'e'));
  auto expected_w9 = round_up_to(transactions.back().length(), bsize);
  std::cout << "Write 9, len = " << expected_w9 << std::endl;
  txc9.t = t.make_transaction(transactions.back());
  t.log(&txc9);

  ASSERT_EQ(num_pages_w9, t.writes.size());
  for (size_t i = 0; i < t.writes.size(); ++i) {
    ASSERT_EQ(psize, t.writes[i].first.length());
  }

  ASSERT_EQ(1, t.aio_submits);
  ASSERT_EQ(0, t.completed_writes.size());
  ASSERT_EQ(
    0,
    t.get_avail());

  // page seqno to be increased by num_pages_w9 due to huge txc alignment
  // with page boundary
  ASSERT_EQ(t.get_page_seqno(), num_pages_w9 + page_seqno_before_w9);
  ASSERT_EQ(t.get_transact_seqno(), 9);

  ASSERT_EQ(t.get_last_submitted_page_seqno(), 5);
  ASSERT_EQ(t.get_last_committed_page_seqno(), 4);
  // two unwiped pages are consumed by this txc hence seqs updated
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 4);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 4);

  txc9.on_write_done(&t);
  ASSERT_EQ(1 , t.completed_writes.size());

  ASSERT_EQ(&txc9, t.completed_writes[0].second);
  // page seqno to be increased by 1 due to "self-submission"
  // indication for a huge page
  ASSERT_EQ(t.get_page_seqno() + 1, t.completed_writes[0].first); // cur page = 21

  flush_cnt = 0;
  t.submitted(&txc9, // pages 4 - 20
    [&]{
      ++flush_cnt;
    });
  ASSERT_EQ(flush_cnt, 1);
  ASSERT_EQ(
    t.get_total(),
    t.get_avail());

  ASSERT_EQ(t.get_last_submitted_page_seqno(), 20);
  ASSERT_EQ(t.get_last_committed_page_seqno(), 20);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 4);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 4);

  /////////////////////////////////////////////////
  //simple transaction to trigger wiping for
  //submitted pages
  //
  t.reset_all();
  transactions.push_back(std::string(1, 'f'));
  auto expected_w10 =
    round_up_to(head_size + transactions.back().length(), bsize);
  std::cout << "Write 10, len = " << expected_w10 << std::endl;
  txc10.t = t.make_transaction(transactions.back());
  t.log(&txc10);
  txc10.on_write_done(&t);
  flush_cnt = 0;
  t.submitted(&txc10, // no-op
    [&]{
      ++flush_cnt;
    });
  ASSERT_EQ(0, flush_cnt);
  ASSERT_EQ(
    expected_w10,
    t.get_total() - t.get_avail());

  ASSERT_EQ(t.get_last_submitted_page_seqno(), 20);
  ASSERT_EQ(t.get_last_committed_page_seqno(), 20);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 20);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 20);

/*  // and one more simple op piggy backed with page wipe
  bl.clear();
  bl.append(string("aaa"));
  transactions.push_back(bl);

  t.submit(&txc2, transactions[9]);

  // +1 write for the op + 15 writes to wipe pages
  ASSERT_EQ(26 + page_count, t.writes.size());
  ASSERT_EQ(10, t.aio_submits);
  ASSERT_EQ(9, t.completed_writes.size());
  // less than a single page is avail atm
  uint64_t expected_w10 = round_up_to(phead_size + thead_size + bl.length(), bsize);
  ASSERT_EQ(
    psize - expected_w10,
    t.get_avail());

  ASSERT_EQ(t.get_page_seqno(), 22);
  ASSERT_EQ(t.get_transact_seqno(), 10);

  ASSERT_EQ(t.get_last_outdated_page_seqno(), 21);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 21);
  // we've just reused a single non-wiped page
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 6);

  t.on_write(t.writes[20].second);
  ASSERT_EQ(10, t.completed_writes.size());

  // duplicate request for commit indication for page seq 21
  ASSERT_EQ(21, t.completed_writes[9].first);
  ASSERT_EQ(&txc2, t.completed_writes[9].second);

  // less than a single page is avail atm
  ASSERT_EQ(
    psize * page_count - expected_w10,
    t.get_avail());

  ASSERT_EQ(t.get_last_outdated_page_seqno(), 21);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 21);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 21);

  t.committed(21); // duplicate commit for page seq 21
  ASSERT_EQ(
    t.get_total() - expected_w10,
    t.get_avail());
  ASSERT_EQ(t.get_last_outdated_page_seqno(), 21);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 21);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 21);

  // 
  // FIXME: verify shutdown - wipe all the pages
  */
}

TEST(BlueStoreWAL, basic_replay) {
  uuid_d uuid;
  uuid.generate_random();

  BlueWALTestContext txc;
  BlueWALTestContext txc2;
  BlueWALTestContext txc3;
  BlueWALTestContext txc4;
  BlueWALTestContext txc5;
  BlueWALTestContext txc6;
  BlueWALTestContext txc7;
  BlueWALTestContext txc8;
  BlueWALTestContext txc9;
  BlueWALTestContext txc10;

  std::vector<std::string> transactions;

  uint64_t psize = 1024;
  uint64_t bsize = 256;
  size_t page_count = 8;
  BluestoreWALTester t(g_ceph_context,
    nullptr,
    uuid,
    psize,
    bsize,
    psize * 4);
  t.init_add_pages(0, t.get_page_size() * 4);
  t.init_add_pages(1ull << 16, t.get_page_size() * 3);
  t.init_add_pages(1ull << 24, t.get_page_size() * 1);

  BluestoreWALTester t2(g_ceph_context,
    nullptr,
    uuid,
    psize,
    bsize,
    psize * 4);
  t2.init_add_pages(0, t2.get_page_size() * 4);
  t2.init_add_pages(1ull << 16, t2.get_page_size() * 3);
  t2.init_add_pages(1ull << 24, t2.get_page_size() * 1);

  auto head_size = t.get_header_size();

  int flush_cnt = 0;
  int db_submit_cnt = 0;
  std::vector<std::string> db_submit_data;
  auto submit_db_fn = [&](const std::string& payload) {
    ++db_submit_cnt;
    db_submit_data.emplace_back(payload);
    return 0;
  };
  auto flush_db_fn = [&]() {
    ++flush_cnt;
  };

  flush_cnt = 0;
  t.shutdown(flush_db_fn);
  ASSERT_EQ(flush_cnt, 1);

  db_submit_cnt = 0;
  flush_cnt = 0;
  t.replay(true, submit_db_fn, flush_db_fn);
  ASSERT_EQ(t.get_total(), t.get_avail());
  ASSERT_EQ(db_submit_cnt, 0);
  ASSERT_EQ(flush_cnt, 0);
  ASSERT_EQ(page_count, t.reads.size());
  ASSERT_EQ(0, t.reads[0].first);
  ASSERT_EQ(bsize, t.reads[0].second);
  ASSERT_EQ(1 * psize, t.reads[1].first);
  ASSERT_EQ(bsize, t.reads[1].second);
  ASSERT_EQ(2 * psize, t.reads[2].first);
  ASSERT_EQ(bsize, t.reads[2].second);
  ASSERT_EQ(3 * psize, t.reads[3].first);
  ASSERT_EQ(bsize, t.reads[3].second);
  ASSERT_EQ((1ull << 16) + 0 * psize, t.reads[4].first);
  ASSERT_EQ(bsize, t.reads[4].second);
  ASSERT_EQ((1ull << 16) + 1 * psize, t.reads[5].first);
  ASSERT_EQ(bsize, t.reads[5].second);
  ASSERT_EQ((1ull << 16) + 2 * psize, t.reads[6].first);
  ASSERT_EQ(bsize, t.reads[6].second);
  ASSERT_EQ((1ull << 24) + 0 * psize, t.reads[7].first);
  ASSERT_EQ(bsize, t.reads[7].second);
  ASSERT_EQ(0, t.writes.size());

  t.reset_all();
  // this will use a single block
  transactions.push_back("transact1");
  // takes all the rest blocks in the page but the last one
  transactions.push_back(std::string(psize - 2 * bsize - head_size - 1, '2'));
  // starts using the next page, all but last two blocks
  transactions.push_back(std::string(psize - 2 * bsize - head_size, '3'));
  // txc 4 & 5 to be merged within a single block
  transactions.push_back(std::string(bsize - head_size * 2 - 1, '4'));
  transactions.push_back("5");
  // take almost 1 block + 1 byte hence causing next page allocation
  transactions.push_back(std::string(bsize + 1 - head_size, '6'));
  // txc 6 & 7 & 8 to be merged within a single block
  transactions.push_back("77");
  transactions.push_back("888");
  // txc 9 is taking all the block but a few bytes (not enough for the header),
  // txc 10 to take the next block
  transactions.push_back(std::string(bsize - head_size - head_size + 5, '9'));
  transactions.push_back("10");

  txc.t = t.make_transaction(transactions[0]);
  t.log(&txc);
  txc2.t = t.make_transaction(transactions[1]);
  t.log(&txc2);
  txc3.t = t.make_transaction(transactions[2]);
  t.log(&txc3);
  txc4.t = t.make_transaction(transactions[3]);
  txc4.set_more_aio_finish(true);
  t.log(&txc4);
  txc5.t = t.make_transaction(transactions[4]);
  t.log(&txc5);

  txc6.t = t.make_transaction(transactions[5]);
  txc6.set_more_aio_finish(true);
  t.log(&txc6);
  txc7.t = t.make_transaction(transactions[6]);
  txc7.set_more_aio_finish(true);
  t.log(&txc7);
  txc8.t = t.make_transaction(transactions[7]);
  t.log(&txc8);
  txc9.t = t.make_transaction(transactions[8]);
  t.log(&txc9);
  txc10.t = t.make_transaction(transactions[9]);
  t.log(&txc10);

  // txc 4 & 5 are merged into a single write, the same for txc 6 & 7 & 8
  ASSERT_EQ(7, t.writes.size());
  ASSERT_EQ(7, t.aio_submits);
  ASSERT_EQ(0, t.completed_writes.size());

  // just to reset wal op context in txc
  // and hence be able to use it again
  txc.on_write_done(&t);

  flush_cnt = 0;
  t.shutdown(flush_db_fn);
  ASSERT_EQ(flush_cnt, 1);

  // clone disk content to t2 to be able to use "pure" WAL object
  t.clone_content(t2);

  db_submit_cnt = 0;
  flush_cnt = 0;
  t2.replay(true, submit_db_fn, flush_db_fn);
  ASSERT_EQ(t2.get_total(), t2.get_avail() + bsize); // dummy txc expected
  ASSERT_EQ(db_submit_cnt, 10);
  for (size_t i = 0; i < (size_t)db_submit_cnt; i++) {
    if (db_submit_data[i] != transactions[i]) {
      fprintf(stderr, " mismatch at %lu\n", i);
    }
    ASSERT_EQ(db_submit_data[i], transactions[i]);
  }
  ASSERT_EQ(flush_cnt, 1);
  ASSERT_EQ(
    + 1 // block for txc1
    + 2 // 2 blocks for txc2: head + tail
    + 1 //1 unused block at page 0
    + 2 //2 blocks for txc3: head + tail
    + 1 //1 block for txc4 + 5
    + 1 //1 unused block at page 1
    + 2 // 2 blocks at page 2 for txc 6 & 7 & 8
    + 2 // 2 blocks at page 2 for txc 9 & 10
    + page_count,
    t2.reads.size());
  ASSERT_EQ(4, t2.writes.size()); //3 pages wiped + dummy txc

  t2.writes.clear();
  t2.reads.clear();
  db_submit_data.clear();
  flush_cnt = 0;
  db_submit_cnt = 0;
  t2.shutdown(flush_db_fn);
  ASSERT_EQ(flush_cnt, 1);
  ASSERT_EQ(0, t2.writes.size());

  db_submit_cnt = 0;
  flush_cnt = 0;
  t2.replay(true, submit_db_fn, flush_db_fn);
  ASSERT_EQ(t2.get_total(), t2.get_avail() + bsize); //dummy txc expected
  ASSERT_EQ(db_submit_cnt, 0);
  ASSERT_EQ(flush_cnt, 0);
  ASSERT_EQ(
    + 1 // 1 dummy txc block
    + 1 // 1 unused block at the end
    + page_count,
    t2.reads.size());

  {
    // yet another new instance for the same WAL
    BluestoreWALTester t3(g_ceph_context,
      nullptr,
      uuid,
      psize,
      bsize,
      psize * 4);
    t3.init_add_pages(0, t3.get_page_size() * 4);
    t3.init_add_pages(1ull << 16, t3.get_page_size() * 3);
    t3.init_add_pages(1ull << 24, t3.get_page_size() * 1);
    t2.clone_content(t3);

    db_submit_cnt = 0;
    flush_cnt = 0;
    t3.replay(true, submit_db_fn, flush_db_fn);
    ASSERT_EQ(t3.get_total(), t3.get_avail() + bsize); //dummy txc expected
    ASSERT_EQ(db_submit_cnt, 0);
    ASSERT_EQ(flush_cnt, 0);
    ASSERT_EQ(
      + 1 // 1 dummy txc block
      + 1 // 1 unused block at the end
      + page_count,
      t3.reads.size());

    t3.log(&txc);
    txc.on_write_done(&t3);
    ASSERT_EQ(1, t3.completed_writes.size());
    ASSERT_EQ(&txc, t3.completed_writes[0].second);
    ASSERT_EQ(6, t3.completed_writes[0].first); // cur_page_seqno = 6
  }
}

int main(int argc, char **argv) {
  std::vector<const char*> args;
  args = argv_to_vec(argc, (const char **)argv);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf.apply_changes(nullptr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
