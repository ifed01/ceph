// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <string.h>
#include <vector>

#include "global/global_init.h"
#include "global/global_context.h"
#include "common/ceph_argparse.h"
#include "include/uuid.h"
#include "include/buffer.h"
#include <gtest/gtest.h>

#include "os/bluestore/BlueStore.h"
#include "os/bluestore/BlueStoreWAL.h"

class BluestoreWALTester : public BluestoreWAL {
protected:
  // following funcs made virtual to be able to make UT stubs for them
  void notify_store(BlueStore* store, uint64_t seqno, void* txc) override {
    completed_writes.emplace_back(seqno, txc);
  }
  void aio_write(uint64_t off,
    bufferlist& bl,
    IOContext* ioc,
    bool buffered) override {
    writes.emplace_back(bl, ioc);
  }
  void aio_submit(IOContext* ioc) override {
    ++submits;
  }

public:
  size_t submits = 0;
  std::vector<std::pair<bufferlist, IOContext*>> writes;
  std::vector<std::pair<uint64_t, void*>> completed_writes;

  BluestoreWALTester(CephContext* _cct,
		      BlockDevice* _bdev,
		      const uuid_d& _uuid,
		      uint64_t psize,
		      uint64_t bsize)
    : BluestoreWAL(_cct, _bdev, _uuid, psize, bsize) {
  }
  void on_write(IOContext* ioc) {
    BluestoreWAL::Op* op = (BluestoreWAL::Op*)ioc->priv;
    aio_finish(nullptr, *op);
  }

  uint64_t get_page_seqno() const {
    return page_seqno;
  }
  uint64_t get_transact_seqno() const {
    return transact_seqno;
  }
  uint64_t get_last_outdated_page_seqno() const {
    return last_outdated_page_seqno;
  }
  uint64_t get_last_wiping_page_seqno() const {
    return last_wiping_page_seqno;
  }
  uint64_t get_last_wiped_page_seqno() const {
    return last_wiped_page_seqno;
  }
};


TEST(BlueStoreWAL, basic) {
  uuid_d uuid;
  uuid.generate_random();

  BlueStore::TransContext txc(g_ceph_context, nullptr, nullptr, nullptr);
  BlueStore::TransContext txc2(g_ceph_context, nullptr, nullptr, nullptr);
  BlueStore::TransContext txc3(g_ceph_context, nullptr, nullptr, nullptr);

  std::vector<bufferlist> transactions;

  bufferlist bl;
  bl.append(string("some transact"));
  transactions.push_back(bl);

  uint64_t psize = 512;
  uint64_t bsize = 128;
  size_t page_count = 16;
  BluestoreWALTester t(g_ceph_context,
    nullptr,
    uuid,
    psize,
    bsize);
  t.init_add_pages(0, t.get_page_size() * 4);
  t.init_add_pages(1ull << 32, t.get_page_size() * (page_count - 4));

  ASSERT_EQ(psize, t.get_page_size());
  ASSERT_EQ(psize * 16, t.get_total());
  ASSERT_EQ(psize * 16, t.get_avail());

  // just to make sure we operate headers small enough for block/page sizes
  // used in this test suite
  auto phead_size = t.get_page_header_size();
  auto thead_size = t.get_transact_header_size();
  ASSERT_EQ(34, phead_size);
  ASSERT_EQ(38, thead_size);

  auto expected_w1 =
    round_up_to(phead_size + thead_size + transactions[0].length(), bsize);
  t.submit(&txc, transactions[0]);

  ASSERT_EQ(1, t.writes.size());
  ASSERT_EQ(expected_w1, t.writes[0].first.length());
  ASSERT_EQ(1, t.submits);
  ASSERT_EQ(0, t.completed_writes.size());
  ASSERT_EQ(
    expected_w1,
    t.get_total() - t.get_avail());

  t.on_write(t.writes[0].second);
  ASSERT_EQ(1, t.completed_writes.size());
  ASSERT_EQ(0, t.completed_writes[0].first); // prev_page_seqno
  ASSERT_EQ(&txc, t.completed_writes[0].second);
  
  t.committed(0); // fake commit ack
  ASSERT_GE(t.get_total(), t.get_avail());

  // another op which consumes the same page
  bl.clear();
  bl.append(string("another transact"));
  transactions.emplace_back(bl);

  auto expected_w2 =
    round_up_to(thead_size + transactions[1].length(), bsize);
  t.submit(&txc2, transactions[1]);

  ASSERT_EQ(2, t.writes.size());
  ASSERT_EQ(expected_w2, t.writes[1].first.length());
  ASSERT_EQ(2, t.submits);
  ASSERT_EQ(1, t.completed_writes.size());
  ASSERT_EQ(
    expected_w1 + expected_w2,
    t.get_total() - t.get_avail());

  t.on_write(t.writes[1].second);
  ASSERT_EQ(2, t.completed_writes.size());
  ASSERT_EQ(0, t.completed_writes[1].first); // still invalid prev_page_seqno
  ASSERT_EQ(&txc2, t.completed_writes[1].second);

  // another op which consumes the next page
  bl.clear();
  bl.append(string(psize - phead_size - thead_size, 'a'));
  transactions.push_back(bl);

  auto expected_w3 =
    round_up_to(phead_size + thead_size + transactions[2].length(), bsize);
  t.submit(&txc, transactions[2]);

  ASSERT_EQ(3, t.writes.size());
  ASSERT_EQ(expected_w3, t.writes[2].first.length());
  ASSERT_EQ(3, t.submits);
  ASSERT_EQ(2, t.completed_writes.size());
  ASSERT_EQ(
    psize // the first page is completely consumed although
	  // the tail is unused
      + expected_w3,
    t.get_total() - t.get_avail());

  t.on_write(t.writes[2].second);
  ASSERT_EQ(3, t.completed_writes.size());
  // page 1 to be exhausted (aka prev_page_seqno)
  ASSERT_EQ(1, t.completed_writes[2].first);
  ASSERT_EQ(&txc, t.completed_writes[2].second);

  t.committed(1); // indicate everything from page 1 is committed
  //first page is now available (but still not wiped)
  ASSERT_EQ(
    expected_w3,
    t.get_total() - t.get_avail());

  // forth op which fully consumes two following pages but the last block
  // page 2 to be indicated as exausted
  bl.clear();
  bl.append(string(2 * (psize - phead_size - thead_size) - bsize, 'b'));
  transactions.push_back(bl);

  auto expected_w4 =
    round_up_to(2 * (phead_size + thead_size) + transactions[3].length(), bsize);
  t.submit(&txc2, transactions[3]);

  ASSERT_EQ(6, t.writes.size()); // 2 pages writes + piggy back to wipe page 1
  ASSERT_EQ(psize, t.writes[3].first.length());
  ASSERT_EQ(expected_w4 - psize, t.writes[4].first.length());
  ASSERT_EQ(bsize, t.writes[5].first.length());
  ASSERT_EQ(t.writes[3].second, t.writes[4].second);
  ASSERT_EQ(t.writes[3].second, t.writes[5].second);

  ASSERT_EQ(4, t.submits);
  ASSERT_EQ(3, t.completed_writes.size());
  ASSERT_EQ(
    psize // the first page is marked as used till wiping completion
      +expected_w3 + expected_w4,
    t.get_total() - t.get_avail());

  t.on_write(t.writes[5].second);
  ASSERT_EQ(4, t.completed_writes.size());

  // page 2 to be indicated as exhausted (aka prev_page_seqno)
  ASSERT_EQ(2, t.completed_writes[3].first);
  ASSERT_EQ(&txc2, t.completed_writes[3].second);
  //page 1 is now wiped
  //commit for page 2 (=expected_w3) is still pending
  ASSERT_EQ(
    expected_w3 + expected_w4,
    t.get_total() - t.get_avail());

  t.committed(2); // indicate everything from page 2 is committed
  //page 1 is now wiped
  //page 2 is available but still not wiped
  ASSERT_EQ(
    expected_w4,
    t.get_total() - t.get_avail());

  // final op which doesn't fit into the current page hence starts using the next one
  bl.clear();
  bl.append(string(bsize, 'c'));
  transactions.push_back(bl);

  auto expected_w5 =
    round_up_to(phead_size + thead_size + transactions[4].length(), bsize);
  t.submit(&txc, transactions[4]);

  ASSERT_EQ(8, t.writes.size()); //additional page 2 wiping write
  ASSERT_EQ(expected_w5, t.writes[6].first.length());
  ASSERT_EQ(t.writes[6].second, t.writes[7].second); // the same ioc
  ASSERT_EQ(5, t.submits);
  ASSERT_EQ(4, t.completed_writes.size());
  ASSERT_EQ(
    psize * 2 // page 3-4 are completely consumed although
	      // the tail is unused
    + psize   // page 2 marked as unavailable due to wiping
    + expected_w5,
    t.get_total() - t.get_avail());

  t.on_write(t.writes[6].second);
  ASSERT_EQ(5, t.completed_writes.size());

  // pages 3-4 to be indicated as exhausted (aka prev_page_seqno)
  ASSERT_EQ(4, t.completed_writes[4].first);
  ASSERT_EQ(&txc, t.completed_writes[4].second);
  //page 2 is now wiped too
  ASSERT_EQ(
    psize * 2 // page 3-4 are still in use
      + expected_w5,
    t.get_total() - t.get_avail());

  // op 5 is not committed yet!!!!
  
  //3 more parallel transactions - last one starts new page 6
  //
  bl.clear();
  bl.append(string(8, 'd'));
  transactions.push_back(bl);
  transactions.push_back(bl);
  transactions.push_back(bl);

  auto expected_w6 =
    round_up_to(thead_size + transactions[5].length(), bsize);
  auto expected_w7 = expected_w6;
  auto expected_w8 =
    round_up_to(phead_size + thead_size + transactions[7].length(), bsize);

  t.submit(&txc, transactions[5]);
  t.submit(&txc2, transactions[6]);
  t.submit(&txc3, transactions[7]);

  ASSERT_EQ(11, t.writes.size());
  ASSERT_EQ(expected_w6, t.writes[8].first.length());
  ASSERT_EQ(expected_w7, t.writes[9].first.length());
  ASSERT_EQ(expected_w8, t.writes[10].first.length());

  ASSERT_EQ(8, t.submits);
  ASSERT_EQ(5, t.completed_writes.size());
  ASSERT_EQ(
    psize * 2	  // page 3-4 are still pending commit
    + psize       // ops 5-7 consume this page
    + expected_w8,
    t.get_total() - t.get_avail());

  t.on_write(t.writes[10].second);
  ASSERT_EQ(5, t.completed_writes.size()); // still wait for prior ops to complete
  t.on_write(t.writes[9].second);
  ASSERT_EQ(5, t.completed_writes.size()); // still wait for prior ops to complete
  t.on_write(t.writes[8].second);
  ASSERT_EQ(8, t.completed_writes.size()); // 3 pending ops are completed

  ASSERT_EQ(0, t.completed_writes[5].first);
  ASSERT_EQ(&txc, t.completed_writes[5].second);
  ASSERT_EQ(0, t.completed_writes[6].first);
  ASSERT_EQ(&txc2, t.completed_writes[6].second);
  ASSERT_EQ(5, t.completed_writes[7].first);
  ASSERT_EQ(&txc3, t.completed_writes[7].second);

  // ops 4-8 are not committed yet!!!!
  ASSERT_EQ(
    psize * 2	    // page 3-4 are still pending commit
      + psize       // ops 5-7 consume this page
      + expected_w8,
    t.get_total() - t.get_avail());

  t.committed(5); // commits pages 3-4-5 (ops 4-7)
  ASSERT_EQ(
    + expected_w8,
    t.get_total() - t.get_avail());

  // current *seqno validation
  ASSERT_EQ(t.get_page_seqno(), 6);
  ASSERT_EQ(t.get_transact_seqno(), 8);

  ASSERT_EQ(t.get_last_outdated_page_seqno(), 5);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 2);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 2);

  //1 huge transaction to take all the space but current page
  //
  bl.clear();
  bl.append(string((page_count - 1) * (psize - phead_size - thead_size), 'e'));
  transactions.push_back(bl);

  t.submit(&txc, transactions[8]);

  ASSERT_EQ(11 + page_count - 1, t.writes.size()); // = 26
  for (size_t i = 11; i < t.writes.size(); ++i) {
    ASSERT_EQ(psize, t.writes[i].first.length());
  }

  ASSERT_EQ(9, t.submits);
  ASSERT_EQ(8, t.completed_writes.size());
  ASSERT_EQ(
    0,
    t.get_avail());

  ASSERT_EQ(t.get_page_seqno(), 21);   
  ASSERT_EQ(t.get_transact_seqno(), 9);

  ASSERT_EQ(t.get_last_outdated_page_seqno(), 5);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 5);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 5);

  t.on_write(t.writes[20].second);
  ASSERT_EQ(9 , t.completed_writes.size());

  ASSERT_EQ(21, t.completed_writes[8].first); // multipage op requests commit for itself
  ASSERT_EQ(&txc, t.completed_writes[8].second);

  t.committed(6); // commits pages 6 (ops 8)
  ASSERT_EQ(
    psize,
    t.get_avail());
  ASSERT_EQ(t.get_last_outdated_page_seqno(), 6);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 5);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 5);

  t.committed(21); // commits all pages but 6
  ASSERT_EQ(
    t.get_total(),
    t.get_avail());
  ASSERT_EQ(t.get_last_outdated_page_seqno(), 21);
  ASSERT_EQ(t.get_last_wiping_page_seqno(), 5);
  ASSERT_EQ(t.get_last_wiped_page_seqno(), 5);

  // and one more simple op piggy backed with page wipe
  bl.clear();
  bl.append(string("aaa"));
  transactions.push_back(bl);

  t.submit(&txc2, transactions[9]);

  // +1 write for the op + 15 writes to wipe pages
  ASSERT_EQ(26 + page_count, t.writes.size());
  ASSERT_EQ(10, t.submits);
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
}

int main(int argc, char **argv) {
  std::vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

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
