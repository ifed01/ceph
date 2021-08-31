// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "include/stat.h"
#include "include/ceph_assert.h"
#include "include/object.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <algorithm>
#include <limits.h>
#include <dirent.h>

using namespace std;
class TestMount {
  ceph_mount_info* cmount = nullptr;
  char dir_path[64];

public:
  TestMount( const char* root_dir_name = "dir0") {
    ceph_create(&cmount, NULL);
    ceph_conf_read_file(cmount, NULL);
    ceph_conf_parse_env(cmount, NULL);
    ceph_assert(0 == ceph_mount(cmount, NULL));

    sprintf(dir_path, "/%s_%d", root_dir_name, getpid());
    ceph_assert(0 == ceph_mkdir(cmount, dir_path, 0777));
  }
  ~TestMount()
  {
    if (cmount) {
      ceph_assert(0 == purge_dir(""));
    }
    ceph_rmdir(cmount, dir_path);
    ceph_shutdown(cmount);
  }

  string make_file_path(const char* relpath) {
    char path[PATH_MAX];
    sprintf(path, "%s/%s", dir_path, relpath);
    return path;
  }

  string make_snap_name(const char* name) {
    char snap_name[64];
    sprintf(snap_name, "%s_%d", name, getpid());
    return snap_name;
  }
  string make_snap_path(const char* sname, const char* subdir = nullptr) {
    char snap_path[PATH_MAX];
    string snap_name = subdir ?
      concat_path(make_snap_name(sname), subdir) :
      make_snap_name(sname);
    sprintf(snap_path, ".snap/%s", snap_name.c_str());
    return snap_path;
  }
  string make_snapdiff_relpath(const char* name1, const char* name2,
    const char* relpath = nullptr) {
    char diff_path[PATH_MAX];
    string snap_name1 = make_snap_name(name1);
    string snap_name2 = make_snap_name(name2);
    if (relpath) {
      sprintf(diff_path, ".snap/.~diff=%s.~diff=%s/%s",
        snap_name1.c_str(), snap_name2.c_str(), relpath);
    } else {
      sprintf(diff_path, ".snap/.~diff=%s.~diff=%s",
        snap_name1.c_str(), snap_name2.c_str());
    }
    return diff_path;
  }

  int mksnap(const char* name) {
    string snap_name = make_snap_name(name);
    return ceph_mksnap(cmount, dir_path, snap_name.c_str(),
      0755, nullptr, 0);
  }
  int rmsnap(const char* name) {
    string snap_name = make_snap_name(name);
    return ceph_rmsnap(cmount, dir_path, snap_name.c_str());
  }
  int get_snapid(const char* name, uint64_t* res)
  {
    ceph_assert(res);
    snap_info snap_info;

    char snap_path[PATH_MAX];
    string snap_name = make_snap_name(name);
    sprintf(snap_path, "%s/.snap/%s", dir_path, snap_name.c_str());
    int r = ceph_get_snap_info(cmount, snap_path, &snap_info);
    if (r >= 0) {
      *res = snap_info.id;
      r = 0;
    }
    return r;
  }

  int write_full(const char* relpath, const string& data)
  {
    auto file_path = make_file_path(relpath);
    int fd = ceph_open(cmount, file_path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (fd < 0) {
      return -EACCES;
    }
    int r = ceph_write(cmount, fd, data.c_str(), data.size(), 0);
    if (r >= 0) {
      ceph_fsync(cmount, fd, 0);
    }
    ceph_close(cmount, fd);
    return r;
  }
  string concat_path(string_view path, string_view name) {
    string s(path);
    if (s.empty() || s.back() != '/') {
      s += '/';
    }
    s += name;
    return s;
  }
  int readfull_and_compare(string_view path,
                           string_view name,
    const string_view expected)
  {
    string s = concat_path(path, name);
    return readfull_and_compare(s.c_str(), expected);
  }
  int readfull_and_compare(const char* relpath,
    const string_view expected)
  {
    auto file_path = make_file_path(relpath);
    int fd = ceph_open(cmount, file_path.c_str(), O_RDONLY, 0);
    if (fd < 0) {
      return -EACCES;
    }
    std::string s;
    s.resize(expected.length() + 1);

    int ret = ceph_read(cmount, fd, s.data(), s.length(), 0);
    ceph_close(cmount, fd);

    if (ret < 0) {
      return -EIO;
    }
    if (ret != int(expected.length())) {
      return -ERANGE;
    }
    s.resize(ret);
    if (s != expected) {
      return -EINVAL;
    }
    return 0;
  }
  int unlink(const char* relpath)
  {
    auto file_path = make_file_path(relpath);
    return ceph_unlink(cmount, file_path.c_str());
  }

  int for_each_readdir(const char* relpath,
    std::function<bool(const dirent* dire)> fn)
  {
    auto subdir_path = make_file_path(relpath);
    struct ceph_dir_result* ls_dir;
    int r = ceph_opendir(cmount, subdir_path.c_str(), &ls_dir);
    if (r != 0) {
      return r;
    }
    struct dirent* result;
    while( nullptr != (result = ceph_readdir(cmount, ls_dir))) {
      if (strcmp(result->d_name, ".") == 0 ||
          strcmp(result->d_name, "..") == 0) {
        continue;
      }
      if (!fn(result)) {
        r = -EINTR;
        break;
      }
    }
    ceph_assert(0 == ceph_closedir(cmount, ls_dir));
    return r;
  }
  int readdir_and_compare(const char* relpath,
    const vector<string>& expected0)
  {
    vector<string> expected(expected0);
    auto end = expected.end();
    int r = for_each_readdir(relpath,
      [&](const dirent* dire) {

        std::string name(dire->d_name);
        auto it = std::find(expected.begin(), end, name);
        if (it == end) {
          std::cerr << "readdir_and_compare error: unexpected name:"
                    << name << std::endl;
          return false;
        }
        expected.erase(it);
        return true;
      });
    if (r == 0 && !expected.empty()) {
      std::cerr << __func__ << " error: left entries:" << std::endl;
      for (auto& e : expected) {
        std::cerr << e << std::endl;
      }
      std::cerr << __func__ << " ************" << std::endl;
      r = -ENOTEMPTY;
    }
    return r;
  }
  int for_each_readdir_snapdiff(const char* snap1_relpath,
    const char* snap2_relpath,
    std::function<bool(const dirent*, uint64_t)> fn)
  {
    auto snap1_path = make_file_path(snap1_relpath);
    auto snap2_path = make_file_path(snap2_relpath);
    ceph_snapdiff_info* info = nullptr;
    ceph_snapdiff_entry_t res_entry;
    int r = ceph_open_snapdiff(cmount,
                               snap1_path.c_str(),
                               snap2_path.c_str(),
                               &info);
    if (r != 0) {
      std::cerr << " Failed to open snapdiff, ret:" << r << std::endl;
      return r;
    }
                               
    while (0 < (r = ceph_readdir_snapdiff(info,
                                          &res_entry))) {
      if (strcmp(res_entry.dir_entry.d_name, ".") == 0 ||
        strcmp(res_entry.dir_entry.d_name, "..") == 0) {
        continue;
      }
      if (!fn(&res_entry.dir_entry, res_entry.snapid)) {
        r = -EINTR;
        break;
      }
    }
    ceph_assert(0 == ceph_close_snapdiff(info));
    return r;
  }
  int readdir_snapdiff_and_compare(const char* snap1_relpath,
    const char* snap2_relpath,
    const vector<pair<string, uint64_t>>& expected0)
  {
    vector<pair<string, uint64_t>> expected(expected0);
    auto end = expected.end();
    int r = for_each_readdir_snapdiff(snap1_relpath,
      snap2_relpath,
      [&](const dirent* dire, uint64_t snapid) {

        pair<string, uint64_t> p = std::make_pair(dire->d_name, snapid);
        auto it = std::find(expected.begin(), end, p);
        if (it == end) {
          std::cerr << "readdir_snapdiff_and_compare error: unexpected name:"
            << dire->d_name << "/" << snapid << std::endl;
          return false;
        }
        expected.erase(it);
        return true;
      });
    if (r == 0 && !expected.empty()) {
      std::cerr << __func__ << " error: left entries:" << std::endl;
      for (auto& e : expected) {
        std::cerr << e.first << "/" << e.second << std::endl;
      }
      std::cerr << __func__ << " ************" << std::endl;
      r = -ENOTEMPTY;
    }
    return r;
  }

  int mkdir(const char* relpath)
  {
    auto path = make_file_path(relpath);
    return ceph_mkdir(cmount, path.c_str(), 0777);
  }
  int rmdir(const char* relpath)
  {
    auto path = make_file_path(relpath);
    return ceph_rmdir(cmount, path.c_str());
  }
  int purge_dir(const char* relpath0, bool inclusive = true)
  {
    int r =
      for_each_readdir(relpath0,
        [&] (const dirent* dire) {
          string relpath = concat_path(relpath0, dire->d_name);
          if (dire->d_type == DT_REG) {
            unlink(relpath.c_str());
          } else if (dire->d_type == DT_DIR) {
            purge_dir(relpath.c_str());
            rmdir(relpath.c_str());
          }
          return true;
        });
    if (r != 0) {
      return r;
    }
    if (*relpath0 != 0) {
      r = rmdir(relpath0);
    }
    return r;
  }

  void remove_all() {
    purge_dir("/", false);
  }

  ceph_mount_info* get_cmount() {
    return cmount;
  }
  void prepareSnapDiffCases();
  void prepareSnapDiffLibCases();
  void prepareSnapDiffLib2Cases();
};


//
// Testing basic SnapDiff functionality.
// Using SnapDiff API exposed via .snap path queries.
// This tosses a bunch of 4 files through 2 snapshots at root folder
// and verifies if SnapDiff (and relevant FS access helpers)
// provide proper results
//
TEST(LibCephFS, SnapDiffSimple)
{
  TestMount test_mount;

  // Create 3 files with some original content
  ASSERT_LT(0, test_mount.write_full("fileA", "hello world"));
  ASSERT_LT(0, test_mount.write_full("fileC", "hello world in another file"));
  ASSERT_LT(0, test_mount.write_full("fileD", "hello world unmodified"));

  // Make the first snapshot: snap1
  // which keeps 3 files: fileA, fileC, fileD
  ASSERT_EQ(0, test_mount.mksnap("snap1"));

  // Now invoke snapshot snap1 listing and make sure it contains
  // all the expected files.
  // This has nothing about SnapDiff so far, just checkiong
  // our FS access helpers work as expected
  //
  std::cout << "---------snap1 listing---------" << std::endl;
  ASSERT_EQ(0, test_mount.for_each_readdir("/",
    [&](const dirent* dire) {
      std::cout << dire->d_name<< std::endl;
      return true;
    }));
  {
    vector<string> expected;
    expected.push_back("fileA");
    expected.push_back("fileC");
    expected.push_back("fileD");
    ASSERT_EQ(0, test_mount.readdir_and_compare("/", expected));
  }
  // Test if we check file content properly for existing file
  ASSERT_EQ(0, test_mount.readfull_and_compare("fileA", "hello world"));
  // And get an error for  non-existing one
  ASSERT_EQ(-ERANGE, test_mount.readfull_and_compare("fileC", "hello world"));

  // Now create/update/remove some files to get a delta between snap1 and head:
  // fileA - content updated
  // fileB - created
  // fileC - removed
  ASSERT_LT(0, test_mount.write_full("fileA", "hello world again"));
  ASSERT_LT(0, test_mount.write_full("fileB", "hello world again in B"));
  ASSERT_EQ(0, test_mount.unlink("fileC"));

  // and make another snapshot: snap2
  ASSERT_EQ(0, test_mount.mksnap("snap2"));

  // get current listing for root, still mostly irelevant to SnapDiff
  std::cout << "---------snap2 listing---------" << std::endl;
  ASSERT_EQ(0, test_mount.for_each_readdir("/",
    [&](const dirent* dire) {
      std::cout << dire->d_name << std::endl;
      return true;
    }));
  {
    vector<string> expected;
    expected.push_back("fileA");
    expected.push_back("fileB");
    expected.push_back("fileD");
    ASSERT_EQ(0, test_mount.readdir_and_compare("/", expected));
  }

  // Make sure SnapDiff returns an error when running SnapDiff gets the same
  // snapshot name for both parties A and B.
  std::cout << "---------invalid snapdiff path, the same snaps---------"
            << std::endl;
  {
    auto snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap1");
    ASSERT_EQ(-ENOENT, test_mount.for_each_readdir(snapdiff_path.c_str(),
      [&](const dirent* dire) {
        return true;
      }));
  }
  // Another negative test: error on invalid snapshot name for SnapDiff's
  // party B
  std::cout << "---------invalid snapdiff path, broken snap2 ---------"
            << std::endl;
  {
    // invalid file path - no slash between snapdiff and file names
    auto snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2");
    string s = snapdiff_path;
    s += "fileA";
    ASSERT_EQ(-EACCES, test_mount.readfull_and_compare(s.c_str(), "hello world again"));
  }

  //
  // Get FS delta (SnapDiff) for snapshots: snap1 and snap2
  //
  // At first just make sure OK is returned and print that delta to cout
  std::cout << "---------snap1 vs. snap2 diff listing---------"
            << std::endl;
  auto snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2");
  ASSERT_EQ(0, test_mount.for_each_readdir(snapdiff_path.c_str(),
    [&](const dirent* dire) {
      std::cout << dire->d_name << std::endl;
      return true;
    }));
  // Now make sure readdir results match our expectations,
  std::cout << "---------reading from snapdiff results---------" << std::endl;
  {
    vector<string> expected;
    expected.push_back("fileA");
    expected.push_back("~fileC");
    expected.push_back("fileB");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(),expected));
  }

  // Now checking if access to files at HEAD still works properly
  ASSERT_EQ(0, test_mount.readfull_and_compare("fileA", "hello world again"));
  ASSERT_EQ(0, test_mount.readfull_and_compare("fileB",
    "hello world again in B"));
  ASSERT_EQ(-EACCES, test_mount.readfull_and_compare("fileC", "hello world"));
  ASSERT_EQ(0, test_mount.readfull_and_compare("fileD",
   "hello world unmodified"));

  // Now checking if access to files through SnapDiff path (snap1 vs. snap2)
  // works as expected
  //

  // Check if fileA has got expected content from snap2
  ASSERT_EQ(0, test_mount.readfull_and_compare(snapdiff_path, "fileA",
   "hello world again"));
  // paranoic check if readfull_and_compare works properly for SnapDiff:
  // i.e. it  returns error when getting unexpected content
  //
  ASSERT_EQ(-EINVAL, test_mount.readfull_and_compare(snapdiff_path, "fileA",
   "hello world AGAIN"));

  // fileB to get proper content from snap2
  ASSERT_EQ(0, test_mount.readfull_and_compare(snapdiff_path, "fileB", "hello world again in B"));

  // fileC to be marked as removed and it has got content from snap1
  ASSERT_EQ(0, test_mount.readfull_and_compare(snapdiff_path, "~fileC", "hello world in another file"));

  std::cout << "------------- closing -------------" << std::endl;

  ASSERT_EQ(0, test_mount.unlink("fileA"));
  ASSERT_EQ(0, test_mount.unlink("fileB"));
  ASSERT_EQ(0, test_mount.unlink("fileD"));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
}

/* The following method creates some files/folders/snapshots layout,
   described in the sheet below.
   We're to test SnapDiff against that structure.

* where:
  - xN denotes file 'x' version N.
  - X denotes folder name
  - * denotes no/removed file/folder

#     snap1        snap2      snap3      head
# a1     |     a1     |    a3    |    a4
# b1     |     b2     |    b3    |    b3
# c1     |     *      |    *     |    *
# *      |     d2     |    d3    |    d3
# f1     |     f2     |    *     |    *
# ff1    |     ff1    |    *     |    *
# g1     |     *      |    g3    |    g3
# *      |     *      |    *     |    h4
# i1     |     i1     |    i1    |    i1
# S      |     S      |    S     |    S
# S/sa1  |     S/sa2  |    S/sa3 |    S/sa3
# *      |     *      |    *     |    S/sh4
# *      |     T      |    T     |    T
# *      |     T/td2  |    T/td3 |    T/td3
# C      |     *      |    *     |    *
# C/cc1  |     *      |    *     |    *
# C/C1   |     *      |    *     |    *
# C/C1/c1|     *      |    *     |    *
# G      |     *      |    G     |    G
# G/gg1  |     *      |    G/gg3 |    G/gg3
# *      |     k2     |    *     |    *
# *      |     l2     |    l2    |    *
# *      |     K      |    *     |    *
# *      |     K/kk2  |    *     |    *
# *      |     *      |    H     |    H
# *      |     *      |    H/hh3 |    H/hh3
# I      |     I      |    I     |    *
# I/ii1  |     I/ii2  |    I/ii3 |    *
# I/iii1 |     I/iii1 |    I/iii3|    *
# *      |     *      |   I/iiii3|    *
# *      |    I/J     |  I/J     |    *
# *      |   I/J/i2   |  I/J/i3  |    *
# *      |   I/J/j2   |  I/J/j2  |    *
# *      |   I/J/k2   |    *     |    *
# *      |     *      |  I/J/l3  |    *
# L      |     L      |    L     |    L
# L/ll1  |    L/ll1   |   L/ll3  |    L/ll3
# L/LL   |    L/LL    |  L/LL    |    L/LL
# *      |    L/LL/ll2|  L/LL/ll3|    L/LL/ll4
# *      |    L/LM    |    *     |    *
# *      |    L/LM/lm2|    *     |    *
# *      |    L/LN    |    L/LN  |    *
*/
void TestMount::prepareSnapDiffCases()
{
  //************ snap1 *************
  ASSERT_LE(0, write_full("a", "file 'a' v1"));
  ASSERT_LE(0, write_full("b", "file 'b' v1"));
  ASSERT_LE(0, write_full("c", "file 'c' v1"));
  ASSERT_LE(0, write_full("f", "file 'f' v1"));
  ASSERT_LE(0, write_full("ff", "file 'ff' v1"));
  ASSERT_LE(0, write_full("g", "file 'g' v1"));
  ASSERT_LE(0, write_full("i", "file 'i' v1"));

  ASSERT_EQ(0, mkdir("S"));
  ASSERT_LE(0, write_full("S/sa", "file 'S/sa' v1"));

  ASSERT_EQ(0, mkdir("C"));
  ASSERT_LE(0, write_full("C/cc", "file 'C/cc' v1"));

  ASSERT_EQ(0, mkdir("C/CC"));
  ASSERT_LE(0, write_full("C/CC/c", "file 'C/CC/c' v1"));

  ASSERT_EQ(0, mkdir("G"));
  ASSERT_LE(0, write_full("G/gg", "file 'G/gg' v1"));

  ASSERT_EQ(0, mkdir("I"));
  ASSERT_LE(0, write_full("I/ii", "file 'I/ii' v1"));
  ASSERT_LE(0, write_full("I/iii", "file 'I/iii' v1"));

  ASSERT_EQ(0, mkdir("L"));
  ASSERT_LE(0, write_full("L/ll", "file 'L/ll' v1"));
  ASSERT_EQ(0, mkdir("L/LL"));

  ASSERT_EQ(0, mksnap("snap1"));
  //************ snap2 *************

  ASSERT_LE(0, write_full("b", "file 'b' v2"));
  ASSERT_EQ(0, unlink("c"));
  ASSERT_LE(0, write_full("d", "file 'd' v2"));
  ASSERT_LE(0, write_full("f", "file 'f' v2"));
  ASSERT_EQ(0, unlink("g"));

  ASSERT_LE(0, write_full("S/sa", "file 'S/sa' v2"));

  ASSERT_EQ(0, mkdir("T"));
  ASSERT_LE(0, write_full("T/td", "file 'T/td' v2"));

  ASSERT_EQ(0, purge_dir("C"));
  ASSERT_EQ(0, purge_dir("G"));

  ASSERT_LE(0, write_full("k", "file 'k' v2"));
  ASSERT_LE(0, write_full("l", "file 'l' v2"));

  ASSERT_EQ(0, mkdir("K"));
  ASSERT_LE(0, write_full("K/kk", "file 'K/kk' v2"));

  ASSERT_LE(0, write_full("I/ii", "file 'I/ii' v2"));

  ASSERT_EQ(0, mkdir("I/J"));
  ASSERT_LE(0, write_full("I/J/i", "file 'I/J/i' v2"));
  ASSERT_LE(0, write_full("I/J/j", "file 'I/J/j' v2"));
  ASSERT_LE(0, write_full("I/J/k", "file 'I/J/k' v2"));

  ASSERT_LE(0, write_full("L/LL/ll", "file 'L/LL/ll' v2"));

  ASSERT_EQ(0, mkdir("L/LM"));
  ASSERT_LE(0, write_full("L/LM/lm", "file 'L/LM/lm' v2"));

  ASSERT_EQ(0, mkdir("L/LN"));

  ASSERT_EQ(0, mksnap("snap2"));
    //************ snap3 *************

  ASSERT_LE(0, write_full("a", "file 'a' v3"));
  ASSERT_LE(0, write_full("b", "file 'b' v3"));
  ASSERT_LE(0, write_full("d", "file 'd' v3"));
  ASSERT_EQ(0, unlink("f"));
  ASSERT_EQ(0, unlink("ff"));
  ASSERT_LE(0, write_full("g", "file 'g' v3"));

  ASSERT_LE(0, write_full("S/sa", "file 'S/sa' v3"));

  ASSERT_LE(0, write_full("T/td", "file 'T/td' v3"));

  ASSERT_EQ(0, mkdir("G"));
  ASSERT_LE(0, write_full("G/gg", "file 'G/gg' v3"));

  ASSERT_EQ(0, unlink("k"));

  ASSERT_EQ(0, purge_dir("K"));

  ASSERT_EQ(0, mkdir("H"));
  ASSERT_LE(0, write_full("H/hh", "file 'H/hh' v3"));

  ASSERT_LE(0, write_full("I/ii", "file 'I/ii' v3"));
  ASSERT_LE(0, write_full("I/iii", "file 'I/iii' v3"));
  ASSERT_LE(0, write_full("I/iiii", "file 'I/iiii' v3"));

  ASSERT_LE(0, write_full("I/J/i", "file 'I/J/i' v3"));
  ASSERT_EQ(0, unlink("I/J/k"));
  ASSERT_LE(0, write_full("I/J/l", "file 'I/J/l' v3"));

  ASSERT_LE(0, write_full("L/ll", "file 'L/ll' v3"));

  ASSERT_LE(0, write_full("L/LL/ll", "file 'L/LL/ll' v3"));

  ASSERT_EQ(0, purge_dir("L/LM"));

  ASSERT_EQ(0, mksnap("snap3"));
  //************ head *************
  ASSERT_LE(0, write_full("a", "file 'a' head"));

  ASSERT_LE(0, write_full("h", "file 'h' head"));

  ASSERT_LE(0, write_full("S/sh", "file 'S/sh' head"));

  ASSERT_EQ(0, unlink("l"));

  ASSERT_EQ(0, purge_dir("I"));

  ASSERT_LE(0, write_full("L/LL/ll", "file 'L/LL/ll' head"));

  ASSERT_EQ(0, purge_dir("L/LN"));
}

//
// This case tests SnapDiff functionality for snap1/snap2 snapshot delta
// retrieved through .snap path-based query API.
// It operates against FS layout created by prepareSnapDiffCases() method,
// see relevant table before that function for FS state overview.
//
TEST(LibCephFS, SnapDiffCases1_2)
{
  TestMount test_mount;

  // Create directory tree evolving through a bunch of snapshots
  test_mount.prepareSnapDiffCases();

  string snapdiff_path;

  {
    // Print snapshot delta (snap1 vs. snap2) results for root in a
    // human-readable form.
    std::cout << "---------snap1 vs. snap2 diff listing---------" << std::endl;
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2");
    ASSERT_EQ(0, test_mount.for_each_readdir(snapdiff_path.c_str(),
      [&](const dirent* dire) {
        std::cout << dire->d_name << " ";
        return true;
      }));
    std::cout << std::endl;

    // Make sure the root delta is as expected
    // One should use columns snap1 and snap2 from
    // the table preceeding prepareSnapDiffCases() function
    // to learn which names to expect in the delta.
    //
    //  - file 'a' is unchanged hence not present in delta
    //  - file 'ff' is unchanged hence not present in delta
    //  - file 'i' is unchanged hence not present in delta
    //
    vector<string> expected;
    expected.push_back("b");  // file 'b' is updated in snap2
    expected.push_back("~c"); // file 'c' is removed in snap2
    expected.push_back("d");  // file 'd' is created in snap2
    expected.push_back("f");  // file 'f' is updated in snap2
    expected.push_back("~g"); // file 'g' is removed in snap2
    expected.push_back("S");  // folder 'S' is present in snap2 hence reported
    expected.push_back("T");  // folder 'T' is created in snap2
    expected.push_back("~C"); // folder '~C' is removed in snap2
    expected.push_back("~G"); // folder '~G' is removed in snap2
    expected.push_back("k");  // file 'k' is created in snap2
    expected.push_back("l");  // file 'l' is created in snap2
    expected.push_back("K");  // folder 'K' is created in snap2
    expected.push_back("I");  // folder 'I' is created in snap2
    expected.push_back("L");  // folder 'L' is present in snap2 but got more
                              // subfolders
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    //
    // selectively check the content for entries from root snapshot diff
    //
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "b", "file 'b' v2"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~c", "file 'c' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "d", "file 'd' v2"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~g", "file 'g' v1"));

    //
    // Make sure snapshot delta for /S (existed at both snap1 and snap2)
    // is as expected
    //
    expected.clear();
    expected.push_back("sa");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "S");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    //
    // Make sure sa file from /S snapshot delta has proper
    // content, i.e one from snap2's sa file
    //
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "sa", "file 'S/sa' v2"));

    //
    // Make sure snapshot delta for /T (created at snap2)
    // is as expected
    //
    expected.clear();
    expected.push_back("td");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "T");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    // check the content for T/td
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "td", "file 'T/td' v2"));

    //
    // Make sure snapshot delta for /C (removed at snap2)
    // is as expected
    //
    expected.clear();
    expected.push_back("~cc");
    expected.push_back("~CC");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "~C");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    // check the content for removed /C/cc
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~cc", "file 'C/cc' v1"));

    //
    // Make sure snapshot delta for /C/CC (removed at snap2)
    // is as expected
    //
    expected.clear();
    expected.push_back("~c");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "~C/~CC");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    // check the content for removed /C/CC/c
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~c", "file 'C/CC/c' v1"));

    //
    // Make sure snapshot delta for /I (created at snap2)
    // is as expected
    //
    expected.clear();
    expected.push_back("ii");
    expected.push_back("J");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "I");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    // check the content for removed /I/ii
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ii", "file 'I/ii' v2"));

    //
    // Make sure snapshot delta for /I/J (created at snap2)
    // is as expected
    //
    expected.clear();
    expected.push_back("i");
    expected.push_back("j");
    expected.push_back("k");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "I/J");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "k", "file 'I/J/k' v2"));

    //
    // Make sure snapshot delta for /L (extended at snap2)
    // is as expected
    //
    expected.clear();
    expected.push_back("LL");
    expected.push_back("LM");
    expected.push_back("LN");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "L");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    //
    // Make sure snapshot delta for /L/LL (updated at snap2)
    // is as expected
    //
    expected.clear();
    expected.push_back("ll");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "L/LL");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/LL/ll' v2"));

    //
    // Make sure snapshot delta for /L/LN (created empty at snap2)
    // is as expected
    //
    expected.clear();
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "L/LN");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    // Make sure snapshot delta for /L/LM (created at snap2)
    // is as expected
    //
    expected.clear();
    expected.push_back("lm");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap2", "L/LM");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "lm", "file 'L/LM/lm' v2"));
  }

  std::cout << "-------------" << std::endl;

  test_mount.remove_all();
  test_mount.rmsnap("snap1");
  test_mount.rmsnap("snap2");
  test_mount.rmsnap("snap3");
}

//
// This case tests SnapDiff functionality for snap2/snap3 snapshot delta
// retrieved through .snap path-based query API.
// It operates against FS layout created by prepareSnapDiffCases() method,
// see relevant table before that function for FS state overview.
//
TEST(LibCephFS, SnapDiffCases2_3)
{
  TestMount test_mount;

  // Create directory tree evolving through a bunch of snapshots
  test_mount.prepareSnapDiffCases();

  string snapdiff_path;

  // Print snapshot delta (snap2 vs. snap3) results for root in a
  // human-readable form.
  {
    std::cout << "---------snap2 vs. snap3 diff listing---------" << std::endl;
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3");
    ASSERT_EQ(0, test_mount.for_each_readdir(snapdiff_path.c_str(),
      [&](const dirent* dire) {
        std::cout << dire->d_name << " ";
        return true;
      }));
    std::cout << std::endl;

    // Make sure the root delta is as expected
    // One should use columns snap1 and snap2 from
    // the table preceeding prepareSnapDiffCases() function
    // to learn which names to expect in the delta.
    //
    //  - file 'c' is removed since snap1 hence not present in delta
    //  - file 'l' is unchanged hence not present in delta
    //  - file 'i' is unchanged hence not present in delta
    //
    vector<string> expected;
    expected.push_back("a");   // file 'a' is updated in snap3
    expected.push_back("b");   // file 'b' is updated in snap3
    expected.push_back("d");   // file 'd' is updated in snap3
    expected.push_back("~f");  // file 'f' is removed in snap3
    expected.push_back("~ff"); // file 'ff' is removed in snap3
    expected.push_back("g");   // file 'g' re-appeared in snap3
    expected.push_back("S");   // folder 'S' is present in snap3 hence reported
    expected.push_back("T");   // folder 'T' is present in snap3 hence reported
    expected.push_back("G");   // folder 'G' re-appeared in snap3 hence reported
    expected.push_back("~k");  // file 'k' is removed in snap3
    expected.push_back("~K");  // folder 'K' is removed in snap3
    expected.push_back("H");   // folder 'H' is created in snap3 hence reported
    expected.push_back("I");   // folder 'I' is present in snap3 hence reported
    expected.push_back("L");   // folder 'L' is present in snap3 hence reported
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    //
    // Check the content for entries from root snapshot diff
    //
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "a", "file 'a' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "b", "file 'b' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "d", "file 'd' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~f", "file 'f' v2"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~ff", "file 'ff' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "g", "file 'g' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~k", "file 'k' v2"));

    //
    // Make sure snapshot delta for /S (children updated) is as expected
    //
    expected.clear();
    expected.push_back("sa");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "S");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "sa", "file 'S/sa' v3"));

    //
    // Make sure snapshot delta for /T (children updated) is as expected
    //
    expected.clear();
    expected.push_back("td");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "T");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "td", "file 'T/td' v3"));

    //
    // Make sure snapshot delta for /G (re-appeared) is as expected
    //
    expected.clear();
    expected.push_back("gg");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "G");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "gg", "file 'G/gg' v3"));

    //
    // Make sure snapshot delta for /K (removed) is as expected
    //
    expected.clear();
    expected.push_back("~kk");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "~K");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~kk", "file 'K/kk' v2"));

    //
    // Make sure snapshot delta for /H (created) is as expected
    //
    expected.clear();
    expected.push_back("hh");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "H");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "hh", "file 'H/hh' v3"));

    //
    // Make sure snapshot delta for /I (children updated) is as expected
    //
    expected.clear();
    expected.push_back("ii");
    expected.push_back("iii");
    expected.push_back("iiii");
    expected.push_back("J");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "I");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ii", "file 'I/ii' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "iii", "file 'I/iii' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "iiii", "file 'I/iiii' v3"));

    //
    // Make sure snapshot delta for /I/J (children updated/removed) is as expected
    //
    expected.clear();
    expected.push_back("i");
    expected.push_back("~k");
    expected.push_back("l");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "I/J");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "i", "file 'I/J/i' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~k", "file 'I/J/k' v2"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "l", "file 'I/J/l' v3"));

    //
    // Make sure snapshot delta for /L (children updated/removed) is as expected
    //
    expected.clear();
    expected.push_back("ll");
    expected.push_back("LL");
    expected.push_back("~LM");
    expected.push_back("LN");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "L");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/ll' v3"));

    //
    // Make sure snapshot delta for /L/LL (children updated) is as expected
    //
    expected.clear();
    expected.push_back("ll");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "L/LL");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/LL/ll' v3"));

    //
    // Make sure snapshot delta for /L/LM (removed) is as expected
    //
    expected.clear();
    expected.push_back("~lm");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "L/~LM");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~lm", "file 'L/LM/lm' v2"));

    //
    // Make sure snapshot delta for /L/LN (created empty) is as expected
    //
    expected.clear();
    snapdiff_path = test_mount.make_snapdiff_relpath("snap2", "snap3", "L/LN");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
  }
  std::cout << "-------------" << std::endl;

  test_mount.remove_all();
  test_mount.rmsnap("snap1");
  test_mount.rmsnap("snap2");
  test_mount.rmsnap("snap3");
}

//
// This case tests SnapDiff functionality for snap1/snap3 snapshot delta
// retrieved through .snap path-based query API.
// It operates against FS layout created by prepareSnapDiffCases() method,
// see relevant table before that function for FS state overview.
//
TEST(LibCephFS, SnapDiffCases1_3)
{
  TestMount test_mount;


  // Create directory tree evolving through a bunch of snapshots
  test_mount.prepareSnapDiffCases();

  string snapdiff_path;

  {
    // Print snapshot delta (snap3 vs. snap1) results for root in a
    // human-readable form.
    std::cout << "---------snap3 vs. snap1 diff listing---------" << std::endl;
    snapdiff_path = test_mount.make_snapdiff_relpath("snap3", "snap1");
    ASSERT_EQ(0, test_mount.for_each_readdir(snapdiff_path.c_str(),
      [&](const dirent* dire) {
        std::cout << dire->d_name << " ";
        return true;
      }));
    std::cout << std::endl;

    // Make sure the root delta is as expected
    // One should use columns snap1 and snap3 from
    // the table preceeding prepareSnapDiffCases() function
    // to learn which names to expect in the delta.
    //
    //  - file 'i' is unchanged hence not present in delta
    //
    vector<string> expected;
    expected.push_back("a");   // file 'a' is updated in snap3
    expected.push_back("b");   // file 'b' is updated in snap3
    expected.push_back("~c");  // file 'c' is removed in snap2
    expected.push_back("d");   // file 'd' is updated in snap3
    expected.push_back("~f");  // file 'f' is removed in snap3
    expected.push_back("~ff"); // file 'ff' is removed in snap3
    expected.push_back("g");   // file 'g' removed in snap2 and
                               // re-appeared in snap3
    expected.push_back("S");   // folder 'S' is present in snap3 hence reported
    expected.push_back("T");   // folder 'T' is present in snap3 hence reported
    expected.push_back("~C");  // folder 'C' is removed in snap2

    // folder 'G' is removed in snap2 and re-appeared in snap3
    // hence we indicate it as both removed and present
    expected.push_back("~G");
    expected.push_back("G");

    expected.push_back("l");   // file 'l' is created in snap2
    expected.push_back("H");   // folder 'H' is created in snap3 hence reported
    expected.push_back("I");   // folder 'I' is created in snap3 hence reported
    expected.push_back("L");   // folder 'L' is created in snap3 hence reported
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));

    //
    // Check the content for entries from root snapshot diff
    //
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "a", "file 'a' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "b", "file 'b' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~c", "file 'c' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "d", "file 'd' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~f", "file 'f' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~ff", "file 'ff' v1"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "g", "file 'g' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "l", "file 'l' v2"));


    //
    // Make sure snapshot delta for /S (children updated) is as expected
    //
    expected.clear();
    expected.push_back("sa");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "S");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "sa", "file 'S/sa' v3"));


    //
    // Make sure snapshot delta for /T (created and children updated) is as expected
    //
    expected.clear();
    expected.push_back("td");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "T");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "td", "file 'T/td' v3"));


    //
    // Make sure snapshot delta for /C (removed) is as expected
    //
    expected.clear();
    expected.push_back("~cc");
    expected.push_back("~CC");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "~C");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~cc", "file 'C/cc' v1"));

    //
    // Make sure snapshot delta for /C/C (removed) is as expected
    //
    expected.clear();
    expected.push_back("~c");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "~C/~CC");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~c", "file 'C/CC/c' v1"));

    //
    // Make sure snapshot delta for /G (removed) is as expected
    //
    expected.clear();
    expected.push_back("~gg");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "~G");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "~gg", "file 'G/gg' v1"));

    //
    // Make sure snapshot delta for /G (re-created) is as expected
    //
    expected.clear();
    expected.push_back("gg");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "G");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "gg", "file 'G/gg' v3"));

    //
    // Make sure snapshot delta for /H (created) is as expected
    //
    expected.clear();
    expected.push_back("hh");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "H");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "hh", "file 'H/hh' v3"));

    //
    // Make sure snapshot delta for /I (chinldren updated) is as expected
    //
    expected.clear();
    expected.push_back("ii");
    expected.push_back("iii");
    expected.push_back("iiii");
    expected.push_back("J");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "I");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "iii", "file 'I/iii' v3"));

    //
    // Make sure snapshot delta for /I/J (created at snap2) is as expected
    //
    expected.clear();
    expected.push_back("i");
    expected.push_back("j");
    expected.push_back("l");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "I/J");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "i", "file 'I/J/i' v3"));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "l", "file 'I/J/l' v3"));

    //
    // Make sure snapshot delta for /I/J (children updated) is as expected
    //
    expected.clear();
    expected.push_back("ll");
    expected.push_back("LL");
    expected.push_back("LN");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "L");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/ll' v3"));

    //
    // Make sure snapshot delta for /L/LL (children updated) is as expected
    //
    expected.clear();
    expected.push_back("ll");
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "L/LL");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
    ASSERT_EQ(0,
      test_mount.readfull_and_compare(snapdiff_path.c_str(), "ll", "file 'L/LL/ll' v3"));

    expected.clear();
    snapdiff_path = test_mount.make_snapdiff_relpath("snap1", "snap3", "L/LN");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snapdiff_path.c_str(), expected));
  }
  std::cout << "-------------" << std::endl;

  test_mount.remove_all();
  test_mount.rmsnap("snap1");
  test_mount.rmsnap("snap2");
  test_mount.rmsnap("snap3");
}

/* The following method creates some files/folders/snapshots layout,
   described in the sheet below.
   We're to test SnapDiff readdir API against that structure.

* where:
  - xN denotes file 'x' version N.
  - X denotes folder name
  - * denotes no/removed file/folder

#    snap1         snap2
# fileA1      | fileA2      |
# *           | fileB2      |
# fileC1      | *           |
# fileD1      | fileD1      |
# dirA        | dirA        |
# dirA/fileA1 | dirA/fileA2 |
# *           | dirB        |
# *           | dirB/fileb2 |
# dirC        | *           |
# dirC/filec1 | *           |
# dirD        | dirD        |
# dirD/fileD1 | dirD/fileD1 |
*/
void TestMount::prepareSnapDiffLibCases()
{
  //************ snap1 *************
  ASSERT_LE(0, write_full("fileA", "hello world"));
  ASSERT_LE(0, write_full("fileC", "hello world to be removed"));
  ASSERT_LE(0, write_full("fileD", "hello world unmodified"));
  ASSERT_EQ(0, mkdir("dirA"));
  ASSERT_LE(0, write_full("dirA/fileA", "file 'A/a' v1"));
  ASSERT_EQ(0, mkdir("dirC"));
  ASSERT_LE(0, write_full("dirC/filec", "file 'C/c' v1"));
  ASSERT_EQ(0, mkdir("dirD"));
  ASSERT_LE(0, write_full("dirD/filed", "file 'D/d' v1"));

  ASSERT_EQ(0, mksnap("snap1"));

  //************ snap2 *************
  ASSERT_LE(0, write_full("fileA", "hello world again in A"));
  ASSERT_LE(0, write_full("fileB", "hello world in B"));
  ASSERT_EQ(0, unlink("fileC"));

  ASSERT_LE(0, write_full("dirA/fileA", "file 'A/a' v2"));
  ASSERT_EQ(0, purge_dir("dirC"));
  ASSERT_EQ(0, mkdir("dirB"));
  ASSERT_LE(0, write_full("dirB/fileb", "file 'B/b' v2"));

  ASSERT_EQ(0, mksnap("snap2"));
}

/*
* Basic functionality testing for the SnapDiff readdir API
*/
TEST(LibCephFS, SnapDiffLib)
{
  TestMount test_mount;

  // Create simple directory tree with a couple of snapshots
  // to test against
  test_mount.prepareSnapDiffLibCases();

  uint64_t snapid1;
  uint64_t snapid2;

  // learn snapshot ids and do basic verification
  ASSERT_EQ(0, test_mount.get_snapid("snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("snap2", &snapid2));
  ASSERT_GT(snapid1, 0);
  ASSERT_GT(snapid2, 0);
  ASSERT_GT(snapid2, snapid1);
  std::cout << snapid1 << " vs. " << snapid2 << std::endl;

  //
  // Make sure root listing for snapshot snap1 is as expected
  //
  {
    std::cout << "---------snap1 listing verification---------" << std::endl;
    string snap_path = test_mount.make_snap_path("snap1");
    vector<string> expected;
    expected.push_back("fileA");
    expected.push_back("fileC");
    expected.push_back("fileD");
    expected.push_back("dirA");
    expected.push_back("dirC");
    expected.push_back("dirD");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snap_path.c_str(), expected));
  }

  //
  // Make sure root listing for snapshot snap2 is as expected
  //
  {
    std::cout << "---------snap2 listing verification---------" << std::endl;
    string snap_path = test_mount.make_snap_path("snap2");
    vector<string> expected;
    expected.push_back("fileA");
    expected.push_back("fileB");
    expected.push_back("fileD");
    expected.push_back("dirA");
    expected.push_back("dirB");
    expected.push_back("dirD");
    ASSERT_EQ(0,
      test_mount.readdir_and_compare(snap_path.c_str(), expected));
  }

  //
  // Print snap1 vs. snap2 delta for the root
  //
  {
    string snap_path = test_mount.make_snap_path("snap2");
    string snap_other_path = test_mount.make_snap_path("snap1");
    std::cout << "---------snap1 vs. snap2 diff listing---------" << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snap_other_path.c_str(),
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  }

  //
  // Make sure snap1 vs. snap2 delta for the root is as expected
  //
  {
    string snap_path = test_mount.make_snap_path("snap2");
    string snap_other_path = test_mount.make_snap_path("snap1");
    std::cout << "---------snap1 vs. snap2 diff listing verification---------"
              << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileA", snapid2);
    expected.emplace_back("fileB", snapid2);
    expected.emplace_back("fileC", snapid1);
    expected.emplace_back("dirA", snapid2);
    expected.emplace_back("dirB", snapid2);
    expected.emplace_back("dirC", snapid1);
    expected.emplace_back("dirD", snapid2);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snap_other_path.c_str(),
        expected));

    // Check if different order of snapshot names in the request doesn't matter
    snap_path = test_mount.make_snap_path("snap1");
    snap_other_path = test_mount.make_snap_path("snap2");
    std::cout << "---------snap2 vs. snap1 diff listing verification---------"
              << std::endl;
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snap_other_path.c_str(),
        expected));
  }

  //
  // Make sure snap1 vs. snap2 delta for /dirA is as expected
  //
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirA");
    string snap_other_path = test_mount.make_snap_path("snap1");
    std::cout << "---------snap1/dirA vs. snap2/dirA diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileA", snapid2);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snap_other_path.c_str(),
        expected));
  }

  //
  // Make sure snap1 vs. snap2 delta for /dirB is as expected
  //
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirB");
    string snap_other_path = test_mount.make_snap_path("snap1");
    std::cout << "---------snap1/dirB vs. snap2/dirB diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileb", snapid2);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snap_other_path.c_str(),
        expected));
  }

  //
  // Make sure snap1 vs. snap2 delta for /dirC is as expected
  //
  {
    string snap_path = test_mount.make_snap_path("snap1", "dirC");
    string snap_other_path = test_mount.make_snap_path("snap2");
    std::cout << "---------snap1/dirC vs. snap2/dirC diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("filec", snapid1);
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snap_other_path.c_str(),
        expected));
  }

  //
  // Make sure snap1 vs. snap2 delta for /dirD is as expected
  //
  {
    string snap_path = test_mount.make_snap_path("snap2", "dirD");
    string snap_other_path = test_mount.make_snap_path("snap1");
    std::cout << "---------snap1/dirD vs. snap2/dirD diff listing verification---------" << std::endl;
    vector<pair<string, uint64_t>> expected;
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snap_other_path.c_str(),
        expected));
  }

  // Make sure SnapDiff returns an error when provided with the same
  // snapshot name for both parties A and B.
  {
    string snap_path = test_mount.make_snap_path("snap2");
    string snap_other_path = snap_path;
    std::cout << "---------invalid snapdiff params, the same snaps---------" << std::endl;
    ASSERT_EQ(-EINVAL, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snap_other_path.c_str(),
      [&](const dirent* dire, uint64_t snapid) {
        return true;
      }));
  }
  // Make sure SnapDiff returns an error when provided with an empty
  // snapshot name for one of the parties
  {
    std::cout << "---------invalid snapdiff params, no snap_other ---------" << std::endl;
    string snap_path = test_mount.make_snap_path("snap2");
    string snap_other_path;
    ASSERT_EQ(-EINVAL, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snap_other_path.c_str(),
      [&](const dirent* dire, uint64_t snapid) {
        return true;
      }));
  }

  std::cout << "------------- closing -------------" << std::endl;
  ASSERT_EQ(0, test_mount.purge_dir(""));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
}

/* The following method creates some files/folders/snapshots layout,
   described in the sheet below.
   We're to test SnapDiff readdir API against that structure.

* where:
  - xN denotes file 'x' version N.
  - X denotes folder name
  - * denotes no/removed file/folder

#     snap1        snap2      snap3      head
# fileA1      | fileA2      | fileA2
# *           | fileB2      | fileB2
# fileC1      | *           | fileC3
# fileD1      | fileD1      | fileD3
# fileF1      | *           | *
# *           | *           | fileE3
# dirA        | dirA        | *
# dirA/fileA1 | dirA/fileA2 | *
# *           | dirB        | *
# *           | dirB/fileb2 | *
# dirC        | *           | *
# dirC/filec1 | *           | *
# dirD        | dirD        | dirD
# dirD/filed1 | dirD/filed1 | dirD/filed1
*/
void TestMount::prepareSnapDiffLib2Cases()
{
  //************ snap1 *************
  ASSERT_LE(0, write_full("fileA", "hello world"));
  ASSERT_LE(0, write_full("fileC", "hello world to be removed temporarily"));
  ASSERT_LE(0, write_full("fileD", "hello world unmodified"));
  ASSERT_LE(0, write_full("fileF", "hello world to be removed completely"));
  ASSERT_EQ(0, mkdir("dirA"));
  ASSERT_LE(0, write_full("dirA/fileA", "file 'A/a' v1"));
  ASSERT_EQ(0, mkdir("dirC"));
  ASSERT_LE(0, write_full("dirC/filec", "file 'C/c' v1"));
  ASSERT_EQ(0, mkdir("dirD"));
  ASSERT_LE(0, write_full("dirD/filed", "file 'D/d' v1"));

  ASSERT_EQ(0, mksnap("snap1"));

  //************ snap2 *************
  ASSERT_LE(0, write_full("fileA", "hello world again in A"));
  ASSERT_LE(0, write_full("fileB", "hello world in B"));
  ASSERT_EQ(0, unlink("fileC"));
  ASSERT_EQ(0, unlink("fileF"));

  ASSERT_LE(0, write_full("dirA/fileA", "file 'A/a' v2"));
  ASSERT_EQ(0, mkdir("dirB"));
  ASSERT_LE(0, write_full("dirB/fileb", "file 'B/b' v2"));
  ASSERT_EQ(0, purge_dir("dirC"));

  ASSERT_EQ(0, mksnap("snap2"));

  //************ snap3 *************
  ASSERT_LE(0, write_full("fileC", "hello world in C recovered"));
  ASSERT_LE(0, write_full("fileD", "hello world in D now modified"));
  ASSERT_LE(0, write_full("fileE", "file 'E' created at snap3"));
  ASSERT_EQ(0, purge_dir("dirA"));
  ASSERT_EQ(0, purge_dir("dirB"));
  ASSERT_EQ(0, mksnap("snap3"));
}

/*
* More versatile SnapDiff readdir API verification,
* includes 3 different snapshots and interleaving/repetitive calls to make sure
* the results aren't spoiled due to caching.
*/
TEST(LibCephFS, SnapDiffLib2)
{
  TestMount test_mount;

  test_mount.prepareSnapDiffLib2Cases();

  // Create simple directory tree with a couple of snapshots to test against
  uint64_t snapid1;
  uint64_t snapid2;
  uint64_t snapid3;
  ASSERT_EQ(0, test_mount.get_snapid("snap1", &snapid1));
  ASSERT_EQ(0, test_mount.get_snapid("snap2", &snapid2));
  ASSERT_EQ(0, test_mount.get_snapid("snap3", &snapid3));
  std::cout << snapid1 << " vs. " << snapid2 << " vs. " << snapid3 << std::endl;
  ASSERT_GT(snapid1, 0);
  ASSERT_GT(snapid2, 0);
  ASSERT_GT(snapid3, 0);
  ASSERT_GT(snapid2, snapid1);
  ASSERT_GT(snapid3, snapid2);

  // define a labda which verifies snap1/snap2/snap3 listings
  auto verify_snap_listing = [&]()
  {
    {
      string snap_path = test_mount.make_snap_path("snap1");

      std::cout << "---------snap1 listing verification---------" << std::endl;
      vector<string> expected;
      expected.push_back("fileA");
      expected.push_back("fileC");
      expected.push_back("fileD");
      expected.push_back("fileF");
      expected.push_back("dirA");
      expected.push_back("dirC");
      expected.push_back("dirD");
      ASSERT_EQ(0,
        test_mount.readdir_and_compare(snap_path.c_str(), expected));
    }
    {
      std::cout << "---------snap2 listing verification---------" << std::endl;
      string snap_path = test_mount.make_snap_path("snap2");
      vector<string> expected;
      expected.push_back("fileA");
      expected.push_back("fileB");
      expected.push_back("fileD");
      expected.push_back("dirA");
      expected.push_back("dirB");
      expected.push_back("dirD");
      ASSERT_EQ(0,
        test_mount.readdir_and_compare(snap_path.c_str(), expected));
    }
    {
      std::cout << "---------snap3 listing verification---------" << std::endl;
      string snap_path = test_mount.make_snap_path("snap3");
      vector<string> expected;
      expected.push_back("fileA");
      expected.push_back("fileB");
      expected.push_back("fileC");
      expected.push_back("fileD");
      expected.push_back("fileE");
      expected.push_back("dirD");
      ASSERT_EQ(0,
        test_mount.readdir_and_compare(snap_path.c_str(), expected));
    }
  };
  // Prepare expected delta for snap1 vs. snap2
  vector<pair<string, uint64_t>> snap1_2_diff_expected;
  snap1_2_diff_expected.emplace_back("fileA", snapid2);
  snap1_2_diff_expected.emplace_back("fileB", snapid2);
  snap1_2_diff_expected.emplace_back("fileC", snapid1);
  snap1_2_diff_expected.emplace_back("fileF", snapid1);
  snap1_2_diff_expected.emplace_back("dirA", snapid2);
  snap1_2_diff_expected.emplace_back("dirB", snapid2);
  snap1_2_diff_expected.emplace_back("dirC", snapid1);
  snap1_2_diff_expected.emplace_back("dirD", snapid2);

  // Prepare expected delta for snap1 vs. snap3
  vector<pair<string, uint64_t>> snap1_3_diff_expected;
  snap1_3_diff_expected.emplace_back("fileA", snapid3);
  snap1_3_diff_expected.emplace_back("fileB", snapid3);
  snap1_3_diff_expected.emplace_back("fileC", snapid3);
  snap1_3_diff_expected.emplace_back("fileD", snapid3);
  snap1_3_diff_expected.emplace_back("fileE", snapid3);
  snap1_3_diff_expected.emplace_back("fileF", snapid1);
  snap1_3_diff_expected.emplace_back("dirA", snapid1);
  snap1_3_diff_expected.emplace_back("dirC", snapid1);
  snap1_3_diff_expected.emplace_back("dirD", snapid3);

  // Prepare expected delta for snap2 vs. snap3
  vector<pair<string, uint64_t>> snap2_3_diff_expected;
  snap2_3_diff_expected.emplace_back("fileC", snapid3);
  snap2_3_diff_expected.emplace_back("fileD", snapid3);
  snap2_3_diff_expected.emplace_back("fileE", snapid3);
  snap2_3_diff_expected.emplace_back("dirA", snapid2);
  snap2_3_diff_expected.emplace_back("dirB", snapid2);
  snap2_3_diff_expected.emplace_back("dirD", snapid3);

  // Helper function to verify readdir_snapdiff returns expected results
  auto verify_snap_diff = [&](
    vector<pair<string, uint64_t>>& expected,
    const char* snap1_name,
    const char* snap2_name,
    const char* dir_name = nullptr)
  {
    string snap_path = test_mount.make_snap_path(snap2_name, dir_name);
    string snap_other_path = test_mount.make_snap_path(snap1_name);
    std::cout << "---------" << snap1_name << " vs. " << snap2_name
              << " diff listing verification for /" << (dir_name ? dir_name : "")
              << std::endl;
    ASSERT_EQ(0,
      test_mount.readdir_snapdiff_and_compare(snap_path.c_str(),
        snap_other_path.c_str(),
        expected));
  };
  // Helper function to print readdir_snapdiff results
  auto print_snap_diff = [&](const char* snap1_name,
                             const char* snap2_name,
                             const char* dir_name = nullptr)
  {
    string snap_path = test_mount.make_snap_path(snap2_name, dir_name);
    string snap_other_path = test_mount.make_snap_path(snap1_name);
    std::cout << "---------" << snap1_name << " vs. " << snap2_name
              << " diff listing for /" << (dir_name ? dir_name : "")
              << std::endl;
    ASSERT_EQ(0, test_mount.for_each_readdir_snapdiff(
      snap_path.c_str(),
      snap_other_path.c_str(),
      [&](const dirent* dire, uint64_t snapid) {
        std::cout << dire->d_name << " snap " << snapid << std::endl;
        return true;
      }));
  };

  // Check snapshot listings on a cold cache
  verify_snap_listing();

  // Check snapshot listings on a warm cache
  verify_snap_listing(); // served from cache

  // Print snap1 vs. snap2 delta against the root folder
  print_snap_diff("snap1", "snap2");
  // Verify snap1 vs. snap2 delta for the root
  verify_snap_diff(snap1_2_diff_expected, "snap1", "snap2");

  // Check snapshot listings on a warm cache once again
  // to make sure it wasn't spoiled by SnapDiff
  verify_snap_listing(); // served from cache

  // Verify snap2 vs. snap1 delta
  verify_snap_diff(snap1_2_diff_expected, "snap2", "snap1");

  // Check snapshot listings on a warm cache once again
  // to make sure it wasn't spoiled by SnapDiff
  verify_snap_listing(); // served from cache

  // Verify snap1 vs. snap3 delta for the root
  verify_snap_diff(snap1_3_diff_expected, "snap1", "snap3");

  // Verify snap2 vs. snap3 delta for the root
  verify_snap_diff(snap2_3_diff_expected, "snap2", "snap3");

  // Check snapshot listings on a warm cache once again
  // to make sure it wasn't spoiled by SnapDiff
  verify_snap_listing(); // served from cache

  // Print snap1 vs. snap2 delta against /dirA folder
  print_snap_diff("snap1", "snap2", "dirA");

  // Verify snap1 vs. snap2 delta for /dirA
  {
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileA", snapid2);
    verify_snap_diff(expected, "snap1", "snap2", "dirA");
  }

  // Print snap1 vs. snap2 delta against /dirB folder
  print_snap_diff("snap1", "snap2", "dirB");

  // Verify snap1 vs. snap2 delta for /dirB
  {
    vector<pair<string, uint64_t>> expected;
    expected.emplace_back("fileb", snapid2);
    verify_snap_diff(expected, "snap1", "snap2", "dirB");
  }

  // Print snap1 vs. snap2 delta against /dirD folder
  print_snap_diff("snap1", "snap2", "dirD");

  // Verify snap1 vs. snap2 delta for /dirD
  {
    vector<pair<string, uint64_t>> expected;
    verify_snap_diff(expected, "snap1", "snap2", "dirD");
  }

  // Check snapshot listings on a warm cache once again
  // to make sure it wasn't spoiled by SnapDiff
  verify_snap_listing(); // served from cache

  // Verify snap1 vs. snap2 delta for the root once again
  verify_snap_diff(snap1_2_diff_expected, "snap1", "snap2");

  // Verify snap2 vs. snap3 delta for the root once again
  verify_snap_diff(snap2_3_diff_expected, "snap3", "snap2");

  // Verify snap1 vs. snap3 delta for the root once again
  verify_snap_diff(snap1_3_diff_expected, "snap1", "snap3");

  std::cout << "------------- closing -------------" << std::endl;
  ASSERT_EQ(0, test_mount.purge_dir(""));
  ASSERT_EQ(0, test_mount.rmsnap("snap1"));
  ASSERT_EQ(0, test_mount.rmsnap("snap2"));
  ASSERT_EQ(0, test_mount.rmsnap("snap3"));
}
