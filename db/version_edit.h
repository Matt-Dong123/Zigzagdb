// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "memtable.h"
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct VirtualMetaData{
  VirtualMetaData(bool left, bool right,
                  bool leftinfty,
                  bool rightinfty,MemTable *mem) : refs(0),allow_seek(0),read_miss(0),total_num(0),
                                                    LeftOpen(left), RightOpen(right),LeftInfty(leftinfty), RightInfty(rightinfty), Mem(mem), total_size(0) {}
  uint64_t number;
  int refs;
  bool LeftOpen;
  bool RightOpen;
  bool LeftInfty;
  bool RightInfty;
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
  MemTable *Mem;
  uint64_t total_size;
  uint64_t allow_seek;
  uint64_t read_miss;
  uint64_t total_num;
};

struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0), isNVMTable(false), Mem(nullptr) {}
  FileMetaData(MemTable *mem) : refs(0), allowed_seeks(1 << 30), file_size(0), isNVMTable(true), Mem(mem){}

  int refs;
  int allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;    // File size in bytes
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
  bool isNVMTable;
  MemTable *Mem;
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest,
               bool isNVMTable, MemTable *Mem) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    f.isNVMTable = isNVMTable;
    f.Mem = Mem;
    new_files_.push_back(std::make_pair(level, f));
  }
  void AddIFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest,
               bool isNVMTable, MemTable *Mem, uint64_t infty_number) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    f.isNVMTable = isNVMTable;
    f.Mem = Mem;
    new_ifiles_.push_back(std::make_pair(level, f));
  }
  void AddMem(int level,VirtualMetaData *f){
    new_mems_.push_back(std::make_pair(level,f));
  }
  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }
  void RemoveIFile(int level, uint64_t file) {
    deleted_ifiles_.insert(std::make_pair(level, file));
  }
  void RemovMem(int level, VirtualMetaData *v){
    deleted_mems_.push_back(std::make_pair(level,v));
  }
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_files_;
  DeletedFileSet deleted_ifiles_;
  std::vector<std::pair<int,VirtualMetaData *> > deleted_mems_;
  std::vector<std::pair<int, FileMetaData>> new_files_;
  std::vector<std::pair<int, FileMetaData>> new_ifiles_;
  std::vector<std::pair<int, VirtualMetaData *>> new_mems_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
