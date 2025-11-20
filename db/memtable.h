// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include "db/dbformat.h"
#include "db/skiplist.h"
#include <map>
#include <set>
#include <string>
#include <unordered_set>

#include "leveldb/db.h"

#include "util/BloomFilter.h"
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;

class MemTable {
 public:

  // replacement of a minimal set of functions:
  void* operator new(std::size_t sz);
  void operator delete(void* ptr);
  void *operator new[](std::size_t sz);

  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTable(const InternalKeyComparator& comparator);

  explicit MemTable(const InternalKeyComparator& comparator, ArenaNVM & arena, bool recovery);

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value);
  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

  void SetMemTableHead(void *ptr);

  void* GetTableOffset();
  void Open();
  void Close();
  uint64_t logfile_number;
  Arena arena_;
  Arena arena_in_momory;
  bool isNVMMemtable;
  port::Mutex usr_mutex_;
  port::RWMutex read_mutex_;
  uint8_t usr;
  ~MemTable();
  BloomFilter bloom_;
  void AddPredictIndex(const char*);
  int  CheckPredictIndex(const char*,size_t len);
 private:
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b, uint16_t p = 0) const;
  };

  typedef SkipList<const char*, KeyComparator> Table;

  // Private since only Unref() should be used to delete it

  KeyComparator comparator_;
  int refs_;
  Table table_;
  Table table_in_memory;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
