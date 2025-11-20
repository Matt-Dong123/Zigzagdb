// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>
#include <unordered_map>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);
int FindMem(const InternalKeyComparator& icmp,
             const std::vector<VirtualMetaData*>& files, const Slice& key);
// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

class Version {
 public:
  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };
  struct node{
    int times;
    int level;
    FileMetaData *f;
  };
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats, FileMetaData *&f, int &level);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  bool CheckFloat(int sum, int times, FileMetaData *f, int level);

  void CleanFloat(){
    file_to_float = nullptr;
    file_to_float_level = -1;
  }
  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);
  void GetOverlappingInputs(int level, const InternalKey* begin,
                            const InternalKey* end,
                            std::vector<FileMetaData*>* inputs,
                            std::vector<VirtualMetaData *> *inserts);
  void GetOverlappingVirtualInputs(int level, const InternalKey* begin,
                            const InternalKey* end,
                            std::vector<FileMetaData*>* inputs,
                            std::vector<VirtualMetaData *> *inserts);
  void GetOverlappingInftyInputs(int level, InternalKey* begin,
                                 InternalKey* end,
                                 std::vector<FileMetaData*>* inputs);
  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);
  void Print_VFiles(const Options *options);
  uint64_t SumVFiles() const {
    uint64_t sum = 0;
    for(int level = 0 ;level < config::kNumLevels; level ++){
      for(auto p:Vfiles_[level]){
        sum += p->total_size;
      }
    }
    return sum;
  }
  int NumFiles(int level) const { return files_[level].size(); }
  int NumVFiles(int level) const { return Vfiles_[level].size(); }
  int compact_level() const {
    const bool size_compaction = (compaction_score_ >= 1);
    const bool seek_compaction = (file_to_compact_ != nullptr);
    if(size_compaction) {
      return compaction_level_;
    }
    else{
      return file_to_compact_level_;
    }
  }
  void Add_Mem(int level,VirtualMetaData *f) {
    Vfiles_[level].push_back(f);
  }
  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;
  class LevelMemNumIterator;
  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        file_to_float(nullptr),
        file_to_float_level(-1),
        compaction_score_(-1),seg_score_(-1),seg_level_(-1),
        compaction_level_(-1) {
  }

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));
  void ForEachOverlappingDisk(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));
  void ForEachOverlappingPmem(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, VirtualMetaData*));
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*),
                          bool (*func2)(void*, int, VirtualMetaData*));
  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  std::vector<FileMetaData*> files_[config::kNumLevels];

  //list of virutal level memtable
  std::vector<VirtualMetaData *> Vfiles_[config::kNumLevels];
  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  FileMetaData* file_to_float;
  int file_to_float_level;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_;
  int compaction_level_;

  double seg_score_;
  int seg_level_;

};

struct node{
  int times;
  int level;
  FileMetaData f;
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;
  uint64_t SumFile(){
    uint64_t sum = 0;
    for(int level = 0; level < config::kNumLevels; level++){
      for(auto f:current_->files_[level]){
        sum+=f->file_size;
      }
      for(auto f:current_->Vfiles_[level]){
        sum+=f->total_size;
      }
    }
    return sum;
  }
  int NumLevelVFiles(int level) const{
    int sum = 0;
    for(auto v:current_->Vfiles_[level]){
      if(v->total_size){
        sum++;
      }
    }
    return sum;
  }
  uint64_t Max_VFile(int level)const{
    uint64_t maxn = 0;
    for(auto v:current_->Vfiles_[level]){
      if(v->total_size){
        maxn = std::max(maxn,v->total_size);
      }
    }
    return maxn;
  }
  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  Compaction* PickFloat(std::map<std::string,int> &key_mp,int &sum,int *level_vis);

  Compaction* PickSeg();
  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  Iterator* MakeInsertIterator(Compaction* c);

  Iterator* MakeSegmentIterator(Compaction* c);
  //only make iterator use ith Mem and jth file
  Iterator* MakeInputIterator(Compaction* c,int i,int j);

  //make for more mem
  Iterator* MakeInputIterator(Compaction* c,int i,int j,int length);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return v->compaction_score_ >= 1 || v->file_to_compact_ != nullptr;
  }

  bool NeedsSegment() const {
    Version* v = current_;
    return v->seg_score_ >=1;
  }

  bool NeedsFloat(int &sum) const{
    return sum >= 10000;
  }
  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);
  void AddLiveVFiles(std::set<uint64_t>* live);
  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

  void SetupOtherInputs(Compaction* c, int level);
 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  void SetupOtherSegments(Compaction *c);
  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }
  bool vfile_empty(int start)const{
    for(int i=start;i<inserts_.size();i++){
      if(inserts_[i]->total_size){
        return false;
      }
    }
    return true;
  }
  uint64_t vfile_sum()const{
    uint64_t sum = 0;
    for(auto v:inserts_){
      sum+=v->total_size;
    }
    return sum;
  }
  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  VirtualMetaData* add_Mem(int i){ return inserts_[i]; }

  int num_add_mem() const { return inserts_.size(); }

  size_t total_size_mem() const {
    size_t sum = 0;
    for(auto v:inserts_){
      sum += v->total_size;
    }
    return sum;
  }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  bool IsTrivialMoveInfty() const;
  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);
  void AddInputDeletionsSeg(VersionEdit* edit);
  void AddInputDeletions(VersionEdit* edit, std::vector<FileMetaData*> &delete_files);
  void AddMemDeletions(VersionEdit* edit, int level, VirtualMetaData *v);
  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[3];  // The two sets of inputs
  // input[2] means infty level for merge
  std::vector<VirtualMetaData *> inserts_;
  //TODO: we use inserts_vector to store the pointer of VirtualMetaData;
  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
