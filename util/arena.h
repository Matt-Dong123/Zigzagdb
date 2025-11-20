// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <string>

namespace leveldb {
#define MEM_THRESH 1.5

class Arena {
 public:
  Arena();

  Arena(const Arena &arena);
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes){
    // The semantics of what to return are a bit messy if we allow
    // 0-byte allocations, so we disallow them here (we don't need
    // them for our internal use).
    assert(bytes > 0);
    if (bytes <= alloc_bytes_remaining_) {
      char* result = alloc_ptr_;
      alloc_ptr_ += bytes;
      alloc_bytes_remaining_ -= bytes;
      return result;
    }
    return AllocateFallback(bytes);
  }

  // Allocate memory with the normal alignment guarantees provided by malloc.
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }
  void Open();
  void Close();
  void* operator new(size_t size);
  void* operator new[](size_t size);
  void operator delete(void* ptr);
  virtual void* CalculateOffset(void* ptr);
  virtual void* getMapStart();
  size_t getAllocRem();
  void *map_start_;
  void *map_end_;
  int is_largemap_set;
  bool nvmarena_;
  long long kSize;
  std::string mfile;
  int fd;
  bool allocation;
  uint64_t total_size;
// private:
  virtual char* AllocateFallback(size_t bytes);
  virtual char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;
  std::vector<uint64_t> blocks_size;
  // Total memory usage of the arena.
  //
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?
 protected:
  std::atomic<size_t> memory_usage_;
};
//
//char* Arena::Allocate(size_t bytes) {
//  // The semantics of what to return are a bit messy if we allow
//  // 0-byte allocations, so we disallow them here (we don't need
//  // them for our internal use).
//  assert(bytes > 0);
//  if (bytes <= alloc_bytes_remaining_) {
//    char* result = alloc_ptr_;
//    alloc_ptr_ += bytes;
//    alloc_bytes_remaining_ -= bytes;
//    return result;
//  }
//  return AllocateFallback(bytes);
//}


class ArenaNVM :public Arena {
 public:
  ArenaNVM(long long size, std::string *filename, bool recovery);
  ArenaNVM();
  ~ArenaNVM();
  void* operator new(size_t size);
  void* operator new[](size_t size);
  void operator delete(void* ptr);
  char* AllocateNVMBlock(size_t block_bytes);
  char* AllocateFallbackNVM(size_t bytes);
  // Allocate memory with the normal alignment guarantees provided by malloc
  char* AllocateAligned(size_t bytes);
  char* AllocateAlignedNVM(size_t bytes);
  char* Allocate(size_t bytes){
    assert(bytes > 0);
    if (bytes <= alloc_bytes_remaining_) {
      char* result = alloc_ptr_;
      alloc_ptr_ += bytes;
      alloc_bytes_remaining_ -= bytes;
#ifdef ENABLE_RECOVERY
      memory_usage_.fetch_add(bytes + sizeof(char*),
                          std::memory_order_relaxed);
#endif
      return result;
    }
    return AllocateFallbackNVM(bytes);
  }
  void* CalculateOffset(void* ptr);
  void* getMapStart();
};
//char* ArenaNVM::Allocate(size_t bytes) {
//  assert(bytes > 0);
//  if (bytes <= alloc_bytes_remaining_) {
//    char* result = alloc_ptr_;
//    alloc_ptr_ += bytes;
//    alloc_bytes_remaining_ -= bytes;
//#ifdef ENABLE_RECOVERY
//    memory_usage_.fetch_add(bytes + sizeof(char*),
//                          std::memory_order_relaxed);
//#endif
//    return result;
//  }
//  return AllocateFallbackNVM(bytes);
//}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
