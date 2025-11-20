// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <unistd.h>
#include <fcntl.h>
#include <cstdlib>
#include <assert.h>
#ifdef ENABLE_RECOVERY
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#endif
namespace leveldb {
// 这是内存申请模块，每次申请的内存不能超过4096-> kBlockSize
static const int kBlockSize = 4096;
static int mmap_count = 0;


Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0), nvmarena_(false), allocation(false), map_end_(0),
    map_start_(0), is_largemap_set(0), fd(-1), kSize(kBlockSize), total_size(0){}
Arena::Arena(const Arena &arena):alloc_ptr_(arena.alloc_ptr_),
                                   alloc_bytes_remaining_(arena.alloc_bytes_remaining_),
                                   nvmarena_(arena.nvmarena_),
                                   allocation(arena.allocation),
                                   map_end_(arena.map_end_),
                                   map_start_(arena.map_start_),
                                   is_largemap_set(arena.is_largemap_set),
                                   fd(arena.fd), kSize(arena.kSize),mfile(arena.mfile),blocks_(arena.blocks_){
  memory_usage_.store(arena.memory_usage_.load(std::memory_order_relaxed),std::memory_order_relaxed);
  total_size = 0;
}
Arena::~Arena() {
#ifdef ENABLE_RECOVERY
  for (size_t i = 0; i < blocks_.size(); i++) {
        if(this->nvmarena_ == true) {
            munmap(blocks_[i], blocks_size[i]);
        }
        else
            delete[] blocks_[i];
    }
    if (fd != -1)
        close(fd);
#else
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
#endif
}

void* Arena:: operator new(size_t size)
{
  return malloc(size);
}

void* Arena::operator new[](size_t size) {
  return malloc(size);
}

void Arena::operator delete(void* ptr)
{
  free(ptr);
}

void* Arena::CalculateOffset(void* ptr) {
  return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(ptr) - reinterpret_cast<intptr_t>(map_start_));
}

void* Arena::getMapStart() {
  return map_start_;
}

void* ArenaNVM::CalculateOffset(void* ptr) {
  return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(ptr) - reinterpret_cast<intptr_t>(map_start_));
}

char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

char* Arena::AllocateAligned(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  blocks_size.push_back(block_bytes);
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}
ArenaNVM::ArenaNVM(long long size, std::string *filename, bool recovery) {
  if(recovery){
    mfile = *filename;
    kSize = MEM_THRESH * size;
    map_start_ = reinterpret_cast<void *>(AllocateNVMBlock(kSize));
    nvmarena_ = true;
    alloc_bytes_remaining_ = *((size_t *)map_start_);
    alloc_ptr_ = reinterpret_cast<char *>(map_start_) + (kSize - alloc_bytes_remaining_);
    map_end_ = 0;
    memory_usage_.store((kSize - alloc_bytes_remaining_));
    allocation = true;
  }
  else{
    alloc_ptr_ = nullptr;  // First allocation will allocate a block
    alloc_bytes_remaining_ = 0;
    map_start_ = map_end_ = 0;
    nvmarena_ = true;
    kSize = MEM_THRESH * size;
    mfile = *filename;
    fd = -1;
    allocation = false;
  }
}
ArenaNVM::ArenaNVM() {
  alloc_ptr_ = nullptr;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;
  map_start_ = map_end_ = 0;
  nvmarena_ = true;
  kSize = kBlockSize;
  mfile = "";
}
size_t Arena::getAllocRem() {
  return alloc_bytes_remaining_;
}

void* ArenaNVM::getMapStart() {
  return map_start_;
}
void* ArenaNVM:: operator new(size_t size)
{
  return malloc(size);
}
void* ArenaNVM::operator new[](size_t size) {
  return malloc(size);
}
void ArenaNVM::operator delete(void* ptr) {
  delete[] reinterpret_cast<char *> (ptr);
  ptr = nullptr;
}
ArenaNVM::~ArenaNVM() {
  for (size_t i = 0; i < blocks_.size(); i++) {
#ifdef ENABLE_RECOVERY
    munmap(blocks_[i], blocks_size[i]);
    blocks_[i] = nullptr;
}
if(fd != -1)
  close(fd);
#else
    delete[] blocks_[i];
    blocks_[i] = nullptr;
  }
#endif
}
void Arena::Open() {
#ifdef ENABLE_RECOVERY
  if (nvmarena_ ){
    //ReOpen Map file
    assert(blocks_.size() == 0);
    if(fd == -1) {
      fd = open(mfile.c_str(), O_RDWR);
      if (fd == -1) {
        fd = open(mfile.c_str(), O_RDWR | O_CREAT, 0664);
        if (fd < 0) {
          return;
        }
      }
    }
    if(ftruncate(fd, total_size) != 0){
      perror("ftruncate failed \n");
      return ;
    }

    char *result = (char *)mmap(nullptr, total_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    assert((uint64_t)result != -1);

    blocks_size.clear();
    blocks_.clear();
    map_start_ = result;
    blocks_.push_back(result);
    blocks_size.push_back(total_size);
    assert(blocks_.size() <= 1);
    alloc_ptr_ = result +total_size - alloc_bytes_remaining_;
  }
#endif
}
void Arena::Close() {
#ifdef ENABLE_RECOVERY
  if(nvmarena_ ){
    munmap(blocks_[0],blocks_size[0]);
    map_start_ = nullptr;
    blocks_.clear();
    blocks_size.clear();
    alloc_ptr_ = nullptr;
    if(fd != -1){
      close(fd);
      fd = -1;
    }
  }
#endif
}
char* ArenaNVM::AllocateNVMBlock(size_t block_bytes) {
#ifdef ENABLE_RECOVERY
  if(fd == -1) {
    fd = open(mfile.c_str(), O_RDWR);
    if (fd == -1) {
      fd = open(mfile.c_str(), O_RDWR | O_CREAT, 0664);
      if (fd < 0) return nullptr;
    }
  }
    if(ftruncate(fd, total_size + block_bytes) != 0){
        perror("ftruncate failed \n");
        return nullptr;
    }
    if(total_size!=0 && blocks_[0] != nullptr){
      munmap(blocks_[0],blocks_size[0]);
    }
    char *result = (char *)mmap(nullptr, total_size + block_bytes, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    assert((uint64_t)result != -1);
    blocks_size.clear();
    blocks_.clear();
    total_size+=block_bytes;
    map_start_ = result;
    blocks_.push_back(result);
    blocks_size.push_back(total_size);
    assert(blocks_.size() <= 1);
    return result + total_size - block_bytes;
#else
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  blocks_size.push_back(block_bytes);
#endif
  return result;
}
char* ArenaNVM::AllocateFallbackNVM(size_t bytes) {
  uint64_t allocate_bytes;
  if(kSize < bytes) {
    allocate_bytes = (bytes / kSize + (bytes % kSize != 0)) * kSize;
  }
  else {
    allocate_bytes = kSize;
  }
  alloc_ptr_ = AllocateNVMBlock(allocate_bytes);
#ifdef ENABLE_RECOVERY
  memory_usage_.fetch_add(bytes + sizeof(char*), std::memory_order_relaxed);
#else
  memory_usage_.fetch_add(allocate_bytes + sizeof(char*), std::memory_order_relaxed);
#endif
  alloc_bytes_remaining_ = allocate_bytes;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}
char* ArenaNVM::AllocateAlignedNVM(size_t bytes) {

  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;

#ifdef ENABLE_RECOVERY
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
    memory_usage_.fetch_add(needed + sizeof(char*), std::memory_order_relaxed);
    } else {
        if (allocation) {
            alloc_bytes_remaining_ = 0;
            result = alloc_ptr_ + slop;
            alloc_ptr_ += needed;
            memory_usage_.fetch_add(needed + sizeof(char*), std::memory_order_relaxed);
        } else {
            result = this->AllocateFallbackNVM(bytes);
        }
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
    return result;
#else
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    result = this->AllocateFallbackNVM(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
#endif
}
//This method just implements virtual function
char* ArenaNVM::AllocateAligned(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    result = this->AllocateFallbackNVM(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
}
}  // namespace leveldb
