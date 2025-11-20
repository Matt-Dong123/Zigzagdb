// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"
#include "port/cache_flush.h"


namespace leveldb {

class Arena;

template <typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena, bool recovery,uint8_t *u,port::Mutex *mutex_);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
#ifdef ENABLE_RECOVERY
  void Insert(const Key& key, uint64_t s = 0);
#else
  void Insert(const Key& key);
#endif

  int GetPre(Node *x,const Key &key,int src)const;

  int GetPre(Node *x,Node *y);
  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  void SetHead(void *ptr);

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
#ifdef USE_OFFSETS
    const Key& key_offset() const;
#else
    const Key& key() const;
#endif

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();
    Node* node_;
    std::atomic<uint64_t> offset;
   private:
    const SkipList* list_;
    // Intentionally copyable
  };

 public:
  uint8_t *usr;
  port::Mutex *usr_mutex_;
  enum { kMaxHeight = 32 };

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  Node* NewNode(const Key& key, int height, bool head_alloc);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n,uint16_t src = 0) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;


  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;  // Arena used for allocations of nodes

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  // Read/written only by Insert().
  Random rnd_;
 public:
  void* head_offset_;   // Head offset from map_start
  Node* head_;
};

// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
#ifdef USE_OFFSETS
  explicit Node(const Key& k, const Key& mem): key_offset (reinterpret_cast<const Key>(mem - k)) {
  }

  Key const key_offset;
#else
  explicit Node(const Key& k) : key(k) {
  }

  Key const key;
#endif
  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
#ifdef USE_OFFSETS
    uint64_t offset = reinterpret_cast<uint64_t>(next_[n].load(std::memory_order_acquire));
    return (offset != 0) ? reinterpret_cast<Node *>((uint64_t)this - offset) : nullptr;
#else
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node *>(next_[n].load(std::memory_order_acquire));
#endif
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
#ifdef USE_OFFSETS
    (x!=nullptr) ?
    next_[n].store(reinterpret_cast<void*>((uint64_t)this - (uint64_t)x), std::memory_order_release) :
    next_[n].store(reinterpret_cast<void*> (0), std::memory_order_relaxed);
#else
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].store(reinterpret_cast<void*>(x), std::memory_order_release);
#endif
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
#ifdef USE_OFFSETS
    uint64_t offset = reinterpret_cast<uint64_t>(next_[n].load(std::memory_order_relaxed));
    return (offset != 0) ? reinterpret_cast<Node *>((uint64_t)this - offset) : nullptr;
#else
    return reinterpret_cast<Node*>(next_[n].load(std::memory_order_relaxed));
#endif
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
#ifdef USE_OFFSETS
    (x!=nullptr) ?
    next_[n].store(reinterpret_cast<void*> ((uint64_t)this - (uint64_t)x), std::memory_order_relaxed) :
    next_[n].store(reinterpret_cast<void*> (0), std::memory_order_relaxed);
#else
    next_[n].store(reinterpret_cast<void*>(x), std::memory_order_relaxed);
#endif
  }
 public:
  uint16_t pre[kMaxHeight]={0};
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<void*> next_[1];
};
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height, bool head_alloc) {
  char* mem;
  bool return_special = head_alloc && arena_->nvmarena_;
  uint64_t key_offset;
  Key key_ptr = key;
  if(arena_->nvmarena_) {
    ArenaNVM *nvm_arena = (ArenaNVM *)arena_;
    key_offset = (uint64_t)key - (uint64_t)arena_->getMapStart();
    if (head_alloc == true)
      mem = nvm_arena->AllocateAlignedNVM(
          sizeof(uint64_t) + sizeof (uint64_t) + sizeof(int) + sizeof(Node) +
          sizeof(std::atomic<void*>) * (height - 1));
    else
      mem = nvm_arena->AllocateAlignedNVM(
          sizeof(Node) + sizeof(std::atomic<void*>) * (height - 1));
    key_ptr = (Key)((uint64_t)arena_->getMapStart() + key_offset);
  }else {
    mem = arena_->AllocateAligned(
        sizeof(Node) + sizeof(std::atomic<void*>)* (height - 1));
  }
#ifndef USE_OFFSETS
  if (return_special){
    char *offset_mem = mem + sizeof(size_t) + sizeof (uint64_t) + sizeof(int);
    return new (offset_mem) Node(key);
  }
  else
    return new (mem) Node(key);
#else
  #ifdef ENABLE_RECOVERY
        if (return_special) {
            char *offset_mem = mem + sizeof(size_t) + sizeof (uint64_t) + sizeof(int);
            return new (offset_mem) Node(key_ptr, (Key)offset_mem);
        } else {
            return new (mem) Node(key_ptr, (Key)mem);
        }
    #else
        return new (mem) Node(key, (Key)mem);
    #endif
#endif
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template<typename Key, class Comparator>
inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != NULL;
}

#ifdef USE_OFFSETS
template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key_offset() const {
#else
template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
#endif
  assert(Valid());
#ifdef USE_OFFSETS
  return node_->key_offset;
#else
  return node_->key;
#endif
}


template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
#ifdef USE_OFFSETS
  node_ = list_->FindLessThan(reinterpret_cast<Key>((uint64_t)node_ - (uint64_t)node_->key_offset));
#else
  node_ = list_->FindLessThan(node_->key);
#endif
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

//template<typename Key, class Comparator>
//inline void SkipList<Key,Comparator>::Iterator::SetHead(void *ptr) {
//  //list_->head_= (Node *)ptr;
//}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n,uint16_t src) const {
  // null n is considered infinite
#ifdef USE_OFFSETS
  return (n != nullptr) && (compare_(reinterpret_cast<Key>((uint64_t)n - (uint64_t)n->key_offset), key,src) < 0);
#else
  return (n != nullptr) && (compare_(n->key, key) < 0);
#endif
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  uint16_t src = 0;
  if(!arena_->nvmarena_) {
    while (true) {
      Node* next = x->Next(level);
      if (KeyIsAfterNode(key, next)) {
        // Keep searching in this list
        x = next;
      } else {
        if (prev != nullptr) prev[level] = x;
        if (level == 0) {
          return next;
        } else {
          // Switch to next list
          level--;
        }
      }
    }
  }
  else{
    while (true) {
      Node* next = x->Next(level);
      if (src <= x->pre[level] && KeyIsAfterNode(key, next, src)) {
        // Keep searching in this list
        src = GetPre(next, key, src);
        x = next;
      } else {
        if (prev != nullptr) prev[level] = x;
        if (level == 0) {
          return next;
        } else {
          // Switch to next list
          level--;
        }
      }
    }
  }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
#ifdef USE_OFFSETS
    assert(x == head_ || compare_(reinterpret_cast<Key>((uint64_t)x - (uint64_t)x->key_offset), key) < 0);
#else
    assert(x == head_ || compare_(x->key, key) < 0);
#endif
    Node* next = x->Next(level);
#ifdef USE_OFFSETS
    if (next == nullptr || compare_(reinterpret_cast<Key>((uint64_t)next - (uint64_t)next->key_offset), key) >= 0) {
#else
    if (next == nullptr || compare_(next->key, key) >= 0) {
#endif
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena, bool recovery,uint8_t *u,port::Mutex *mutex_)
    : compare_(cmp),
      arena_(arena),
//      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  usr = u;
  usr_mutex_ = mutex_;
//  for (int i = 0; i < kMaxHeight; i++) {
//    head_->SetNext(i, nullptr);
//  }
#ifdef ENABLE_RECOVERY
  if (recovery) {
        ArenaNVM *arena_nvm = (ArenaNVM*) arena_;
        head_ = (Node*)((uint8_t*)arena_nvm->getMapStart() + sizeof(size_t) + sizeof(uint64_t) + sizeof(int));
    }
    else {
        head_ = NewNode(0, kMaxHeight, true);
    }
#endif
#ifdef ENABLE_RECOVERY
  //do nothing
#else
  head_ = NewNode(0, kMaxHeight, false);
#endif

  head_offset_ = (reinterpret_cast<void*>(arena_->CalculateOffset(static_cast<void*>(head_))));
  if (!recovery) {
    for (int i = 0; i < kMaxHeight; i++) {
      head_->SetNext(i, NULL);
    }
    if(arena->nvmarena_){
      arena_->Close();
      head_ = nullptr;
    }
  }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::GetPre(SkipList::Node* x, const Key& key,
                                      int src) const{
  const Key key1 = reinterpret_cast<Key>((uint64_t)x - (uint64_t)x->key_offset);
  uint32_t key_length1;
  const char* key_ptr1 = GetVarint32Ptr(key1, key1 + 5, &key_length1);
  uint32_t key_length2;
  //remove magic number
  const char* key_ptr2 = GetVarint32Ptr(key, key + 5, &key_length2);
  key_length1-=8;
  key_length2-=8;
  for(int i=src;i<std::min(key_length1,key_length2);i++){
    if(*(key_ptr1+i)!=*(key_ptr2+i)){
      return i;
    }
  }
  return std::min(key_length1,key_length2);
}
template <typename Key, class Comparator>
int SkipList<Key, Comparator>::GetPre(SkipList::Node* x, SkipList::Node* y){
  const Key key1 = reinterpret_cast<Key>((uint64_t)x - (uint64_t)x->key_offset);
  const Key key2 = reinterpret_cast<Key>((uint64_t)y - (uint64_t)y->key_offset);
  uint32_t key_length1;
  const char* key_ptr1 = GetVarint32Ptr(key1, key1 + 5, &key_length1);
  uint32_t key_length2;
  //remove magic number
  const char* key_ptr2 = GetVarint32Ptr(key2, key2 + 5, &key_length2);
  key_length1-=8;
  key_length2-=8;
  for(int i=0;i<std::min(key_length1,key_length2);i++){
    if(*(key_ptr1+i)!=*(key_ptr2+i)){
      return i;
    }
  }
  return std::min(key_length1,key_length2);
}

#ifdef ENABLE_RECOVERY
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key, uint64_t s) {
#else
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
#endif
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
#ifdef ENABLE_RECOVERY
  if (arena_->nvmarena_) {
    head_ = (Node *)((uint64_t)arena_->getMapStart() + (uint64_t)head_offset_);
  }
  Node* prev[kMaxHeight] = {nullptr};
  Node* x = FindGreaterOrEqual(key, prev);
  uint64_t prev_offset[kMaxHeight] = {0};
#endif
  // Our data structure does not allow duplicate insertion
#ifdef USE_OFFSETS
//  assert(x == nullptr || !Equal(key, reinterpret_cast<Key>((uint64_t)x - (uint64_t)x->key_offset)));
#else
//  assert(x == nullptr || !Equal(key, x->key));
#endif
  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed);
  }
#ifdef ENABLE_RECOVERY
  if (arena_->nvmarena_)
    for (int i = 0; i < height; i++) {
      prev_offset[i] = (uint64_t)prev[i] - (uint64_t)arena_->getMapStart();
    }
#endif
  x = NewNode(key, height, false);

#ifdef ENABLE_RECOVERY
  // this maybe mmap new file,so we must check the value
  if (arena_->nvmarena_) {

    head_ = (Node *)((uint64_t)arena_->getMapStart() + (uint64_t)head_offset_);
    for (int i = 0; i < height; i++) {
      prev[i] = (Node *)((uint64_t)arena_->getMapStart() + prev_offset[i]);
    }
  }
#endif
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
    if(arena_->nvmarena_){
      if(prev[i]!=head_)
        prev[i]->pre[i]=GetPre(prev[i],x);
      if(x->NoBarrier_Next(i)!= nullptr)
        x->pre[i]=GetPre(x,x->NoBarrier_Next(i));
    }
    //president
  }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
#ifdef USE_OFFSETS
  if (x != nullptr && Equal(key, reinterpret_cast<Key>((uint64_t)x - (uint64_t)x->key_offset))) {
#else
  if (x != nullptr && Equal(key, x->key)) {
#endif
    return true;
  } else {
    return false;
  }
}
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::SetHead(void *ptr){
  head_ = reinterpret_cast<Node *>(ptr);
  head_offset_ = (reinterpret_cast<void*>(arena_->CalculateOffset(static_cast<void*>(head_))));
}
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
