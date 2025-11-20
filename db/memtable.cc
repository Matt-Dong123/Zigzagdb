// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include <iostream>

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data, uint16_t pre = 0) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  p += pre;
  assert(len>=pre);
  return Slice(p, len - pre);
}

void MemTable::AddPredictIndex
    (const char* data) {
  this->bloom_.add((const uint8_t *)data, strlen((const char*)data));
}

int MemTable::CheckPredictIndex
    (const char* data,size_t len) {
  return this->bloom_.possiblyContains((const uint8_t *)data,
                                       (size_t)len);
}


void* MemTable::operator new(std::size_t sz) {
  return malloc(sz);
}

void* MemTable::operator new[](std::size_t sz) {
  return malloc(sz);
}
void MemTable::operator delete(void* ptr)
{
  free(ptr);
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_,false,&usr,&usr_mutex_),
      table_in_memory(comparator_, &arena_in_momory,false,&usr,&usr_mutex_),
      bloom_(BLOOMSIZE, BLOOMHASH), isNVMMemtable(false),usr(0) {}

MemTable::MemTable(const InternalKeyComparator& comparator, ArenaNVM & arena, bool recovery)
    :comparator_(comparator),
     refs_(0),
     logfile_number(0),usr(0),bloom_(BLOOMSIZE, BLOOMHASH),
     table_in_memory(comparator_, &arena_in_momory,false,&usr,&usr_mutex_),
     arena_(arena),table_(comparator_, &arena_, recovery,&usr,&usr_mutex_){
  arena_.nvmarena_ = arena.nvmarena_;
  isNVMMemtable = true;
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() {
  if(arena_.nvmarena_){
    ArenaNVM *nvm_arena = (ArenaNVM *)&arena_;
    return nvm_arena->MemoryUsage();
  }
  return arena_.MemoryUsage();
}

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr,uint16_t p) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr,p);
  Slice b = GetLengthPrefixedSlice(bptr,p);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table), table(table) {
#ifdef ENABLE_RECOVERY
    if(table->arena_->nvmarena_) {
      table->usr_mutex_->Lock();
      if (*table->usr == 0) {
        table->arena_->Open();
        table->SetHead((void*)((uint64_t)table->arena_->getMapStart() +
                               (uint64_t)table->head_offset_));
      }
      (*table->usr)++;
      table->usr_mutex_->Unlock();
    }
#endif
  }

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() {
#ifdef ENABLE_RECOVERY
    if(table->arena_->nvmarena_) {
      table->usr_mutex_->Lock();
      if (*table->usr == 1) {
        if (table->arena_->nvmarena_) {
          //        *table->m_height = table->GetMaxHeight();
          table->arena_->Close();
          table->head_ = nullptr;
        }
      }
      (*table->usr)--;
      table->usr_mutex_->Unlock();
    }
#endif
  }

  bool Valid() const override {
    return iter_.Valid();
  }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }

#ifdef USE_OFFSETS
  virtual const char *GetNodeKey(){
    return reinterpret_cast<const char *>((uint64_t)iter_.node_ - (uint64_t)iter_.key_offset());
  }
#else
  virtual const char *GetNodeKey(){return iter_.key(); }
#endif

#if defined(USE_OFFSETS)
  virtual Slice key() const { return GetLengthPrefixedSlice(reinterpret_cast<const char *>((uint64_t)iter_.node_ - (uint64_t)iter_.key_offset())); }
#else
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
#endif
  virtual Slice value() const {
#if defined(USE_OFFSETS)
    Slice key_slice = GetLengthPrefixedSlice(reinterpret_cast<const char *>((uint64_t)iter_.node_ - (uint64_t)iter_.key_offset()));
#else
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
#endif
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }
  //NoveLSM
  //virtual void SetHead(void *ptr) { iter_.SetHead(ptr); }
  void* operator new(std::size_t sz) {
    return malloc(sz);
  }

  void* operator new[](std::size_t sz) {
    return malloc(sz);
  }
  void operator delete(void* ptr)
  {
    free(ptr);
  }

  Status status() const override {
    return Status::OK();
  }

 private:
  MemTable::Table::Iterator iter_;
  MemTable::Table *table;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

void MemTable::SetMemTableHead(void *ptr){
  table_.SetHead(ptr);
}

void* MemTable::GetTableOffset(){
  return table_.head_offset_;
}

void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = nullptr;

  if(arena_.nvmarena_) {
    ArenaNVM *nvm_arena = (ArenaNVM *)&arena_;
    buf = nvm_arena->Allocate(encoded_len);
  }else {
    buf = arena_.Allocate(encoded_len);
  }
  if(buf == nullptr){
    perror("Memory allocation failed");
    exit(-1);
  }
  char* p = EncodeVarint32(buf, internal_key_size);
  if (this->isNVMMemtable == true) {
#ifdef _ENABLE_PMEMIO
    //memcpy_persist(p, (void *)key.data(), key_size);
    memcpy(p, key.data(), key_size);
    char *keystr = (char*)key.data();
    keystr[key_size]=0;
    AddPredictIndex((const char *)keystr);
#else
    memcpy(p, key.data(), key_size);
#endif
  }else{
    memcpy(p, key.data(), key_size);
  }
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  uint64_t offset = (uint64_t)buf - (uint64_t)arena_.getMapStart() ;
#ifdef ENABLE_RECOVERY
  table_.Insert(buf, s);
#else
  table_.Insert(buf);
#endif
  if(isNVMMemtable){
    //
    //buf is start
    assert(buf != nullptr);
    size_t offset_value_size = VarintLength(offset);
    const size_t memory_encode_len = VarintLength(internal_key_size) +
                                     internal_key_size + VarintLength(offset_value_size) +
                                     offset_value_size;
    buf = arena_in_momory.Allocate(memory_encode_len);
    if(buf == nullptr){
      perror("Memory allocation failed");
      exit(-1);
    }
    p = EncodeVarint32(buf, internal_key_size);
    memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, (s << 8) | type);
    p += 8;
    p = EncodeVarint32(p, offset_value_size);
    p = EncodeVarint32(p, offset);
    assert(p == buf + memory_encode_len);
    assert(offset <= (uint64_t)arena_.total_size);
    table_in_memory.Insert(buf,s);
  }
}

void MemTable::Open() {
#ifdef ENABLE_RECOVERY
  if(arena_.nvmarena_) {
    usr_mutex_.Lock();
    if (usr == 0) {
      arena_.Open();
      table_.SetHead((void*)((uint64_t)arena_.getMapStart() +
                             (uint64_t)table_.head_offset_));
    }
    usr++;
    usr_mutex_.Unlock();
  }
#endif
}
void MemTable::Close() {
#ifdef ENABLE_RECOVERY
  if(arena_.nvmarena_) {
    usr_mutex_.Lock();
    if (usr == 1) {
        arena_.Close();
        table_.head_ = nullptr;
    }
    usr--;
    usr_mutex_.Unlock();
  }
#endif
}
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Open();
  Slice memkey = key.memtable_key();
  Table *check = nullptr;
  if(isNVMMemtable){
    check = &table_in_memory;
  }
  else{
    check = &table_;
  }
  Table::Iterator iter(check);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
#if defined(USE_OFFSETS)
    const char* entry = reinterpret_cast<const char *>((uint64_t)iter.node_ - (uint64_t)iter.key_offset());
#else
    const char* entry = iter.key();
#endif
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
        Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          //from pm read
          if(isNVMMemtable){
            uint32_t offset;
            GetVarint32Ptr(v.data(),v.data() + v.size(),&offset);
            Slice va = GetLengthPrefixedSlice((const char *)arena_.getMapStart() + offset + ((uint64_t)key_ptr-(uint64_t)entry) + key_length);
            value->assign(va.data(), va.size());
          }
          else
            value->assign(v.data(), v.size());
          Close();
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          Close();
          return true;
      }
    }
  }
  Close();
  return false;
}

}  // namespace leveldb
