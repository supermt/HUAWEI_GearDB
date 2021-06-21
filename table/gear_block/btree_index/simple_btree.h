//
// Created by jinghuan on 6/21/21.
//

#ifndef ROCKSDB_SIMPLE_BTREE_H
#define ROCKSDB_SIMPLE_BTREE_H

#include <stdint.h>

#include <string>

#include "include/rocksdb/slice.h"
namespace ROCKSDB_NAMESPACE {

typedef struct _BTreeValues BTreeValues;
uint64_t bt_values_get_count(BTreeValues *values);
uint64_t bt_values_get_value(BTreeValues *values, uint64_t index);
void bt_values_destory(BTreeValues *values);

typedef struct BTreeOpenFlag {
  const char *file;
  uint64_t order;
  int create_if_missing;
  int error_if_exist;
} BTreeOpenFlag;

typedef Slice key_type;

typedef struct _BTree BTree;

BTree *bt_open(BTreeOpenFlag flag);
void bt_insert(BTree *bt, uint64_t key, uint64_t value);
std::string bt_print(BTree *bt);
// if crush befor bt_flush, any modification will be lost.
// if crush inside bt_flush, the tree will be corrupted
void bt_flush(BTree *bt);
void bt_close(BTree *bt);
BTreeValues *bt_search(BTree *bt, uint64_t limit, key_type key);
BTreeValues *bt_search_range(BTree *bt, uint64_t limit, key_type key_min,
                             key_type key_max);

};      // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_SIMPLE_BTREE_H
