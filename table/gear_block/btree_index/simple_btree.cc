//
// Created by jinghuan on 6/21/21.
//

#include "simple_btree.h"

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "include/rocksdb/slice.h"
#include "linklist.h"
namespace ROCKSDB_NAMESPACE {
typedef struct {
  uint64_t magic;
  // fixed content
  uint64_t order;
  uint64_t blk_size;
  uint64_t root_blkid;
  uint64_t blk_counts;
  uint64_t max_blkid;
  // padding to blk_size
} BTreeMetaBlk;
#define BTREE_FILE_MAGIC 0xbbbbbbbb

typedef struct {
  BTreeMetaBlk *blk;
  int dirty;
} BTreeMeta;

enum bt_node_type { root = 1, internal = 2, leaf = 3 };
typedef struct {
  // fixed content
  bt_node_type type;
  uint64_t key_counts;
  uint64_t parent_blkid;
  uint64_t left_sibling_blkid;
  uint64_t right_sibling_blkid;
  // key and pointers is decided by order of the tree:
  // for i = 0:order-2
  //     uint64_t child_i_index or value
  //     uint64_t key_i
  // and one extra:
  // uint64_t child_order-1_index

  // sizeof(key and pointers) = (order * 2 - 1 ) * 64
  // block size = sizeof(key and pointers) + sizeof(BTreeNodeBlk)
} BTreeNodeBlk;

typedef struct {
  BTree *tree;
  BTreeNodeBlk *blk;
  uint64_t blkid;

  struct list_head chain;   // new or dirty or deleted?
  struct list_head *state;  // which chain this node in? (the block status)
} BTreeNode;

struct _BTree {
  char *file_path;
  int file_fd;

  BTreeNode *root;
  BTreeMeta *meta;

  // keep node states, only flush new and modified node's blk
  struct list_head new_node_chain;
  struct list_head deleted_node_chain;
  struct list_head dirty_node_chain;

  // calculated by meta for convenience
  key_type min_keys;  // for none root
  key_type max_keys;  // for none root

  // node will be loaded to memory first time it is accessed
  // we keep blkidx to node here, not loaded node have value NULL.
  BTreeNode **blkid_to_node;
};
struct _BTreeValues {
  uint64_t counts;
  uint64_t *values;
};

static BTreeValues *bt_values_new();
static void bt_values_put_value(BTreeValues *values, uint64_t value);

// statement of functions
static uint64_t bt_next_blkid(BTree *bt);
static uint64_t bt_get_order(BTree *bt);
static key_type bt_get_max_keys(BTree *bt);
static key_type bt_get_min_keys(BTree *bt);
static uint64_t bt_get_blksize(BTree *bt);
static void bt_load_blk(BTree *bt, void *dst, uint64_t index);
static void bt_set_node(BTree *bt, uint64_t blkid, BTreeNode *node);
static BTreeNode *bt_get_node(BTree *bt, uint64_t blkid);
static void bt_meta_destory(BTreeMeta *meta);
static uint64_t bt_meta_get_blksize(BTreeMeta *meta);
static uint64_t bt_meta_get_order(BTreeMeta *meta);
static uint64_t bt_meta_get_maxblkid(BTreeMeta *meta);
static uint64_t bt_meta_next_blkid(BTreeMeta *meta);
static uint64_t bt_meta_get_root_blkid(BTreeMeta *meta);
static BTreeNode *bt_node_new_from_file(BTree *bt, uint64_t blkid);
static BTreeNodeBlk *bt_node_blk_new_from_file(BTree *bt, uint64_t blkid);

// Btree file operations

static key_type bt_get_max_keys(BTree *bt) { return bt->max_keys; }

static key_type bt_get_min_keys(BTree *bt) { return bt->min_keys; }

static uint64_t bt_get_order(BTree *bt) { return bt_meta_get_order(bt->meta); }

static uint64_t bt_get_blksize(BTree *bt) {
  return bt_meta_get_blksize(bt->meta);
}

static uint64_t bt_get_max_blkid(BTree *bt) {
  return bt_meta_get_maxblkid(bt->meta);
}

static uint64_t bt_next_blkid(BTree *bt) {
  return bt_meta_next_blkid(bt->meta);
}

static uint64_t bt_get_root_blkid(BTree *bt) {
  return bt_meta_get_root_blkid(bt->meta);
}

static void bt_set_node(BTree *bt, uint64_t blkid, BTreeNode *node) {
  uint64_t max_blkid;
  max_blkid = bt_get_max_blkid(bt);

  assert(blkid <= max_blkid);

  // TODO....  only realloc if really needed
  if (blkid == max_blkid)
    bt->blkid_to_node = (BTreeNode **)realloc(
        bt->blkid_to_node, sizeof(BTreeNode *) * (max_blkid + 1));
  bt->blkid_to_node[blkid] = node;
}
static BTreeNodeBlk *bt_node_blk_new_from_file(BTree *bt, uint64_t blkid) {
  BTreeNodeBlk *blk;
  uint64_t blksize;

  blksize = bt_get_blksize(bt);
  blk = (BTreeNodeBlk *)malloc(blksize + sizeof(uint64_t) * 2);
  memset(blk, 0, blksize);
  bt_load_blk(bt, blk, blkid);
  return blk;
}

static BTreeNode *bt_node_new_from_file(BTree *bt, uint64_t blkid) {
  BTreeNode *node;
  BTreeNodeBlk *blk;
  blk = bt_node_blk_new_from_file(bt, blkid);
  node = (BTreeNode *)malloc(sizeof(BTreeNode));
  node->blk = blk;
  node->blkid = blkid;
  node->tree = bt;
  node->state = NULL;  // clean node.

  bt_set_node(bt, blkid, node);

  return node;
}
static BTreeNode *bt_get_node(BTree *bt, uint64_t blkid) {
  uint64_t max_blkid;
  max_blkid = bt_get_max_blkid(bt);

  assert(blkid <= max_blkid);

  // TODO....  only realloc if really needed
  if (bt->blkid_to_node[blkid] == NULL) bt_node_new_from_file(bt, blkid);

  return bt->blkid_to_node[blkid];
}

static void bt_open_file(BTree *bt) {
  if (bt->file_fd == -1) {
    bt->file_fd = open(bt->file_path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  }
  assert(bt->file_fd != -1);
}

static void bt_load_blk(BTree *bt, void *dst, uint64_t blkid) {
  uint64_t rtv;
  uint64_t blksize;

  // when load Meta block, blksize is unknown
  if (blkid == 0)
    blksize = sizeof(BTreeMetaBlk);
  else
    blksize = bt_get_blksize(bt);

  bt_open_file(bt);
  rtv = lseek(bt->file_fd, blkid * blksize, SEEK_SET);
  assert(rtv == blkid * blksize);
  rtv = read(bt->file_fd, dst, blksize);
  assert(rtv == blksize);
}

static void bt_store_blk(BTree *bt, uint64_t blkid) {
  uint64_t rtv;
  uint64_t blksize;
  void *blk;

  if (blkid == 0)
    blk = (void *)bt->meta->blk;
  else
    blk = (void *)bt->blkid_to_node[blkid]->blk;

  blksize = bt_get_blksize(bt);
  bt_open_file(bt);
  rtv = lseek(bt->file_fd, blkid * blksize, SEEK_SET);
  assert(rtv == blkid * blksize);
  rtv = write(bt->file_fd, blk, blksize);
  assert(rtv == blksize);
}

static void bt_load_meta(BTree *bt) {
  BTreeMeta *meta;

  meta = bt_meta_new_from_file(bt);

  bt->meta = meta;
}

static void bt_load_root(BTree *bt) {
  uint64_t root_blk_id;
  BTreeNode *root;

  root_blk_id = bt_get_root_blkid(bt);
  root = bt_node_new_from_file(bt, root_blk_id);

  bt->root = root;
}

static BTree *bt_new_from_file(const char *file) {
  BTree *bt;
  uint64_t order;

  bt = (BTree *)malloc(sizeof(BTree));
  INIT_LIST_HEAD(&bt->deleted_node_chain);
  INIT_LIST_HEAD(&bt->new_node_chain);
  INIT_LIST_HEAD(&bt->dirty_node_chain);
  bt->file_path = (char *)malloc(strlen(file) + 1);
  strcpy(bt->file_path, file);
  bt->file_fd = -1;
  bt_load_meta(bt);
  order = bt_get_order(bt);
  bt->max_keys = order - 1;
  bt->min_keys = order / 2;
  // node blk id start from 1
  bt->blkid_to_node =
      (BTreeNode **)malloc(sizeof(BTreeNode *) * (bt_get_max_blkid(bt) + 1));
  memset(bt->blkid_to_node, 0,
         sizeof(BTreeNode *) * (bt_get_max_blkid(bt) + 1));

  bt_load_root(bt);

  return bt;
}

static BTree *bt_new_empty(const char *file, uint64_t order) {
  uint64_t blksize;
  BTree *bt;

  bt = (BTree *)malloc(sizeof(BTree));

  INIT_LIST_HEAD(&bt->deleted_node_chain);
  INIT_LIST_HEAD(&bt->new_node_chain);
  INIT_LIST_HEAD(&bt->dirty_node_chain);

  bt->file_path = (char *)malloc(strlen(file) + 1);
  strcpy(bt->file_path, file);
  bt->file_fd = -1;
  //  bt->max_keys = order - 1;
  //  bt->min_keys = order / 2;
  bt->max_keys = Slice();
  bt->min_keys = Slice();

  blksize = sizeof(BTreeNodeBlk) + (order * 2 - 1) * sizeof(uint64_t);
  assert(blksize >= sizeof(BTreeMetaBlk));

  bt->meta = bt_meta_new_empty(order, blksize);

  bt->blkid_to_node =
      (BTreeNode **)malloc(sizeof(BTreeNode *) * (bt_get_max_blkid(bt) + 1));
  memset(bt->blkid_to_node, 0, bt_get_max_blkid(bt) + 1);

  bt->root = bt_node_new_empty(bt, BT_NODE_TYPE_LEAF | BT_NODE_TYPE_ROOT);

  return bt;
}

void bt_flush(BTree *bt) {
  BTreeNode *node;

  if (bt->meta->dirty) bt_store_blk(bt, 0);

  list_for_each_entry(node, &bt->new_node_chain, chain)
      bt_store_blk(bt, bt_node_get_blkid(node));

  list_for_each_entry(node, &bt->dirty_node_chain, chain)
      bt_store_blk(bt, bt_node_get_blkid(node));
}

void bt_insert(BTree *bt, uint64_t key, uint64_t value) {
  BTreeNode *leaf;

  leaf = bt_node_search(bt->root, key);
  bt_node_leaf_insert(leaf, key, value);

  assert(bt_node_get_key_count(leaf) <= bt->max_keys);
}

BTreeValues *bt_search_range(BTree *bt, uint64_t limit, uint64_t key_min,
                             uint64_t key_max) {
  BTreeNode *leaf;
  BTreeValues *values;
  int remind;
  int b_continue;

  values = bt_values_new();
  leaf = bt_node_search(bt->root, key_min);

  do {
    remind = limit - bt_values_get_count(values);
    b_continue =
        bt_node_leaf_fetch_values(leaf, values, remind, key_min, key_max);

    leaf = bt_node_get_right_sibling(leaf);
    if (leaf == NULL) break;

  } while (b_continue);

  return values;
}

BTreeValues *bt_search(BTree *bt, uint64_t limit, uint64_t key) {
  return bt_search_range(bt, limit, key, key);
}

BTree *bt_open(BTreeOpenFlag flag) {
  assert(flag.file);
  if (access(flag.file, F_OK) != -1) {
    if (flag.error_if_exist) return NULL;
    return bt_new_from_file(flag.file);
  } else {
    // file not exist!
    if (!flag.create_if_missing) return NULL;
    if (flag.order % 2 != 1 || flag.order < 3) return NULL;
    return bt_new_empty(flag.file, flag.order);
  }
}

void bt_close(BTree *bt) {
  uint64_t i;

  // flush tree to disk
  bt_flush(bt);

  // destory loaded node
  for (i = 1; i <= bt_get_max_blkid(bt); i++) {
    if (bt->blkid_to_node[i]) bt_node_destory(bt->blkid_to_node[i]);
  }
  // destory meta
  bt_meta_destory(bt->meta);

  free(bt->blkid_to_node);
  free(bt->file_path);
  free(bt);
}

static void bt_load_blk(BTree *bt, void *dst, uint64_t blkid) {
  uint64_t rtv;
  uint64_t blksize;

  // when load Meta block, blksize is unknown
  if (blkid == 0)
    blksize = sizeof(BTreeMetaBlk);
  else
    blksize = bt_get_blksize(bt);

  bt_open_file(bt);
  rtv = lseek(bt->file_fd, blkid * blksize, SEEK_SET);
  assert(rtv == blkid * blksize);
  rtv = read(bt->file_fd, dst, blksize);
  assert(rtv == blksize);
}

// Begin of Btree Meta
static BTreeMetaBlk *bt_meta_blk_new_empty(uint64_t order, uint64_t blksize) {
  BTreeMetaBlk *blk;

  blk = (BTreeMetaBlk *)malloc(blksize);
  // prevent valgrind complain Syscall param write(buf) points to uninitialised
  // byte(s)
  memset(blk, 0, blksize);
  blk->magic = BTREE_FILE_MAGIC;
  blk->order = order;
  blk->blk_size = blksize;
  blk->blk_counts = 1;
  blk->max_blkid = 0;
  blk->root_blkid = 1;
  return blk;
}

static BTreeMetaBlk *bt_meta_blk_new_from_file(BTree *bt) {
  BTreeMetaBlk *blk;

  blk = (BTreeMetaBlk *)malloc(sizeof(BTreeMetaBlk));
  bt_load_blk(bt, blk, 0);                 // metablk has blkid 0
  assert(blk->magic == BTREE_FILE_MAGIC);  // bad tree file
  blk = (BTreeMetaBlk *)realloc(blk, blk->blk_size);

  memset((char *)blk + sizeof(BTreeMetaBlk), 0,
         blk->blk_size - sizeof(BTreeMetaBlk));

  return blk;
}

static void bt_meta_blk_destory(BTreeMetaBlk *blk) { free(blk); }

static BTreeMeta *bt_meta_new_empty(uint64_t order, uint64_t blksize) {
  BTreeMeta *meta;
  BTreeMetaBlk *blk;

  blk = bt_meta_blk_new_empty(order, blksize);
  meta = (BTreeMeta *)malloc(sizeof(BTreeMeta));
  meta->dirty = 1;
  meta->blk = blk;

  return meta;
}
static void bt_meta_destory(BTreeMeta *meta) {
  bt_meta_blk_destory(meta->blk);
  free(meta);
}

static BTreeMeta *bt_meta_new_from_file(BTree *bt) {
  BTreeMeta *meta;
  BTreeMetaBlk *blk;

  blk = bt_meta_blk_new_from_file(bt);
  meta = (BTreeMeta *)malloc(sizeof(BTreeMeta));
  meta->dirty = 0;
  meta->blk = blk;

  return meta;
}

static void bt_meta_set_root_blkid(BTreeMeta *meta, uint64_t root_blkid) {
  meta->dirty = 1;
  meta->blk->root_blkid = root_blkid;
}

static void bt_set_root_blkid(BTree *bt, uint64_t root_blkid) {
  bt_meta_set_root_blkid(bt->meta, root_blkid);
}

static uint64_t bt_meta_get_root_blkid(BTreeMeta *meta) {
  return meta->blk->root_blkid;
}

static uint64_t bt_meta_get_order(BTreeMeta *meta) { return meta->blk->order; }

static uint64_t bt_meta_get_blksize(BTreeMeta *meta) {
  return meta->blk->blk_size;
}

static uint64_t bt_meta_get_maxblkid(BTreeMeta *meta) {
  return meta->blk->max_blkid;
}

static uint64_t bt_meta_next_blkid(BTreeMeta *meta) {
  meta->dirty = 1;
  meta->blk->blk_counts++;
  return ++meta->blk->max_blkid;
}
// end of BtreeMeta
}  // namespace ROCKSDB_NAMESPACE