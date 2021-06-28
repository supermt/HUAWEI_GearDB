//
// Created by jinghuan on 6/18/21.
//

#ifndef ROCKSDB_FMALLOC_BTREE_H
#define ROCKSDB_FMALLOC_BTREE_H

#include <stdint.h>
#include <string.h>

#include <string>
#include <utility>
#include <vector>

class BKVS {
 public:
  BKVS() {}
  virtual ~BKVS() {}

  virtual int Get(const char *table, const char *key, uint32_t kl,
                  const std::vector<std::string> &fields,
                  std::vector<std::pair<std::string, std::string>> &results) {
    (void)table;
    (void)key;
    (void)kl;
    (void)fields;
    (void)results;
    return -ENOTSUP;
  }

  virtual int Get(const char *table, const char *key, uint32_t kl,
                  std::vector<std::pair<std::string, std::string>> &results) {
    (void)table;
    (void)key;
    (void)kl;
    (void)results;
    return -ENOTSUP;
  }

  virtual const char *Get(const char *table, const char *key, uint32_t kl) {
    (void)table;
    (void)key;
    (void)kl;
    return NULL;
  }

  const char *Get(const char *table, const char *key) {
    return Get(table, key, strlen(key));
  }

  const char *Get(const char *key, uint32_t kl) { return Get(NULL, key, kl); }

  const char *Get(const char *key) { return Get(NULL, key); }

  virtual int Put(const char *table, const char *key, uint32_t kl,
                  std::vector<std::pair<std::string, std::string>> &values) {
    (void)table;
    (void)key;
    (void)kl;
    (void)values;
    return -ENOTSUP;
  }

  virtual int Put(const char *table, const char *key, uint32_t kl,
                  const char *val, uint32_t vl) {
    (void)table;
    (void)key;
    (void)kl;
    (void)val;
    (void)vl;
    return -ENOTSUP;
  }

  int Put(const char *table, const char *key, const char *val) {
    return Put(table, key, strlen(key), val, strlen(val));
  }

  int Put(const char *key, uint32_t kl, const char *val, uint32_t vl) {
    return Put(NULL, key, kl, val, vl);
  }

  int Put(const char *key, const char *val) { return Put(NULL, key, val); }

  virtual int Del(const char *table, const char *key, uint32_t kl) {
    (void)table;
    (void)key;
    (void)kl;
    return -ENOTSUP;
  }

  int Del(const char *table, const char *key) {
    return Del(table, key, strlen(key));
  }

  int Del(const char *key, uint32_t kl) { return Del(NULL, key, kl); }

  int Del(const char *key) { return Del(NULL, key); }

  int Update(const char *table, const char *key, uint32_t kl,
             std::vector<std::pair<std::string, std::string>> &values) {
    return Put(table, key, kl, values);
  }

  int Update(const char *table, const char *key, uint32_t kl, const char *val,
             uint32_t vl) {
    return Put(table, key, kl, val, vl);
  }

  int Update(const char *table, const char *key, const char *val) {
    return Put(table, key, strlen(key), val, strlen(val));
  }

  int Update(const char *key, uint32_t kl, const char *val, uint32_t vl) {
    return Put(NULL, key, kl, val, vl);
  }

  int Update(const char *key, const char *val) { return Put(NULL, key, val); }
};

BKVS *createBTKVS(const char *filepath);


#endif  // ROCKSDB_FMALLOC_BTREE_H
