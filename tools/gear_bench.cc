//
// Created by jinghuan on 7/6/21.
//
#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else
#include <rocksdb/gear_bench.h>
int main(int argc, char** argv) {
  return ROCKSDB_NAMESPACE::gear_bench(argc, argv);
}
#endif