/*
 *
 * Copyright 2020 Kenichi Yasukata
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef _FMALLOC_H
#define _FMALLOC_H

#include <fcntl.h>
#include <malloc.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

#include "fmptr.h"

#ifndef USE_DL_PREFIX
#define USE_DL_PREFIX 1
#endif

#include <malloc.h>

#include <cstdint>
#include <cstdlib>

#include "dlmalloc.h"
#include "fmptr.h"

#ifndef PAGE_SIZE
#define PAGE_SIZE (4096)
#endif

#ifndef FMALLOC_MAGIC
#define FMALLOC_MAGIC (123456)
#endif

#define FMALLOC_OFF (PAGE_SIZE * 2)
#define FMALLOC_MIN_CHUNK (1UL << 24)

/*
 * data layout on a file
 *
 * 0             4KB           8KB                 end
 * |-- fm_super --|-- for app --|-- ... malloc ...--|
 *
 * 4KB ~ 8KB is reserved for app, and assumes to have
 * a pointer to a root object.
 */

/*
 * on-disk representation of super block
 * this always occupies first 4KB of a file
 */
struct fm_super {
  uint64_t magic;
  uint64_t total_size;
  uint64_t chunk_size;
  uint64_t num_chunk;
  uint8_t bm_[0]; /* this must be at the end */

  void set_total_size(uint64_t size) {
    this->total_size = (size / PAGE_SIZE) * PAGE_SIZE;
    if (total_size < FMALLOC_MIN_CHUNK) {
      num_chunk = 1;
      chunk_size = total_size;
    } else {
      num_chunk = (PAGE_SIZE - sizeof(struct fm_super)) * 8;
      chunk_size = (((size - FMALLOC_OFF) / PAGE_SIZE) * PAGE_SIZE) / num_chunk;
      if (chunk_size < FMALLOC_MIN_CHUNK) {
        chunk_size = FMALLOC_MIN_CHUNK;
        num_chunk = total_size / chunk_size;
      }
    }
  }

  void munmap_locked(void *mem) {
    int idx = m2i((void *)((uint64_t)this + FMALLOC_OFF), mem);
    bitmap_release(bm_, idx);
  }

  void *mmap_locked(void) {
    int idx = bitmap_grab(bm_);
    if (idx < 0) {
      fprintf(stderr, "bitmap_grab failed\n");
      return MAP_FAILED;
    }
    return i2m(this, idx);
  }

  void bitmap_set(int idx) { bm_[idx / 8] |= (1UL << (idx % 8)); }

  void bitmap_release(uint8_t *bm, int idx) {
    bm[idx / 8] &= ~(1UL << (idx % 8));
  }

  int bitmap_grab(uint8_t *bm) {
    unsigned long i, j;
    for (i = 0; i < (total_size / chunk_size); i++) {
      for (j = 0; j < 8; j++) {
        if (num_chunk <= i * 8 + j) return -1;
        if (!(bm[i] & (1UL << j))) {
          bm[i] |= (1UL << j);
          return i * 8 + j;
        }
      }
    }
    return -1;
  }

  int m2i(void *mem, void *ptr) {
    return (int)(((uint64_t)ptr - (uint64_t)(mem)) / chunk_size);
  }

  void *i2m(void *mem, int idx) {
    return (void *)((uint64_t)mem + (uint64_t)(chunk_size * idx));
  }
} __attribute__((packed));

/* in-memory reference to super block */
struct fm_info {
  int fd;
  void *mem;
  struct fm_super *s;

  fm_info(int _fd, void *_mem, struct fm_super *_s)
      : fd(_fd), mem(_mem), s(_s) {}
};

struct fm_info *fmalloc_init(const char *filepath, bool *init);
void fmalloc_set_target(struct fm_info *fi);

void *fmalloc(size_t size);
void ffree(void *addr);

#endif