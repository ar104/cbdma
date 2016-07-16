/*
 * Chaos 
 *
 * Copyright 2015 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Multithreaded LZ4 support
#ifndef _LZ4_MT_
#define _LZ4_MT_

#include<lz4frame.h>
#include<lz4.h>
#include<stdio.h>
#include<stdlib.h>
#include "select.h"


#define LZ4_BLOCKSIZE (4*1024*1024)
#define LZ4_INDEX 7

//#define LZ4_BLOCKSIZE (1*1024*1024)
///#define LZ4_INDEX 6

//#define LZ4_BLOCKSIZE (256*1024)
//#define LZ4_INDEX 5

//#define LZ4_BLOCKSIZE (64*1024)
//#define LZ4_INDEX 4

const int MAGICNUMBER_SIZE = 4;

class lz4_uncompression_work {
    LZ4F_decompressionContext_t uncomp_ctx;
    unsigned long total_comp_bytes;
    
public:
    char *fd_in;
    unsigned long fillbytes;
    unsigned char *comp_data;
    unsigned long comp_bytes;
    unsigned long expected_next;
    unsigned char *uncomp_data;
    unsigned long uncomp_offset;
    volatile unsigned long uncomp_bytes;
    unsigned long total_uncomp_time;
    volatile bool busy;

    lz4_uncompression_work() {
      total_comp_bytes = 0;
      fillbytes = 0;
      unsigned long comp_bound = LZ4F_compressFrameBound(LZ4_BLOCKSIZE, NULL);
      comp_data = (unsigned char *) map_anon_memory(comp_bound);
      uncomp_data = (unsigned char *) map_anon_memory(LZ4_BLOCKSIZE);
      comp_bytes = 0;
      uncomp_bytes = 0;
      uncomp_offset = 0;
      total_uncomp_time = 0;
      busy = false;
    }

    unsigned long startup(unsigned char *magic_bytes) {
      LZ4F_errorCode_t errorCode;
      errorCode = LZ4F_createDecompressionContext(&uncomp_ctx, LZ4F_VERSION);
      if (LZ4F_isError(errorCode)) {
        fprintf(stderr, "Unable to initialize context.");
        exit(-1);
      }
      // Read heder
      unsigned long outbytes = LZ4_BLOCKSIZE;
      unsigned long inbytes = MAGICNUMBER_SIZE;
      unsigned long nextbyts;
      nextbyts = LZ4F_decompress(uncomp_ctx,
                                 uncomp_data,
                                 &outbytes,
                                 magic_bytes,
                                 &inbytes, NULL);
      if (LZ4F_isError(nextbyts)) {
        fprintf(stderr, "Error ! on uncompress startup inital bytes");
        exit(-1);
      }
      if (nextbyts != 7) {
        printf("Unexpected nextbytes on startup !\n");
        exit(-1);
      }
      magic_bytes += MAGICNUMBER_SIZE;
      outbytes = LZ4_BLOCKSIZE;
      inbytes = 3;
      nextbyts = LZ4F_decompress(uncomp_ctx, uncomp_data, &outbytes,
                                 magic_bytes,
                                 &inbytes, NULL);
      if (LZ4F_isError(nextbyts)) {
        fprintf(stderr, "Error ! on uncompress startup flags");
        exit(-1);
      }
      magic_bytes += 3;
      return *(unsigned int *) magic_bytes;
    }

    void shutdown() {
      LZ4F_errorCode_t errorCode;
      errorCode = LZ4F_freeDecompressionContext(uncomp_ctx);
      if (LZ4F_isError(errorCode)) {
        fprintf(stderr, "Unable to destroy context.");
        exit(-1);
      }
    }

    unsigned int fill(unsigned long block_size)
    {
      uncomp_bytes = 0;
      uncomp_offset = 0;
      unsigned long saved_block_size = block_size;
      unsigned long outbytes = LZ4_BLOCKSIZE;
      unsigned long inbytes = 4;
      total_comp_bytes += inbytes;
      // feed it the size
      LZ4F_errorCode_t errorCode;
      errorCode = LZ4F_decompress(uncomp_ctx,
                                  uncomp_data,
                                  &outbytes,
                                  &saved_block_size,
                                  &inbytes,
                                  NULL);
      if (LZ4F_isError(errorCode)) {
        fprintf(stderr, "Feed Error ! %s", LZ4F_getErrorName(uncomp_bytes));
        printf("See error msg\n");
        exit(-1);
      }
      unsigned long next;
      next = block_size & ((1U << 31) - 1);
      fillbytes = next;
      if (next) {
        comp_bytes = next; // Don't show the size
        next += 4;
	memcpy(comp_data, fd_in, next);
	fd_in += next;
	unsigned int next_block_size = *(unsigned int *) (comp_data + next - 4);
        return next_block_size;
      }
      else {
        return 0;
      }
    }

    void uncompress() {
      if (fillbytes > 0) {
        total_comp_bytes += comp_bytes;
        uncomp_bytes = LZ4_BLOCKSIZE;
        unsigned long retbytes =
            LZ4F_decompress(uncomp_ctx,
                            uncomp_data,
                            (unsigned long *) &uncomp_bytes,
                            comp_data,
                            &comp_bytes,
                            NULL);
	if (LZ4F_isError(retbytes)) {
          printf("Error ! %s", LZ4F_getErrorName(uncomp_bytes));
          printf("See error msg");
          exit(-1);
        }
      }
      uncomp_offset = 0;
      __sync_synchronize();
      busy = false;
    }

    ~lz4_uncompression_work() {
      printf("LZ4::DECOMPRESSION::BYTES %lu\n", total_comp_bytes);
      printf("LZ4::DECOMPRESSION::TIME %lu\n",  total_uncomp_time);
    }
};

#endif
