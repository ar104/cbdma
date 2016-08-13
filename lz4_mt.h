// Multithreaded LZ4 support
#ifndef _LZ4_MT_
#define _LZ4_MT_

#include<lz4frame.h>
#include<lz4.h>
#include<stdio.h>
#include<stdlib.h>
#include "select.h"
#include <errno.h>

#define LZ4_BLOCKSIZE (4*1024*1024)
#define LZ4_INDEX 7

//#define LZ4_BLOCKSIZE (1*1024*1024)
///#define LZ4_INDEX 6

//#define LZ4_BLOCKSIZE (256*1024)
//#define LZ4_INDEX 5

//#define LZ4_BLOCKSIZE (64*1024)
//#define LZ4_INDEX 4

const int MAGICNUMBER_SIZE = 4;

static void read_from_file(int fd,
                           unsigned char *input,
                           unsigned long bytes_to_read) {
  while (bytes_to_read) {
    unsigned long bytes_read = read(fd, input, bytes_to_read);
    if (bytes_read == -1UL) {
      if (errno != EAGAIN) {
	printf("Stream readin unsuccessful: %s", strerror(errno));
        exit(-1);
      }
    }
    else if (bytes_read == 0) {
      // Note: silently return if EOF
      break;
    }
    else {
      input += bytes_read;
      bytes_to_read -= bytes_read;
    }
  }
}

static void write_to_file(int fd,
                          unsigned char *output,
                          unsigned long bytes_to_write) {
  while (bytes_to_write) {
    unsigned long bytes_written = write(fd, output, bytes_to_write);
    if (bytes_written == -1UL) {
      if (errno != EAGAIN) {
	printf("Stream writeout unsuccessful:%s",
	       strerror(errno));
        exit(-1);
      }
    }
    else {
      output += bytes_written;
      bytes_to_write -= bytes_written;
    }
  }
}

class lz4_uncompression_work {
    LZ4F_decompressionContext_t uncomp_ctx;
    unsigned long total_comp_bytes;
    
public:
    int fd_in;
    unsigned long fillbytes;
    unsigned char *comp_data;
    unsigned long comp_bytes;
    unsigned long expected_next;
    unsigned char *uncomp_data;
    unsigned long uncomp_offset;
    volatile unsigned long uncomp_bytes;
    unsigned long total_uncomp_time;
    volatile bool busy;
    volatile bool terminate;
    
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
      terminate = false;
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
	read_from_file(fd_in, comp_data, next);
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


class lz4_compression_work {
    LZ4F_compressionContext_t comp_ctx;
    unsigned long total_comp_bytes;
public:
    int fd_out;
    unsigned long comp_bound;
    unsigned char *comp_data;
    unsigned char *uncomp_data;
    unsigned long uncomp_bytes;
    unsigned long uncomp_offset;
    volatile bool busy;
    volatile bool terminate;
    volatile unsigned long outbytes;

    lz4_compression_work() {
      total_comp_bytes = 0;
      comp_bound = LZ4F_compressFrameBound(LZ4_BLOCKSIZE, NULL);
      comp_data = (unsigned char *) map_anon_memory(comp_bound); 
      uncomp_data = (unsigned char *) map_anon_memory(LZ4_BLOCKSIZE);
      uncomp_bytes = 0;
      uncomp_offset = 0;
      busy = false;
      terminate = false;
    }

    void startup(bool leader) {
      LZ4F_preferences_t prefs;
      LZ4F_errorCode_t errorCode;
      errorCode = LZ4F_createCompressionContext(&comp_ctx, LZ4F_VERSION);
      if (LZ4F_isError(errorCode)) {
        printf("%s\n", LZ4F_getErrorName(errorCode));
        printf("Unable to initialize context.");
        exit(-1);
      }
      /* Set compression parameters */
      memset(&prefs, 0, sizeof(prefs));
      prefs.autoFlush = 1;
      prefs.compressionLevel = 1;
      prefs.frameInfo.blockMode = (LZ4F_blockMode_t) 1;
      prefs.frameInfo.blockSizeID = (LZ4F_blockSizeID_t) LZ4_INDEX;
      prefs.frameInfo.contentChecksumFlag = (LZ4F_contentChecksum_t) 0;
      outbytes = LZ4F_compressBegin(comp_ctx, comp_data, comp_bound, &prefs);
      if (LZ4F_isError(outbytes)) {
        printf("%s\n", LZ4F_getErrorName(outbytes));
        printf("Error ! compress begin");
        exit(-1);
      }
      if (leader) {
	write_to_file(fd_out, comp_data, outbytes);
      }
    }

    void shutdown(bool tail) {
      LZ4F_errorCode_t errorCode;
      unsigned long outbytes = LZ4F_compressEnd(comp_ctx,
                                                comp_data,
                                                comp_bound,
                                                NULL);
      if (LZ4F_isError(outbytes)) {
        printf("%s\n", LZ4F_getErrorName(outbytes));
        printf("Error compress end");
        exit(-1);
      }
      if (tail) {
	write_to_file(fd_out, comp_data, outbytes);
      }
      errorCode = LZ4F_freeCompressionContext(comp_ctx);
      if (LZ4F_isError(errorCode)) {
        printf("%s\n", LZ4F_getErrorName(errorCode));
        fprintf(stderr, "Unable to destroy context.");
        exit(-1);
      }
    }

    void compress() {
      total_comp_bytes += uncomp_bytes;
      outbytes =
          LZ4F_compressUpdate(comp_ctx,
                              comp_data,
                              comp_bound,
                              uncomp_data,
                              uncomp_bytes,
                              NULL);
      if (LZ4F_isError(outbytes)) {
        fprintf(stderr, "Error ! %s", LZ4F_getErrorName(outbytes));
        printf("See error msg");
        exit(-1);
      }
      uncomp_bytes = 0;
      uncomp_offset = 0;
      __sync_synchronize();
      busy = false;
    }

    ~lz4_compression_work() {
      printf("LZ4::COMPRESSION::BYTES %lu\n", total_comp_bytes);
    }
};

#endif
