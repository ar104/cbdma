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

#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<fcntl.h>
#include<unistd.h>
#include<string.h>
#include "lz4_mt.h"
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>

unsigned long fill_input(int fd, char *buffer) {
  unsigned long got, total = 0;
  do {
    got = read(fd, (void *) buffer, LZ4_BLOCKSIZE - total);
    total += got;
    buffer += got;
  } while (got != 0 && total != LZ4_BLOCKSIZE);
  return total;
}


void flush_output(int fd, char *buffer, unsigned long size) {
  write_to_file(fd, (unsigned char *) buffer, size);
}


int main(int argc, char *argv[]) {
  if (argc < 4) {
    fprintf(stderr, "Usage %s infile outfile threads\n", argv[0]);
    exit(-1);
  }
  char *src = (char *) malloc(LZ4_BLOCKSIZE);
  int fd_in = open(argv[1], O_RDONLY);
  int fd_out = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
  unsigned long comp_array_size = atol(argv[3]);
  lz4_compression_work *comp_array;
  unsigned long comp_head;
  unsigned long comp_tail;
  boost::asio::io_service lz4_comp_service;
  boost::asio::io_service::work service_wk(lz4_comp_service);
  boost::thread_group lz4_comp_pool;

  comp_array = new lz4_compression_work[comp_array_size];
  for (unsigned long i = 0; i < comp_array_size; i++) {
    lz4_comp_pool.create_thread(boost::bind(&boost::asio::io_service::run,
                                            &lz4_comp_service));
    comp_array[i].fd_out = fd_out;
    comp_array[i].startup(i == 0);
  }
  comp_head = comp_tail = 0;
  unsigned long inbytes;
  unsigned long total_bytes = 0;
  while (true) {
    inbytes = fill_input(fd_in, src);
    if (!inbytes) break;
    total_bytes += inbytes;
    unsigned char *buffer = (unsigned char *) src;
    while (inbytes > 0) {
      if (comp_array[comp_head].uncomp_bytes == LZ4_BLOCKSIZE) {
        comp_array[comp_head].busy = true;
        lz4_comp_service.post(boost::bind(&lz4_compression_work::compress,
                                          &comp_array[comp_head]));
        comp_head = (comp_head + 1) % comp_array_size;
        if (comp_head == comp_tail) {
          while (comp_array[comp_tail].busy);
          write_to_file(comp_array[comp_tail].fd_out,
                        comp_array[comp_tail].comp_data,
                        comp_array[comp_tail].outbytes);
	  comp_tail = (comp_tail + 1) % comp_array_size;
        }
      }
      unsigned long tocopy = (inbytes > (LZ4_BLOCKSIZE - comp_array[comp_head].uncomp_bytes)) ?
                             (LZ4_BLOCKSIZE - comp_array[comp_head].uncomp_bytes) : inbytes;
      memcpy(comp_array[comp_head].uncomp_data + comp_array[comp_head].uncomp_offset, buffer, tocopy);
      buffer += tocopy;
      inbytes -= tocopy;
      comp_array[comp_head].uncomp_offset += tocopy;
      comp_array[comp_head].uncomp_bytes += tocopy;
    }
  }
  while (comp_tail != comp_head) {
    while (comp_array[comp_tail].busy);
    write_to_file(comp_array[comp_tail].fd_out,
                  comp_array[comp_tail].comp_data,
                  comp_array[comp_tail].outbytes);
    comp_array[comp_tail].shutdown(false);
    comp_tail = (comp_tail + 1) % comp_array_size;
  }
  if (comp_array[comp_head].uncomp_bytes > 0) {
    comp_array[comp_head].compress();
    write_to_file(comp_array[comp_head].fd_out,
                  comp_array[comp_head].comp_data,
                  comp_array[comp_head].outbytes);
  }
  comp_array[comp_head].shutdown(true);
  close(fd_in);
  close(fd_out);
  lz4_comp_service.stop();
  lz4_comp_pool.join_all();
  delete[] comp_array;
  printf("Compressed bytes %lu", total_bytes);
  return 0;
}

