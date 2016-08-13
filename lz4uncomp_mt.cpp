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

#include<lz4frame.h>
#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<fcntl.h>
#include<unistd.h>
#include<pthread.h>
#include "lz4_mt.h"

unsigned long fill_input(int fd, char *buffer, unsigned long inbytes) {
  unsigned long got, total = 0;
  do {
    got = read(fd, buffer, inbytes - total);
    total += got;
    buffer += got;
  } while (got != 0 && total != inbytes);
  return got;
}

void flush_output(int fd, char *buffer, unsigned long size) {
  unsigned long wrote;
  while (size) {
    wrote = write(fd, buffer, size);
    size -= wrote;
    buffer += wrote;
  }
}

void* lz4_uncomp_service_start(void *arg)
{
  lz4_uncompression_work *wk = (lz4_uncompression_work *)arg;
  while(true) {
    if(wk->busy) {
      wk->uncompress();
    }
    if(wk->terminate) {
      break;
    }
  }
  return NULL;
}


int main(int argc, char *argv[]) {
  if (argc < 4) {
    fprintf(stderr, "Usage %s infile outfile threads\n", argv[0]);
    exit(-1);
  }
  int fd_in = open(argv[1], O_RDONLY);
  int fd_out = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);

  unsigned long uncomp_array_size = atol(argv[3]);
  lz4_uncompression_work *uncomp_array;
  pthread_t lz4uncomp_thread[uncomp_array_size];
  unsigned long uncomp_ptr;
  unsigned long total_bytes = 0;
  uncomp_array = new lz4_uncompression_work[uncomp_array_size];
  for (unsigned long i = 0; i < uncomp_array_size; i++) {
    uncomp_array[i].fd_in = fd_in;
    if(pthread_create(&lz4uncomp_thread[i],
		      NULL,
		      lz4_uncomp_service_start,
		      &uncomp_array[i]) != 0) {
      printf("failed to launch exec thread\n");
      exit(-1);
    }
  }
  unsigned long nextbytes;
  unsigned char header[11];
  read_from_file(fd_in, header, 11);
  nextbytes = uncomp_array[0].startup(header);
  for (unsigned long i = 1; i < uncomp_array_size; i++) {
    (void) uncomp_array[i].startup(header);
  }
  for (unsigned long i = 0; i < uncomp_array_size; i++) {
    total_bytes += nextbytes;
    nextbytes = uncomp_array[i].fill(nextbytes);
    uncomp_array[i].busy = true;
  }
  uncomp_ptr = 0;
  while (uncomp_array[uncomp_ptr].fillbytes) {
    while (uncomp_array[uncomp_ptr].busy);
    /* Write Block */
    flush_output(fd_out,
                 (char *) uncomp_array[uncomp_ptr].uncomp_data,
                 uncomp_array[uncomp_ptr].uncomp_bytes);
    total_bytes += nextbytes;
    nextbytes = uncomp_array[uncomp_ptr].fill(nextbytes);
    uncomp_array[uncomp_ptr].busy = true;
    uncomp_ptr = (uncomp_ptr + 1) % uncomp_array_size;
  }
  for (unsigned long i = 0; i < uncomp_array_size; i++) {
    uncomp_array[i].shutdown();
    uncomp_array[i].terminate = true;
    pthread_join(lz4uncomp_thread[i], NULL);
  }
  printf("Decompressed %lu\n", total_bytes);
  return 0;
}
