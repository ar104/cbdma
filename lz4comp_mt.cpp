#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<fcntl.h>
#include<unistd.h>
#include<string.h>
#include<pthread.h>
#include "lz4_mt.h"
#include "select.h"

static const int ALIGNED_BLOCKSIZE =
  ((LZ4_BLOCKSIZE)/sizeof(sales_table_row_t))*sizeof(sales_table_row_t);

unsigned long fill_input(int fd, char *buffer) {
  unsigned long got, total = 0;
  do {
    got = read(fd, (void *) buffer, ALIGNED_BLOCKSIZE - total);
    total += got;
    buffer += got;
  } while (got != 0 && total != ALIGNED_BLOCKSIZE);
  return total;
}


void flush_output(int fd, char *buffer, unsigned long size) {
  write_to_file(fd, (unsigned char *) buffer, size);
}

void* lz4_comp_service_start(void *arg)
{
  lz4_compression_work *wk = (lz4_compression_work *)arg;
  while(true) {
    if(wk->busy) {
      wk->compress();
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
  char *src = (char *) malloc(ALIGNED_BLOCKSIZE);
  int fd_in = open(argv[1], O_RDONLY);
  int fd_out = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
  unsigned long comp_array_size = atol(argv[3]);
  pthread_t lz4comp_thread[comp_array_size];
  lz4_compression_work *comp_array;
  unsigned long comp_head;
  unsigned long comp_tail;
  comp_array = new lz4_compression_work[comp_array_size];
  for (unsigned long i = 0; i < comp_array_size; i++) {
    comp_array[i].fd_out = fd_out;
    comp_array[i].startup(i == 0);
    if(pthread_create(&lz4comp_thread[i],
		      NULL,
		      lz4_comp_service_start,
		      &comp_array[i]) != 0) {
      printf("failed to launch exec thread\n");
      exit(-1);
    }
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
      if (comp_array[comp_head].uncomp_bytes == ALIGNED_BLOCKSIZE) {
        comp_array[comp_head].busy = true;
	comp_head = (comp_head + 1) % comp_array_size;
        if (comp_head == comp_tail) {
          while (comp_array[comp_tail].busy);
          write_to_file(comp_array[comp_tail].fd_out,
                        comp_array[comp_tail].comp_data,
                        comp_array[comp_tail].outbytes);
	  comp_tail = (comp_tail + 1) % comp_array_size;
        }
      }
      unsigned long tocopy = (inbytes > (ALIGNED_BLOCKSIZE - comp_array[comp_head].uncomp_bytes)) ?
                             (ALIGNED_BLOCKSIZE - comp_array[comp_head].uncomp_bytes) : inbytes;
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
    comp_array[comp_tail].terminate = true;
    comp_tail = (comp_tail + 1) % comp_array_size;
  }
  if (comp_array[comp_head].uncomp_bytes > 0) {
    comp_array[comp_head].compress();
    write_to_file(comp_array[comp_head].fd_out,
                  comp_array[comp_head].comp_data,
                  comp_array[comp_head].outbytes);
  }
  comp_array[comp_head].shutdown(true);
  comp_array[comp_head].terminate = true;
  close(fd_in);
  close(fd_out);
  
  for(int i=0;i<comp_array_size;i++) {
    pthread_join(lz4comp_thread[i], NULL);
  }
  delete[] comp_array;
  printf("Compressed bytes %lu\n", total_bytes);
  return 0;
}

