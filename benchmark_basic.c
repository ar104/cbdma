#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include"select.h"
#include<sys/types.h>
#include<unistd.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<sys/mman.h>
#include<sys/time.h>

static unsigned long get_current_rtc()
{
  struct timeval tm;
  gettimeofday(&tm, NULL);
  return tm.tv_sec*1000000 + tm.tv_usec;
}

void *map_output(unsigned long size)
{
  void *space = mmap(NULL, size > 0 ? size : 4096,
                     PROT_READ | PROT_WRITE,
                     MAP_ANONYMOUS | MAP_SHARED, -1, 0);
  if (space == MAP_FAILED) {
    printf("map output failed\n");
    exit(-1);
  }
  if (mlock(space, size) < 0) {
    printf("mlock output failed\n");
    exit(-1);
  }
  return space;
}

void *map_file(const char *fname, unsigned long* sizep)
{
  int fd = open(fname, O_RDONLY);
  if(fd == -1) {
    printf("Failed to open file %s\n", fname);
    exit(-1);
  }
  *sizep = lseek(fd, 0, SEEK_END);
  (void)lseek(fd, 0, SEEK_SET);
  void * mapped_file = mmap(0,
			    *sizep,
			    PROT_READ,
			    MAP_FILE | MAP_SHARED,
			    fd,
			    0);
  if (mapped_file == MAP_FAILED) {
    printf("map failed\n");
    exit(-1);
  }
  if (mlock(mapped_file, *sizep) < 0) {
    printf("failed to mlock file\n");
    exit(-1);
  }
  return mapped_file;
}

void run_date_select(const char *fname, unsigned long dleft, unsigned long dright)
{
  unsigned long size;
  sales_table_row_t *data = (sales_table_row_t *)map_file(fname, &size);
  sales_table_row_t *output = (sales_table_row_t *)map_output(size);
  unsigned long input_size = size; 
  sales_table_row_t *output_start = output;
  unsigned long start_time = get_current_rtc();
  while(size  >= sizeof(sales_table_row_t)) {
    int e = select_year(data, dleft, dright);
    if(e) {
      memcpy(output, data, sizeof(sales_table_row_t));
      output++;
    }
    data++;
    size -= sizeof(sales_table_row_t);
  }
  unsigned long stop_time = get_current_rtc();
  printf("Size = %lu Selectivity=%lf Throughput = %lf GB/s\n",
	 input_size,
	 (sizeof(sales_table_row_t)*(output - output_start))/((double)input_size),
	 ((double)input_size)/(1000*(stop_time - start_time)));
}

int main(int argc, char *argv[])
{
  if(argc < 3) {
    printf("Usage: %s benchmark params\n", argv[0]);
    printf("benchmark = 1 select date params=file date_left date_right\n");
    exit(-1);
  }
  if(atoi(argv[1]) == 1) {
    run_date_select(argv[2], atol(argv[3]), atol(argv[4]));
  }
}
