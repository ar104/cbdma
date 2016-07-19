#include"lz4_mt.h"
#include<pthread.h>

static unsigned long get_current_rtc()
{
  struct timeval tm;
  gettimeofday(&tm, NULL);
  return tm.tv_sec*1000000 + tm.tv_usec;
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

sales_table_row_t* run_date_select(sales_table_row_t *data,
				   unsigned long size,
				   sales_table_row_t *output,
				   unsigned long dleft,
				   unsigned long dright)
{
  while(size  >= sizeof(sales_table_row_t)) {
    volatile int e = select_year(data, dleft, dright);
    if(e) {
      //memcpy(output, data, sizeof(sales_table_row_t));
      output++;
    }
    data++;
    size -= sizeof(sales_table_row_t);
  }
  return output;
}

typedef struct {
  sales_table_row_t *volatile data_in;
  volatile unsigned long size;
  sales_table_row_t *data_out;
  volatile bool busy;
  volatile bool eof;
  unsigned long dleft;
  unsigned long dright;
} channel_t;
  
void* consumer(void * arg)
{
  channel_t *channel = (channel_t *)arg;
  sales_table_row_t *output_start = channel->data_out;
  unsigned long start_time;
  unsigned long total_input_size = 0;
  unsigned long total_time = 0;
  while(!channel->eof) {
    while(!channel->busy);
    if(channel->eof)
      break;
    start_time = get_current_rtc();
    total_input_size += channel->size;
    channel->data_out = run_date_select(channel->data_in,
					channel->size,
					channel->data_out,
					channel->dleft,
					channel->dright);
    total_time += (get_current_rtc() - start_time);
    channel->busy = false;
  }
  printf("Size = %lu Selectivity=%lf Throughput = %lf GB/s\n",
	 total_input_size,
	 (sizeof(sales_table_row_t)*(channel->data_out - output_start))/((double)total_input_size),
	 ((double)total_input_size)/(1000*total_time));
  return NULL;
}


int main(int argc, char *argv[])
{
  if(argc < 3) {
    printf("Usage: %s benchmark params\n", argv[0]);
    printf("benchmark = 1 select date params=file date_left date_right\n");
    exit(-1);
  }
  if(atoi(argv[1]) == 1) {
    channel_t channel;
    pthread_t exec_thread;
    channel.data_in  = (sales_table_row_t *)map_file(argv[2],
						     (unsigned long *)&channel.size);
    channel.data_out = (sales_table_row_t *)map_anon_memory(channel.size);
    channel.dleft = atol(argv[3]);
    channel.dright = atol(argv[4]);
    channel.busy = false;
    channel.eof = false;
    if(pthread_create(&exec_thread, NULL, consumer, &channel) != 0) {
      printf("failed to launch exec thread\n");
      exit(-1);
    }
    unsigned long start_time = get_current_rtc();
    channel.busy = true;
    while(channel.busy);
    unsigned long total_time = get_current_rtc() - start_time;
    channel.eof = true;
    channel.busy = true;
    pthread_join(exec_thread, NULL);
    printf("TIME = %lu\n", total_time);
  }
}
