#include "lz4_mt.h"
#include<pthread.h>
#include<sched.h>
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
  unsigned long start_time, total_time = 0;
  unsigned long total_input_size = 0;

  /// Bind
  cpu_set_t my_set;        
  CPU_ZERO(&my_set);       
  CPU_SET(1, &my_set);     
  sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
  /////////
  
  while(!channel->eof) {
    while(!channel->busy);
    if(channel->eof)
      break;
    total_input_size += channel->size;
    start_time = get_current_rtc();
    channel->data_out = run_date_select(channel->data_in,
					channel->size,
					channel->data_out,
					channel->dleft,
					channel->dright);
    total_time       +=  (get_current_rtc() - start_time);
    channel->busy = false;
  }
  printf("Size = %lu Selectivity=%lf Throughput = %lf GB/s\n",
	 total_input_size,
	 (sizeof(sales_table_row_t)*(channel->data_out - output_start))/((double)total_input_size),
	 ((double)total_input_size)/(1000*total_time));
  return NULL;
}

typedef struct {
  int me;
  char * volatile range_start;
  char * volatile range2_start;
  volatile unsigned long range_size;
  volatile bool busy;
  volatile bool terminate;
} prefetch_t;

void * prefetch(void *arg)
{
  prefetch_t *p = (prefetch_t *)arg;

  /// Bind
  cpu_set_t my_set;        
  CPU_ZERO(&my_set);       
  CPU_SET(2 + p->me, &my_set);     
  sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
  /////////
  volatile char total = 0;
  while(!p->terminate) {
    while(!p->busy);
    if(p->terminate)
      break;
    while(p->range_size) {
      unsigned long chunk = (p->range_size  > 64) ? 64:p->range_size;
      total = total + *p->range_start;
      //__builtin_prefetch(p->range_start, 0, 1);
      //__builtin_prefetch(p->range2_start, 1, 2);
      p->range_size  -= chunk;
      p->range_start += chunk;
      p->range2_start += chunk;
    }
    p->busy = false;
  }
  return NULL;
}

void do_prefetch(char *range,
		 char *range2,
		 unsigned long size,
		 prefetch_t *prefetchers,
		 int prefetchers_cnt)
{
  unsigned long chunk = size/prefetchers_cnt;
  for(int i=0;i<prefetchers_cnt;i++) {
    prefetchers[i].range_start = range;
    prefetchers[i].range2_start = range2;
    prefetchers[i].range_size  = chunk;
    if(i == prefetchers_cnt)
      prefetchers[i].range_size += size %prefetchers_cnt;
    range  += prefetchers[i].range_size;
    range2 += prefetchers[i].range_size; 
    prefetchers[i].busy = true;
  }
  for(int i=0;i<prefetchers_cnt;i++) {
    while(prefetchers[i].busy);
  }
}

int main(int argc, char *argv[])
{
  if(argc < 6) {
    printf("Usage: %s benchmark params\n", argv[0]);
    printf("benchmark = 1 select date params=file date_left date_right prefetch_cores\n");
    exit(-1);
  }
  int prefetch_cores = atoi(argv[5]);
  prefetch_t prefetchers[prefetch_cores];
  pthread_t prefetcher_threads[prefetch_cores];
  printf("prefetch_cores = %d\n", prefetch_cores);
  for(int i=0;i<prefetch_cores;i++) {
    prefetchers[i].busy = false;
    prefetchers[i].terminate = false;
    prefetchers[i].me = i;
    if(pthread_create(&prefetcher_threads[i], NULL, prefetch, &prefetchers[i]) != 0) {
      printf("failed to launch exec thread\n");
      exit(-1);
    }
  }
  if(atoi(argv[1]) == 1) {
    channel_t channel;
    pthread_t exec_thread;
    unsigned long size;
    sales_table_row_t *data_in = (sales_table_row_t *)map_file(argv[2], &size);
    unsigned long chunksize = ((8*1024*1024)/sizeof(sales_table_row_t))*sizeof(sales_table_row_t);
    channel.data_out = (sales_table_row_t *)map_anon_memory(size);
    channel.dleft = atol(argv[3]);
    channel.dright = atol(argv[4]);
    channel.eof = false;
    channel.busy = false;
    if(pthread_create(&exec_thread, NULL, consumer, &channel) != 0) {
      printf("failed to launch exec thread\n");
      exit(-1);
    }
    unsigned long start_time = get_current_rtc();
    while(size != 0) {
      channel.data_in = data_in;
      unsigned long round_size = (chunksize > size) ? size:chunksize;
      channel.size    = round_size;
      char *data_out = (char *)channel.data_out;
      channel.busy    = true;
      size -= round_size;
      data_in += (round_size/sizeof(sales_table_row_t));
      data_out += (round_size/sizeof(sales_table_row_t));
      if(size > 0)
	do_prefetch((char *)data_in,
		    data_out,
		    size > chunksize ? chunksize:size,
		    prefetchers,
		    prefetch_cores);
      while(channel.busy);
    }
    unsigned long total_time = get_current_rtc() - start_time;
    channel.eof = true;
    channel.busy = true;
    pthread_join(exec_thread, NULL);
    for(int i=0;i<prefetch_cores;i++) {
      prefetchers[i].terminate = true;
      prefetchers[i].busy = true;
      pthread_join(prefetcher_threads[i], NULL);
    }
    printf("TOTAL TIME = %lu\n", total_time);
  }
}
