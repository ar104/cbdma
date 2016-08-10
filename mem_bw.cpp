#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>

void *map_anon_memory(unsigned long size)
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

static __inline__ unsigned long rdtsc(void)
{
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return ( (unsigned long)lo)|( ((unsigned long)hi)<<32 );
}

// Mem
//#define SIZE (80*1024*1024UL)
//#define LOOPS 200UL

// L3
#define SIZE (10*1024*1024UL)
#define LOOPS 800UL

// L2
//#define SIZE (128*1024UL)
//#define LOOPS 128000UL

volatile char *from_space;
volatile char *to_space;

main()
{

  from_space = (volatile char *)map_anon_memory(SIZE);
  to_space = (volatile char *)map_anon_memory(SIZE);
  cpu_set_t my_set;        
  CPU_ZERO(&my_set);       
  CPU_SET(2, &my_set);     
  sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
  // One pass to cache if possible
  for(unsigned long index = 0; index < SIZE; index+=64) {
    to_space[index] = from_space[index];
  }
  unsigned long start = rdtsc();
  for(int loops=0;loops < LOOPS;loops++) {
    for(unsigned long index = 0; index < SIZE; index+=64) {
      to_space[index] = from_space[index];
    }
  }
  unsigned long total_time = rdtsc() - start;
  unsigned long total_bytes = SIZE*LOOPS;
  double bw = ((double)total_bytes)/(total_time);
  printf("BW = %lf B/cycle\n", bw);
}
