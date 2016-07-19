#include <stdio.h>
#include <sched.h>
#include "select.h"

static __inline__ unsigned long long rdtsc(void)
{
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}


#define SIZE (20*1024*1024UL)
#define LOOPS 200UL

main()
{

  volatile char *space = (volatile char *)map_anon_memory(SIZE); // 20 MB
  register char total = 0;
  cpu_set_t my_set;        
  CPU_ZERO(&my_set);       
  CPU_SET(2, &my_set);     
  sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
  // Bring into L3 cache
  for(unsigned long index = 0; index < SIZE; index++) {
    total = space[index];
  }
  unsigned long start = rdtsc();
  for(int loops=0;loops < LOOPS;loops++) {
    for(unsigned long index = 0; index < SIZE; index++) {
      total = space[index];
    }
  }
  unsigned long total_time = rdtsc() - start;
  unsigned long total_bytes = SIZE*LOOPS;
  double bw = ((double)total_bytes)/(total_time);
  printf("BW = %lf B/cycle\n", bw);
}
