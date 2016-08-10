#include<sys/time.h>
#include<sched.h>
#include<stdio.h>

static __inline__ unsigned long rdtsc(void)
{
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return ( (unsigned long)lo)|( ((unsigned long)hi)<<32 );
}

static unsigned long get_current_rtc()
{
  struct timeval tm;
  gettimeofday(&tm, NULL);
  return tm.tv_sec*1000000 + tm.tv_usec;
}

main()
{
  
  /// Bind
  cpu_set_t my_set;        
  CPU_ZERO(&my_set);       
  CPU_SET(2, &my_set);     
  sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
  /////////
  unsigned long start = get_current_rtc();
  unsigned long start_tsc = rdtsc();
  while(get_current_rtc() < start + 1000000);
  unsigned long stop_tsc = rdtsc();
  printf("%lu", stop_tsc - start_tsc);
}
