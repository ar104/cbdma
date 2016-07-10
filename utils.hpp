#ifndef _CLOCK_
#define _CLOCK_
#include<sys/time.h>
#include "logging.hpp"
#include<fcntl.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<sys/mman.h>

static void memory_utils_err(const char *op, const char *subop) {
  BOOST_LOG_TRIVIAL(error) << op << "::" << subop << " failed:" <<
  strerror(errno);
}

//! Real time clock
class rtc_clock {
  const char *msg;
  unsigned long dump_interval;
  unsigned long accumulator;
  unsigned long samples;
  unsigned long start_time;
  static unsigned long get_current_rtc()
  {
    struct timeval tm;
    gettimeofday(&tm, NULL);
    return tm.tv_sec*1000000 + tm.tv_usec;
  }

 public:
  rtc_clock(const char *msg_in, 
	    unsigned long dump_interval_in)
    :msg(msg_in),
     dump_interval(dump_interval_in)
  {
    accumulator = 0;
    samples = 0;
    start_time = get_current_rtc();
  }

  static unsigned long current_time()
  {
    return get_current_rtc();
  }

  void sample_interval(unsigned long sample_start)
  {
    unsigned long mark = current_time();
    accumulator += (mark - sample_start);
    samples++;
    if((mark - start_time) >= dump_interval) {
      BOOST_LOG_TRIVIAL(info) << msg
			      << ((double)accumulator/samples);
      accumulator = 0;
      samples = 0;
      start_time = mark;
    }
  }

  static void sleep_us(unsigned long usecs)
  {
    unsigned long start = current_time();
    while(!((current_time() - start) >=  usecs));
  }
};

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

static void read_from_file(int fd,
                           unsigned char *input,
                           unsigned long bytes_to_read) {
  while (bytes_to_read) {
    unsigned long bytes_read = read(fd, input, bytes_to_read);
    if (bytes_read == -1UL) {
      if (errno != EAGAIN) {
        BOOST_LOG_TRIVIAL(fatal) <<
        "Stream readin unsuccessful:" << strerror(errno);
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
static void *map_anon_memory(unsigned long size,
                             bool mlocked,
                             const char *operation,
                             bool zero = false) {
  void *space = mmap(NULL, size > 0 ? size : 4096,
                     PROT_READ | PROT_WRITE,
                     MAP_ANONYMOUS | MAP_SHARED, -1, 0);
  if (space == MAP_FAILED) {
    memory_utils_err(operation, "mmap");
    exit(-1);
  }
  if (mlocked) {
    if (mlock(space, size) < 0) {
      memory_utils_err(operation, "mlock");
    }
  }
  if (zero) {
    memset(space, 0, size);
  }
  return space;
}


#endif
