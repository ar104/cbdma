.PHONY:all

all: convert_store_sales benchmark benchmark_prefetch mem_bw calibrate_tsc lz4comp_mt lz4uncomp_mt

CFLAGS=-O3

LIBS = -lc -lcyclone -lcraft -lzmq -lboost_system -lboost_date_time -lpmemobj -lpmem -lpthread -lboost_thread -llz4

CC=g++

convert_store_sales: convert_store_sales.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $@

benchmark: benchmark.c
	$(CC) $(CFLAGS) $^ -lpthread -o $@

benchmark_prefetch: benchmark_prefetch.c
	$(CC) $(CFLAGS) $^ -lpthread -o $@

calibrate_tsc:calibrate_tsc.c
	$(CC) $(CFLAGS) $^ -o $@

lz4comp_mt:lz4comp_mt.cpp
	$(CXX) $(CFLAGS) $^ $(LIBS) -o $@

lz4uncomp_mt:lz4uncomp_mt.cpp
	$(CXX) $(CFLAGS) $^ $(LIBS) -o $@

mem_bw: mem_bw.cpp
	$(CXX) $(CFLAGS) $^ -lpthread -o $@

clean:
	rm -f convert_store_sales benchmark benchmark_prefetch mem_bw calibrate_tsc lz4comp_mt lz4uncomp_mt
