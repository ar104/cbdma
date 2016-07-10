.PHONY:all

all: convert_store_sales lz4comp_mt

CFLAGS=-O3

LIBS = -lc -lcyclone -lcraft -lzmq -lboost_system -lboost_date_time -lpmemobj -lpmem -lpthread -lboost_thread

CC=g++

convert_store_sales: convert_store_sales.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $@

lz4comp_mt: lz4comp_mt.cpp
	$(CC) $(CFLAGS) $^ $(LIBS) -o $@


clean:
	rm -f convert_store_sales lz4_comp_mt *.o
