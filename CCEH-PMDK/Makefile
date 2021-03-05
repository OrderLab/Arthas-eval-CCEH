.PHONY: all clean

CXX := wllvm++
CFLAGS := -std=c++11 -I./ -lrt -O0

all: test

test: src/test.cpp CCEH
	$(CXX) $(CFLAGS) -o bin/cceh src/test.cpp src/CCEH.o -lpmemobj -lpmem -DINSERT -DSEARCH
	$(CXX) $(CFLAGS) -o bin/cceh_search src/test.cpp src/CCEH.o -lpmemobj -lpmem -DSEARCH
	$(CXX) $(CFLAGS) -o bin/ccehCoW src/test.cpp src/CCEH_CoW.o -lpmemobj -lpmem -DINSERT -DSEARCH
	$(CXX) $(CFLAGS) -o bin/ccehCoW_search src/test.cpp src/CCEH_CoW.o -lpmemobj -lpmem -DSEARCH

CCEH: src/CCEH.h src/CCEH.cpp
	$(CXX) $(CFLAGS) -c -o src/CCEH.o src/CCEH.cpp -DINPLACE -lpmemobj -lpmem -g
	$(CXX) $(CFLAGS) -c -o src/CCEH_CoW.o src/CCEH.cpp -lpmemobj -lpmem -g
	
clean:
	rm -rf src/*.o bin/* 
