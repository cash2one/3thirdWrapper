DEST=test

SOURCE=$(wildcard *.cpp)
OBJ=$(patsubst %.cpp, %.o, $(SOURCE))
CFLAGS= -g -Wall -fPIC -std=c++0x
THIRD_PAHT=/data/3third
OWN_COMM_PATH=/data/own_comm
CC=g++

INC=/data/3third_wapper/include/  /data/3third/zookeeper_c/zookeeper/include/  /data/3third/memcache/libmemcached-1.0.18/libmemcached/
LIB=3rd_wrapper comm_tool  zookeeper_mt memcached pthread 
LIBPATH=/data/3third_wapper/lib/  /data/own_comm/lib/  /data/3third/zookeeper_c/zookeeper/lib/ /data/3third/memcache/libmemcached-1.0.18/libmemcached/.libs/

all:$(DEST)

$(DEST):$(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(addprefix -L, $(LIBPATH))  $(addprefix -l, $(LIB))	

%.o:%.cpp
	$(CC) $(CFLAGS) -o $@ -c $< $(addprefix -I, $(INC)) 

clean:
	rm -rf $(DEST) *.o

install:
	../script/auto_header.sh 3rd_wrapper.h willrlzhang
	cp *.h ../include
	cp *.a ../lib
