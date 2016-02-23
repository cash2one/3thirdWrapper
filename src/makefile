DEST=lib3rd_wrapper.a

SOURCE=$(wildcard *.cpp)
OBJ=$(patsubst %.cpp, %.o, $(SOURCE))
CFLAGS= -g -Wall -fPIC -std=c++0x
THIRD_PAHT=/data/3third
OWN_COMM_PATH=/data/own_comm
CC=g++ 

INC=$(THIRD_PAHT)/zookeeper_c/zookeeper/include/

all:$(DEST)

$(DEST):$(OBJ)
	ar -rs $(DEST) $(OBJ)

%.o:%.cpp
	$(CC) $(CFLAGS) -o $@ -c $< $(addprefix -I, $(INC)) 

clean:
	rm -rf $(DEST) *.o

install:
	../script/auto_header.sh 3rd_wrapper.h willrlzhang
	cp *.h ../include
	cp *.a ../lib
