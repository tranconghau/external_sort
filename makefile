CC=g++
CFLAGS=-I. -std=c++11 -lpthread -O2 -DVERBOSE
BIN = ex_sort

OBJ = MinHeap.o Utilities.o ex_sort.o

%.o: %.cpp
	@$(CC) -c -o $@ $< $(CFLAGS)

$(BIN): $(OBJ)
	@$(CC) -o $@ $^ $(CFLAGS) 

all: $(BIN)


clean:
	@rm -rf $(BIN) $(OBJ)
