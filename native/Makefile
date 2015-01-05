CC=gcc
CFLAGS=-I.
DEPS = http.h
OBJ = dorkdb.o
EXE = dorkdb

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

dorkdb: $(OBJ)
	gcc -o $@ $^ $(CFLAGS) $(LIBS)


.PHONY: clean

clean:
	rm -f *.o *~ $(EXE)