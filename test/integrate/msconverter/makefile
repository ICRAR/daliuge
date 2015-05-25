ifeq ($(VENDOR),cray)
	CC=cc -dynamic
else
	CC=mpic++
endif
CCFLAGS=
LDFLAGS=-lcasa_tables -lcasa_casa
STMANFILES=libadiosstman.so
TARGET=ms_converter ms_checker

all: ${TARGET} cl

${TARGET}: ${TARGET:=.cc} 
	$(CC) $@.cc -o $@ $(CCFLAGS) $(LDFLAGS) $(STMANFILES) 

cl:
	rm -rf *.casa *.e* *.o*

clean:cl
	rm -rf ${TARGET} *.dSYM *.so

re: clean all
