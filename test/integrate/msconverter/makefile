ADIOSSTMAN_INC=-I/Users/jason/Desktop/storageManager/adiosStMan
ADIOSSTMAN_LIB=-L/Users/jason/Desktop/storageManager/adiosStMan


ifeq ($(VENDOR),cray)
	CC=cc -dynamic
else
	CC=mpic++
endif

CCFLAGS=${ADIOSSTMAN_INC}
LDFLAGS=-lcasa_tables -lcasa_casa -ladiosstman ${ADIOSSTMAN_LIB}

TARGET=ms_converter ms_checker

all: ${TARGET} cl

${TARGET}: ${TARGET:=.cc} 
	$(CC) $@.cc -o $@ $(CCFLAGS) $(LDFLAGS)

cl:
	rm -rf *.casa *.e* *.o*

clean:cl
	rm -rf ${TARGET} *.dSYM *.so

re: clean all
