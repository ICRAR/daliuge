ADIOSSTMAN_INC=-I/home/jason/AdiosStMan
ADIOSSTMAN_LIB=-L/home/jason/AdiosStMan
#ADIOSSTMAN_INC=-I/home/jwang/AdiosStMan
#ADIOSSTMAN_LIB=-L/home/jwang/AdiosStMan
#ADIOSSTMAN_INC=-I/Users/jason/Desktop/AdiosStMan
#ADIOSSTMAN_LIB=-L/Users/jason/Desktop/AdiosStMan

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
