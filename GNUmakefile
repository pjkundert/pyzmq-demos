# 
# GNU 'make'-file for building 
.PHONY:		FORCE all zmq pyzmq

all:		zmq pyzmq


zmq:		../zeromq2-1
../zeromq2-1:	FORCE
	@if [ ! -d $@ ]; then						\
		git clone https://github.com/zeromq/zeromq2-1.git $@;	\
	fi
	@if [ ! -r $@/configure ]; then					\
		cd $@; ./autogen.sh;					\
	fi
	@if [ ! -r $@/Makefile ]; then					\
		cd $@;./configure;					\
	fi
	cd $@; make && sudo make install

pyzmq:		../pyzmq
../pyzmq:	zmq FORCE
	@if [ ! -d F$@ ]; then						\
		git clone https://github.com/zeromq/pyzmq.git $@;	\
	fi
	cd $@; python setup.py build_ext --inplace --zmq=/usr/local
	cd $@; python setup.py test
	cd $@; sudo python setup.py install
