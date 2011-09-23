# 
# GNU 'make'-file for building 
.PHONY:			FORCE all zmq zmq-install pyzmq pyzmq-install

#ZMQVER 		:= 3-0
ZMQVER 		:= 2-1

all:			pyzmq

zmq:			../zeromq$(ZMQVER) FORCE
	@if [ ! -d $< ]; then						\
	    git clone https://github.com/zeromq/zeromq$(ZMQVER).git $<;	\
	fi
	cd $<; git pull
	@if [ ! -r $</configure ]; then					\
	    cd $<; ./autogen.sh;					\
	fi
	@if [ ! -r $</Makefile ]; then					\
	    cd $<;./configure;						\
	fi
	cd $<; make

zmq-install:		../zeromq$(ZMQVER) FORCE
	cd $<; sudo -n install

pyzmq:			../pyzmq zmq FORCE
	@if [ ! -d $@ ]; then						\
	    git clone https://github.com/zeromq/pyzmq.git $<;		\
	fi
	cd $@; python setup.py configure --zmq=/usr/local

pyzmq-install:		../pyzmq
	cd $@; sudo -n python setup.py install

pyzmq-test:		../pyzmq
	cd $<; python setup.py configure --zmq=/usr/local
	cd $<; python setup.py build_ext --inplace
	cd $<; python setup.py test
