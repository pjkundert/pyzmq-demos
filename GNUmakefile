# 
# GNU 'make'-file for building 
.PHONY:			FORCE all zmq zmq-install pyzmq pyzmq-install

#ZMQVER 		:= 2-1
ZMQVER 		:= 3-0
ZMQURI		:= git://github.com/pjkundert/zeromq$(ZMQVER).git 
PYZURI		:= git://github.com/pjkundert/pyzmq.git
CUTURI		:= git://github.com/pjkundert/cut.git

all:			pyzmq

test:		../cut zmq
	@if [ ! -d $< ]; then						\
	    git clone $(CUTURI) $<;					\
	fi
	cd $@; make test

../zeromq$(ZMQVER):
	    git clone $(ZMQURI) $@

zmq:			../zeromq$(ZMQVER) FORCE
	@if [ ! -r $</configure ]; then					\
	    cd $<; ./autogen.sh;					\
	fi
	@if [ ! -r $</Makefile ]; then					\
	    cd $<;./configure;						\
	fi
	cd $<; make V=1

zmq-clean::		../zeromq$(ZMQVER) FORCE
	cd $<; make -k distclean

zmq-clean::		../zeromq$(ZMQVER) FORCE
	cd $<; make clean

zmq-install:		../zeromq$(ZMQVER) FORCE
	cd $<; sudo -n make install

zmq-test:		../zeromq$(ZMQVER) FORCE
	cd $</tests/.lib; LD_LIBRARY_PATH=

../pyzmq:
	git clone $(PYZURI) $@

pyzmq:			../pyzmq zmq FORCE
	cd $<; python setup.py configure --zmq=/usr/local

pyzmq-clean:		../pyzmq FORCE
	cd $<; python setup.py clean

pyzmq-install:		../pyzmq FORCE
	cd $<; sudo -n python setup.py install

pyzmq-test:		../pyzmq FORCE
	cd $<; python setup.py configure --zmq=/usr/local
	cd $<; python setup.py build_ext --inplace
	cd $<; python setup.py test
