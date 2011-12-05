
# 
# GNU 'make' file
# 

PY=python
#PY=/cygdrive/c/Python27/python
#PYTEST=$(PY) -m pytest --capture=no
PYTEST=py.test --capture=no

RPCURI		:= http://bzr.json-rpc.org/trunk

.PHONY: all clean FORCE
all:		jsonrpc

clean:
	rm  *.pyc

# Only run tests in this directory.
test:
	@py.test --version || echo "py.test not found; run 'sudo easy_install pytest'?"
	$(PYTEST) *_test.py


# Make the jsonrpc module available for import
jsonrpc:		../../json-rpc FORCE
	@if [ ! -L $@ ]; then						\
	    ln -fs $</python-jsonrpc/jsonrpc $@;			\
	fi
../../json-rpc:		FORCE
	@if [ ! -d $@ ]; then						\
	    bzr checkout $(RPCURI) $@;					\
	fi

# Run only tests with a prefix containing the target string, eg test-blah
test-%:
	$(PYTEST) *$*_test.py

unit-%:
	$(PYTEST) -k $*

# 
# Target to allow the printing of 'make' variables, eg:
# 
#     make print-CXXFLAGS
# 
print-%:
	@echo $* = $($*) 
	@echo $*\'s origin is $(origin $*)

