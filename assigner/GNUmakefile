
# 
# GNU 'make' file
# 

PY=python
PYTEST=$(PY) -m pytest --capture=no

.PHONY: all clean FORCE
all:				

clean:
	rm  *.pyc

# Only run tests in this directory.
test:
	@py.test --version || echo "py.test not found; run 'sudo easy_install pytest'?"
	$(PYTEST) *_test.py

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

