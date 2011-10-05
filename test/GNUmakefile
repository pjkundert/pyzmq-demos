
# 
# 0MQ C++ Unit Tests
# 
#     Here is where we test the functionality of the 0MQ 0MQ
# implementation underlying the pyzmq Python 0MQ bindings.  We use the
# CUT C++ Unit Test framework, to allow us to

VERSION		= 4.1.2
ARCH		= $(shell uname -m | sed -e s/i.86/i386/ -e s/9000.*/hppa/)
#DEBUG		= yes 		# anything turns on -g
#OPT		= 3		# defaults to 3

# We want to deduce these, unless they are explicitly set...
ifeq ($(origin CC),default)
    CC		=
endif
ifeq ($(origin CXX),default)
    CXX		=
endif

# Default flags for building test executables.  Not required by
# default (only cut.H is required by client programs)!  Figure out
# deguggable vs. optimized, limit optimization level to practical limit
ifeq ($(ARCH),i386)
    ifneq ($(DEBUG),)
	CXXFLAGS	+= -g
    endif

    ifneq ($(OPT),)
	CXXFLAGS	+= -O$(OPT)
    else
	CXXFLAGS	+= -O3
    endif

    ifeq ($(CXX),icc)
        CXXFLAGS += -Wall
    else
        ifeq ($(CXX),)
            CXX	 = g++
        endif
        CXXFLAGS += -pipe		 		\
		   -Wall
    endif
endif
ifeq ($(ARCH),sun4v)
    ifeq ($(CXX),)
        CXX	 = CC
    endif
    CXXFLAGS 		+= +w
    ifneq ($(DEBUG),)
	CXXFLAGS	+= -g
    endif

    ifneq ($(OPT),)
	CXXFLAGS	+= -xO$(OPT)
    else
	CXXFLAGS	+= -xO4
    endif
endif
ifeq ($(ARCH),x86_64)
    ifneq ($(DEBUG),)
	CXXFLAGS	+= -g
    endif

    ifneq ($(OPT),)
	CXXFLAGS	+= -O$(OPT)
    else
	CXXFLAGS	+= -O3
    endif

    ifeq ($(CXX),icc)
        CXXFLAGS += -Wall
    else
        ifeq ($(CXX),)
            CXX	 = g++
        endif
        CXXFLAGS += -pipe		 		\
		   -Wall
    endif
endif
ifeq ($(ARCH),hppa)
    ifneq ($(DEBUG),)
	CXXFLAGS	+= -g
    else
	ifneq ($(OPT),)
	    # Opimisation level specified (incompatible with -g)...
	    ifneq ($(LWOPT),0)
	        # Specific (non-zero) optimisation level set; use it
		CXXFLAGS+= +O$(OPT)
            endif
	else
            CXXFLAGS	+= +O2
	endif
    endif

    ifeq ($(CXX),)
        CXX	= aCC
    endif
    CXXFLAGS   += -AA +p				\
		  +DAportable				 \
		  -D_HPUX_SOURCE			  \
		  -D_POSIX_C_SOURCE=199506L		   \
		  -D_RWSTD_MULTI_THREAD
    # 
    # on HP-UX aCC, the -mt flag sets the right -D... and -l... flags for thread-safety.
    # Even if -mt is used to compile all objects, if -mt isn't used to link the target,
    # most all 'pthread_...' functions will compile to stubs, which is most efficient
    # in single-threaded executables.
    # 
    CXXFLAGS	       += -mt
endif

all:			test_subs

CXXFLAGS	+= -I. -I../../zeromq3-0/include -I../../cut
LDFLAGS		+= -L. -L../../zeromq3-0/src/.libs -static
LOADLIBES	+= -lzmq

test_subs:		CXXFLAGS  += -DTESTSTANDALONE
test_subs:		LOADLIBES += -lzmq -luuid -lpthread
test_subs:		test_subs.C

test:			all
	./test_subs
