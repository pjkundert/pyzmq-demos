#!/usr/bin/env python

import zmq
import zmqjsonrpc

# Create 0MQ transport
context         = zmq.Context()
socket          = context.socket( zmq.REQ )
socket.connect( "tcp://localhost:55151" )
remote          = zmqjsonrpc.client( socket=socket, name="boo" )

# Creates callable to method "first" and invokes; then "second"
print "first:  %s" % ( remote.first( "some", "args" ))
print "second: %s" % ( remote.second( "yet", "others" ))

# Clean up; destroy proxy (closes socket), terminate context
del remote
context.term()
