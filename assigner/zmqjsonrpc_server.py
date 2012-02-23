#!/usr/bin/env python

import zmq
import zmqjsonrpc

# Create some objects to access remotely
class Boo( object ):
    def first( self, *args ):
        return ", ".join( str( a ) for a in args )

    second              = first

boo                     = Boo()


# Create 0MQ transport
context         = zmq.Context()
socket          = context.socket( zmq.XREP )
socket.bind( "tcp://*:55151" )

# Serve RPC requests forever (or 'til Ctrl-C, whichever comes first),
# to *any* global object.  Restrict access with different 'root' dict
zmqjsonrpc.server( socket=socket, root=globals() ).serve()

# Clean up 0MQ socket (management assumed by server), and context
del server
context.term()
