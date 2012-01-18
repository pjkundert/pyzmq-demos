import zmq
import zmqjsonrpc as zjr
import zhelpers
import json
import threading
import time
import traceback

port				= 11223




class Boo( object ):
    def first( self, *args ):
        return ", ".join( str( a ) for a in args )

    second = first

boo				= Boo()

def test_base_client():
    global port
    port		       += 1
    
    
    # Create 0MQ transport
    context			= zmq.Context()
    socket			= context.socket( zmq.REQ )
    remote			= zjr.client( socket=socket, name="boo" )

    # Create the test server and connect client to it
    svr				= context.socket( zmq.XREP )
    svr.setsockopt( zmq.RCVTIMEO, 250 )
    svr.bind( "tcp://*:%d" % ( port ))
    socket.connect( "tcp://localhost:%d" % ( port ))

    svrthr                      = zjr.server_thread( socket=svr, root=globals() )
    svrthr.start()

    # Create callable to method "first" and invoke; then "second"
    result1			= remote.first( "some", "args" )
    assert result1 == "some, args"
    result2			= remote.second( "yet", "others" )
    assert result2 == "yet, others"

    svrthr.join()
    svr.close()

    # Clean up; destroy proxy, then close sockets, terminate context
    del remote
    socket.close()
    context.term()


