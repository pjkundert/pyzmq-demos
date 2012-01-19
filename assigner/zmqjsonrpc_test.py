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

    def _hidden( self, *args ):
        return "Can't run me!" + self.first( *args )

boo				= Boo()


def sub( lhs, rhs ):
    return lhs - rhs

def test_base_client():
    global port
    port		       += 1
    
    # Create 0MQ transport
    context			= zmq.Context()
    socket			= context.socket( zmq.REQ )

    # Create the test server and connect client to it
    svr				= context.socket( zmq.XREP )
    svr.setsockopt( zmq.RCVTIMEO, 250 )
    svr.bind( "tcp://*:%d" % ( port ))
    socket.connect( "tcp://localhost:%d" % ( port ))

    svrthr                      = zjr.server_thread( root=globals(), socket=svr )
    svrthr.start()

    # Create callable to method "first" and invoke; then "second"
    remboo			= zjr.client( socket=socket, name="boo" )
    result			= remboo.first( "some", "args" )
    assert result == "some, args"
    result			= remboo.second( "yet", "others" )
    assert result == "yet, others"

    # Try a global method
    remgbl			= zjr.client( socket=socket )
    result			= remgbl.sub( 42, 11 )
    assert result == 31

    # Various ways of failing to invoke nonexistent methods
    try:
        # no such method
        result			= remboo.nothere( "whatever" )
        assert not "nonexistent method found: %s" % result
    except Exception, e:
        assert type(e) is zjr.Error
        assert "No method" in str(e)
        assert "-32601" in str(e)
    # Hidden method (leading '_')
    try:
        result			= remboo._hidden( "whatever" )
        assert not "nonexistent method found: %s" % result
    except Exception, e:
        assert type(e) is zjr.Error
        assert "No method" in str(e)
        assert "-32601" in str(e)
    # No such function
    try:
        result			= remgbl.dir()
        assert not "nonexistent method found: %s" % result
    except Exception, e:
        assert type(e) is zjr.Error
        assert "No method" in str(e)
        assert "-32601" in str(e)

    svrthr.join()
    svr.close()

    # Clean up; destroy proxy, then close sockets, terminate context
    del remboo
    del remgbl
    socket.close()
    context.term()


