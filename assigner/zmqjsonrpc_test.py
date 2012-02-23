import zmq
import zmqjsonrpc as zjr
import zhelpers
import json
import threading
import time
import traceback

port                            = 11223




class Boo( object ):
    def first( self, *args ):
        return "%s: %s" % ( 
            threading.current_thread().name, ", ".join( str( a ) for a in args ))

    second = first

    def _hidden( self, *args ):
        return "Can't run me!" + self.first( *args )

boo                             = Boo()


def sub( lhs, rhs ):
    return lhs - rhs


def test_rpc_base_1_to_N():
    global port

    N                           = 5

    # Create 0MQ transport
    context                     = zmq.Context()
    cli                         = context.socket( zmq.REQ )
    cli.bind( "tcp://*:%d" % ( port ))

    # Create the test server and connect client to it
    thr                         = []
    for i in xrange( N ):
        svr                     = context.socket( zmq.XREP )
        svr.connect( "tcp://localhost:%d" % ( port ))
        svrthr                  = zjr.server_thread( root=globals(), socket=svr )
        svrthr.name             = "Server-%d" % ( i )
        svrthr.start()
        thr.append( svrthr )

    port                       += 1


    # Create callable to method "first" and invoke; then "second"
    remboo                      = zjr.client( socket=cli, name="boo" )
    result                      = remboo.first( "some", "args" )
    assert result.endswith( "some, args" )
    result                      = remboo.second( "yet", "others" )
    assert result.endswith( "yet, others" )

    # Try a global method; uses the same 'cli' socket, hence same server
    # 
    # NOTE: We are *reusing* the same 'cli' 0MQ socket.  This is OK, since all
    # client instances are in the same thread.  However, since the 'client'
    # assumes authority for the socket, they will *both* try to clean it up
    # (close it).  Generally, we'd suggest you have a separate socket for each
    # 'client' -- which would be required, anyway, if each is invoked from
    # within a separate application Thread.
    remgbl                      = zjr.client( socket=cli )
    result                      = remgbl.sub( 42, 11 )
    assert result == 31

    # Various ways of failing to invoke nonexistent methods
    for rpc in [ 
        remboo.nothere,                 # no such method
        remboo._hidden,                 # Hidden method (leading '_')
        remgbl.dir,                     # No such function
        ]:
        try:
            result              = rpc()
            assert not "nonexistent method found: %s" % result
        except Exception, e:
            assert type(e) is zjr.Error
            assert "Method not found" in str(e)
            assert "-32601" in str(e)

    # Stop all (stoppable) threads, cleaning up all server 0MQ sockets.  Dispose
    # of all client instances, closing the (shared) 0MQ socket multiple times,
    # which is OK with 0MQ.
    while thr:
        thr.pop().join()
    del remboo
    del remgbl # (Not strictly necessary; 'cli' already closed)
    context.term()


def test_rpc_session():
    global port
    
    # Create 0MQ transport
    context                     = zmq.Context()
    cli                         = context.socket( zmq.REQ )
    cli.connect( "tcp://localhost:%d" % ( port ))

    # Create the test server and connect client to it
    svr                         = context.socket( zmq.XREP )
    svr.bind( "tcp://*:%d" % ( port ))
    port                       += 1
    pool                        = 5

    # Create a server_session, allowing access to globals (including
    # 'boo') Uses one port for the monitor socket, plus 'pool' ports
    # for the server pool.
    svrthr                      = zjr.server_session_thread(
        root=globals(), socket=svr, pool=pool, iface="127.0.0.1", port=11245 )
    svrthr.start()
    port                        += 1 + pool

    # Create a client_session, attempting to access methods of "boo"
    remses                      = zjr.client_session( socket=cli, name="boo" )

    result                      = remses.first( "all", "good", "boys", 
                                                "deserve", 1, "fudge" )
    assert result.endswith( "all, good, boys, deserve, 1, fudge" )

    del remses
    svrthr.join()
    context.term()
