import zmq
import zmqjsonrpc
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
        svrthr                  = zmqjsonrpc.server_thread( root=globals(), socket=svr )
        svrthr.name             = "Server-%d" % ( i )
        svrthr.start()
        thr.append( svrthr )

    port                       += 1


    # Create callable to method "first" and invoke; then "second"
    remboo                      = zmqjsonrpc.client( socket=cli, name="boo" )
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
    remgbl                      = zmqjsonrpc.client( socket=cli )
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
            assert type(e) is zmqjsonrpc.Error
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


def test_rpc_session_1_to_N():
    global port

    iface                       = "127.0.0.1"

    # Create 0MQ transport
    context                     = zmq.Context()
    cli                         = context.socket( zmq.REQ )
    cli.connect( "tcp://localhost:%d" % ( port ))

    # Create the test server and connect client to it
    svr                         = context.socket( zmq.XREP )
    svr.bind( "tcp://%s:%d" % ( iface, port ))
    port                       += 1
    pool                        = 5

    # Create a server_session, allowing access to globals (including 'boo').
    # Uses one port for the master socket, one for the monitor socket, plus
    # 'pool' ports for the server pool sockets.
    svrthr                      = zmqjsonrpc.server_session_thread(
        root=globals(), socket=svr, pool=pool, iface=iface, port=port )
    svrthr.start()
    port                        += 1 + pool

    # Create a client_session, attempting to access methods of "boo"
    remses                      = zmqjsonrpc.client_session( socket=cli, name="boo" )

    result                      = remses.first( "all", "good", "boys", 
                                                "deserve", 1, "fudge" )
    assert result.endswith( "all, good, boys, deserve, 1, fudge" )

    del remses
    svrthr.join()
    context.term()

def test_rpc_sessions_M_to_S_x_N():
    """
    Test M client Threads accessing N server Threads, with sessions
    assigned persistently to each client 'til completion.  We'll use a
    single server_session_thread with a pool size of 5, with the
    client_session connecting.
    """
    global port

    iface                       = "127.0.0.1"

    # There are more (M) client than (S*N) server threads, so later clients will
    # be forced to wait for server pool sessions to be released.  Invoke X RPCs
    # in each client session, ensuring that the same server thread executes
    # every call.
    M                           = 10
    S                           = 2
    N                           = 3
    X                           = 1000

    # Create S server_session pools with N Threads.  After this block,
    # 'servers' will contain the addresses of all server_thread pools
    # which all client_session should connect to.
    contexts                    = []
    threads                     = []
    servers                     = []
    for _ in xrange( S ):
        # Allocate the server_session pool's master socket
        contexts.append( zmq.Context() )
        servers.append( "tcp://%s:%d" % ( iface, port ))
        port                   += 1
        socket                 = contexts[-1].socket( zmq.XREP )
        socket.bind( servers[-1] )

        # Create a server_session on the master socket, allowing access to
        # globals (including 'boo').  Used one port for the monitor socket, plus
        # 'N' ports for the server pool, starting at 'port'.
        threads.append( zmqjsonrpc.server_session_thread(
                root=globals(), socket=socket, pool=N, iface=iface, port=port ))
        port                   += 1 + N

    # Create M client_session Threads, eacho attempting to access
    # methods of "boo" via any available sessions from the
    # server_session pools.
    for m in xrange( M ):
        # Create the client_session's socket, connecting it to every
        # server_session pool's master socket.
        contexts.append( zmq.Context() )
        socket                  = contexts[-1].socket( zmq.REQ )
        for address in servers:
            socket.connect( address )

        def target( m, X, socket ):
            print "Client %2d: starts" % ( m )
            remote              = zmqjsonrpc.client_session( socket=socket, name="boo" )
            print "Client %2d: obtains session" % ( m )
            first               = None
            for _ in xrange( X ):
                result          = remote.first( "all", "good", "boys", 
                                                "deserve", m, "fudge" )
                if first is None:
                    first       = result
                    print "Client %2d: %s" % ( m, first )
                    continue
                assert result == first
            print "Client %2d: done" % ( m )

        threads.append( threading.Thread(
                target=target, kwargs={ "m": m, "X": X, "socket": socket, } ))

    # Start all threads, then join in reversed order -- clients first, then
    # servers.  All contexts should then close cleanly.
    for t in threads:
        t.start()
    for t in reversed( threads ):
        t.join()
    for c in contexts:
        c.term()
