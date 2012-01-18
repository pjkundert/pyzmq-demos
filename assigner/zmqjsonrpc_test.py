import zmq
import zmqjsonrpc as zjr
import zhelpers
import json
import threading
import time
import traceback

port				= 11223


class stoppable( threading.Thread ):
    """
    Supports external thread-safe Thread stop signalling.
    """
    def __init__( self, *args, **kwargs ):
        self.__stop	 	= threading.Event()
        super( stoppable, self ).__init__( *args, **kwargs )

    def stop( self ):
        self.__stop.set()

    def stopped( self ):
        return self.__stop.is_set()

    def join( self, *args, **kwargs ):
        self.stop()
        super( stoppable, self ).join( *args, **kwargs )


class server_thread( stoppable ):
    """
    Simulate a simple JSON-RPC style server on the given socket.  The optional
    latency is the frequency at which the thread checks to see if it's been
    stopped.  The JSON-RPC spec allows a list of 1 or more requests to be
    transported, which maps nicely onto the 0MQ multipart messages.
    """
    def __init__( self, socket, latency=None, **kwargs ):
        self._socket		= socket
        self._latency		= float( latency or 0.25 )
        super( server_thread, self ).__init__( **kwargs )

    def run( self ):
        poller			= zmq.core.poll.Poller()
        poller.register( self._socket, flags=zmq.POLLIN )
        while not self.stopped():
            for socket, _ in poller.poll( timeout=self._latency ):
                replies		= []
                received	= socket.recv_multipart()
                separator	= received.index('')
                prefix		= received[:separator+1]
                print "Server rx. [%s]" % (
                    ", ".join( [ zhelpers.format_part( m )
                                 for m in received ] ))
                for request in received[separator+1:]:
                    try:
                        req 	= json.loads( request )
                        assert req['jsonrpc'] == "2.0"
                    except Exception, e:
                        rpy 	= {
                            "jsonrpc":	zjr.JSONRPC_VERSION,
                            "id":	None,
                            "error":    zjr.Error(zjr.INVALID_REQUEST,
                                                  data="Bad JSON-RPC").__dict__,
                        }
                    else:
                        result 	= ", ".join( [ str( p )
                                               for p in req['params']] )
                        rpy     = {
                            "jsonrpc":  req['jsonrpc'],
                            "id":       req['id'],
                            "result":   result,
                            "error":    None,
                        }
                    replies.append( json.dumps( rpy, **zjr.JSON_OPTIONS ))

                print "Server tx. [%s]" % (
                    ", ".join( [ zhelpers.format_part( m )
                                 for m in prefix + replies ] ))
                socket.send_multipart( replies, prefix=prefix )


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

    svrthr                      = server_thread( socket=svr )
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


class zmqwrapper( stoppable ):
    """
    Handle incoming traffic on a supplied socket, 'til stopped.  Assumes that
    the 0MQ socket has a timeout, if caller expects run to check stopped with
    any regularity.
    """
    def __init__( self, socket, handler, *args, **kwargs ):
        super( zmqwrapper, self ).__init__( *args, **kwargs )
        self.__socket		= socket
        self.__handler		= handler

    def run( self ):
        while not self.stopped():
            request		= self.__socket.recv_multipart()
            if request:
                print "Request: [%s]" % (
                    ", ".join(  [ zhelpers.format_part( m )
                                  for m in request ] ))
                self.__handler.handleRequest( request )


