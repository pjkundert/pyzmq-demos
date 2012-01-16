import zmq
import zmqjsonrpc
import zhelpers
import json
import threading
import time

port				= 11223


def test_simulate_server():
    global port
    port 		       += 1
    clictx                      = zmq.Context()
    cli                         = clictx.socket( zmq.REQ )
    cli.connect( "tcp://localhost:%d" % ( port ))

    svrctx                      = zmq.Context()
    svr                         = svrctx.socket( zmq.XREP )
    svr.bind( "tcp://*:%d" % ( port ))

    # Simulate the Server side of the 0MQ JSON-RPC request, receiving
    # and processing the raw 0MQ request.
    def svrfun():
        request         	= svr.recv_multipart()
        print "Server rx. [%s]" % (
            ", ".join( [ zhelpers.format_part( m )
                         for m in request ] ))
        assert len( request ) == 5
        assert request[1] == ""	   	     # Route prefix separator
        assert request[2].lower() == "1"     # Session ID
        assert request[3].lower() == zmqjsonrpc.CONTENT_TYPE
        prefix			= request[:2]
        sessid                  = request[2:3]

        requestdict             = json.loads( request[4] )
        assert requestdict['method'] == "boo.foo"
        assert len( requestdict['params'] ) == 3

        result                  = ", ".join( [ str( p )
                                               for p in requestdict['params']] )
        replydict               = {
            "jsonrpc":          requestdict['jsonrpc'],
            "id":               requestdict['id'],
            "result":           result,
            "error":            None,
            }
        reply		       	= [json.dumps( replydict,
                                               **zmqjsonrpc.JSON_OPTIONS)]
        print "Server tx. [%s]" % (
            ", ".join( [ zhelpers.format_part( m )
                         for m in prefix + sessid + reply ] ))
            
        svr.send_multipart( sessid + reply, prefix=prefix )

    svrthr                      = threading.Thread( target=svrfun )
    svrthr.start()

    pxy                         = zmqjsonrpc.ServiceProxy(
                                      socket=zmqjsonrpc.session_socket(
                                          cli, sessid=["1"] ),
                                      serviceName="boo" )
    result                      = pxy.foo( 1, 2, 3 )
    assert result == "1, 2, 3"
    del pxy

    svrthr.join()
    svr.close()
    svrctx.term()
    cli.close()
    clictx.term()

class stoppable( threading.Thread ):
    """
    Supports external thread-safe Thread stop signalling.
    """
    def __init__( self, *args, **kwargs ):
        super( stoppable, self ).__init__( *args, **kwargs )
        self.__stop	 	= threading.Event()

    def stop( self ):
        self.__stop.set()

    def stopped( self ):
        return self.__stop.is_set()

    def join( self, *args, **kwargs ):
        self.stop()
        super( stoppable, self ).join( *args, **kwargs )


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


class boo( object ):
    def foo( *args ):
        return ", ".join( str( a ) for a in args )

def test_simplest():
    global port
    handler			= zmqjsonrpc.ServiceHandler( service=boo() )
    result			= handler.handleRequest(
        '{"params":[1,2,3],"jsonrpc":"2.0","method":"boo.foo","id":"1"}' )
    assert result == "1, 2, 3"

    port 		       += 1
    clictx                      = zmq.Context()
    cli                         = clictx.socket( zmq.REQ )
    cli.connect( "tcp://localhost:%d" % ( port ))
    svrctx                      = zmq.Context()
    svr                         = svrctx.socket( zmq.REP )
    svr.setsockopt( zmq.RCVTIMEO, 250 )
    svr.bind( "tcp://*:%d" % ( port ))

    cli.send_multipart( [ "hi" ] )
    assert svr.recv_multipart()  == [ "hi" ]
    svr.send_multipart( [ "lo" ] )
    assert cli.recv_multipart() == [ "lo" ]

    svrthr                      = zmqwrapper( socket=svr, handler=handler )
    svrthr.daemon		= True
    svrthr.start()

    pxy                         = zmqjsonrpc.ServiceProxy(
                                      socket=cli,
                                      serviceName="boo" )
    result                      = pxy.foo( 1, 2, 3 )
    assert result == "1, 2, 3"
    del pxy

    svrthr.join()
