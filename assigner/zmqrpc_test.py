import zmq
import zmqrpc
import json
import threading

def test_zmqrpc_simulate_server():
    clictx		= zmq.Context()
    cli			= clictx.socket( zmq.XREQ )
    cli.connect( 'tcp://localhost:11223' )

    svrctx		= zmq.Context()
    svr			= svrctx.socket( zmq.XREP )
    svr.bind( 'tcp://*:11223' )

    # Simulate the Server side of the RPC request
    def svrfun():
        prefix, request	= svr.recv_multipart()
        assert len( prefix ) == 1
        assert len( request ) == 3
        assert request[0].lower()	== '1' # Session ID
        sessid	 	= request[0]
        assert request[1].lower()	== 'content-type: application/json';
        requestdict		= json.loads( request[2] )
        print "Request: ", repr( requestdict )
        replydict 	= {
            'jsonrpc':	"2.0",
            'index':	"1",
            'result':	", ".join( [ str( p ) for p in requestdict['params']] ),
            'id':	"1",
            'error':	None,
            }
        svr.send_multipart( [sessid] + [json.dumps( replydict )],
                            prefix=prefix )
    svrthr		= threading.Thread( target=svrfun )
    svrthr.start()

    pxy			= zmqrpc.ServiceProxy(
        socket=zmqrpc.session_socket( cli, sessid=["1"] ), name="boo" )
    result 		= pxy.foo( 1, 2, 3 )


    assert result == "1, 2, 3"

    svrthr.join()
    del pxy
    svr.close()
    svrctx.term()
    cli.close()
    clictx.term()

