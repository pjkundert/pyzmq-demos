import zmq
import zmqjsonrpc
import zhelpers
import json
import threading

def test_zmqrpc_simulate_server():
    clictx                      = zmq.Context()
    cli                         = clictx.socket( zmq.REQ )
    cli.connect( 'tcp://localhost:11223' )

    svrctx                      = zmq.Context()
    svr                         = svrctx.socket( zmq.XREP )
    svr.bind( 'tcp://*:11223' )

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

