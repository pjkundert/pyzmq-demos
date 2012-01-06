import zmq
import zmqjsonrpc
import json
import threading

def test_zmqrpc_simulate_server():
    clictx                      = zmq.Context()
    cli                         = clictx.socket( zmq.XREQ )
    cli.connect( 'tcp://localhost:11223' )

    svrctx                      = zmq.Context()
    svr                         = svrctx.socket( zmq.XREP )
    svr.bind( 'tcp://*:11223' )

    # Simulate the Server side of the 0MQ JSON-RPC request
    def svrfun():
        prefix, request         = svr.recv_multipart()
        assert len( prefix ) == 1
        assert len( request ) == 3
        assert request[0].lower() == "1"     # Session ID
        assert request[1].lower() == zmqjsonrpc.CONTENT_TYPE
        sessid                  = request[0]

        requestdict             = json.loads( request[2] )
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
        svr.send_multipart( [sessid] + [json.dumps( replydict,
                                                    **zmqjsonrpc.JSON_OPTIONS)],
                            prefix=prefix )

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
