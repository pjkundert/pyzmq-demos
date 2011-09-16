import zmq

port			= 55667

def test_closing_full():
    ctx			= zmq.Context()

    global port
    port	       += 1
    out 		= ctx.socket( zmq.XREQ )
    out.bind( "tcp://*:%d" % port )

    inc			= ctx.socket( zmq.XREP )
    inc.connect( "tcp://localhost:%d" % port )

    # Test that ..._multipart and zmq.RCVMORE behave as we think
    out.send_multipart( ['', 'TEST1'] )
    msg			= inc.recv_multipart()
    assert len( msg ) == 3    

    out.send_multipart( ['', 'TEST2'] )
    msg 		= []
    poller		= zmq.Poller()
    poller.register(inc, zmq.POLLIN)
    while poller.poll( timeout=100 ):
        msg.append( inc.recv() )
        if not inc.getsockopt( zmq.RCVMORE ):
            break
    assert len( msg ) == 3

    ## Add another stage; see if we can intercept and re-route between
    ## inc/nxt
    #port	       += 1
    #nxt			= ctx.socket( zmq.XREQ )
    #nxt.connect( "tcp://localhost:%d" % port )
    #
    #lst			= ctx.socket( zme.XREP )
    #nxt.bind( "tcp://*:%d" % port )


    out.close()
    inc.close()
    ctx.term()

