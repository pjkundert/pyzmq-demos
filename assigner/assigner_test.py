import zmq
import zhelpers

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


def test_sending_receiver_ids():
    """
    When multiple sockets are connected and to the same destination, are the
    "receiving" socket IDs the same for all sending sockets?  Say we have:

        XREQ  <--\
                  +--> XREP (B)
    (A) XREP  <--/

    if we send via an XREQ to an XREP, and we use it to send us back the ID, can
    we use this same ID to route *outwards* via the (A) XREP, *to* the same
    destinatino XREP (B)?  Or, is a (random) ID for XREP (B) synthesiAed by each
    XREQ/XREP that connects to is (unless, of course, we sent an zmq.IDENTITY
    for XREP (B), which is obsolete in 0MQ 3+...)
    """
    ctx			= zmq.Context()

    global port
    port	       += 1

    xrepb		= ctx.socket( zmq.XREP )
    xrepb.bind( "tcp://*:%d" % port )

    xreqa		= ctx.socket( zmq.XREQ )
    xreqa.connect( "tcp://localhost:%d" % port )

    xrepa		= ctx.socket( zmq.XREP )
    xrepa.connect( "tcp://localhost:%d" % port )

    # From XREQ, XREP(B) will add a source socket ID on recv...
    xreqa.send_multipart( [ '', 'something' ] )
    rx			= xrepb.recv_multipart()
    print "msg: %s" % ", ".join( [ zhelpers.format_part( msg )
                                   for msg in rx ] )
    assert len( rx ) == 3

    # Try using that socket ID as a destination for routing to the same XREP(B)
    # from XREP(A)
    poller = zmq.Poller()
    poller.register( xrepb, zmq.POLLIN)
    xrepa.send_multipart( rx )
    socks = dict(poller.poll( timeout=1000 ))
    assert len( socks ) == 1
    rx2			= xrepb.recv_multipart()


