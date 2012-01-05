import zmq
import zhelpers

port			= 55667

def test_closing_full():
    ctx			= zmq.Context()
    ver			= zmq.zmq_version()[:3]

    global port
    port	       += 1
    out 		= ctx.socket( zmq.XREQ )
    out.bind( "tcp://*:%d" % port )

    inc			= ctx.socket( zmq.XREP )
    inc.connect( "tcp://localhost:%d" % port )

    try:
        # Test that ..._multipart and zmq.RCVMORE behave as we think
        out.send_multipart( ['', 'TEST1'] )
        msg			= inc.recv_multipart()
        if ver in ("2.1", "3.1"):
            assert len( msg ) == 3    
        elif ver in ("3.0"):
            assert isinstance( msg, tuple )
            assert len( msg[0] ) == 1
            assert len( msg[1] ) == 2
        else:
            assert False and "Unknown zmq version"
        
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
        
    finally:
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
    destination XREP (B)?  Or, is a (random) ID for XREP (B) syntheised by each
    XREQ/XREP that connects to is (unless, of course, we sent an zmq.IDENTITY
    for XREP (B), which is obsolete in 0MQ 3+...)

    Note that, under 0MQ 3+, the REQ now adds a request ID as a "LABEL" entry,
    instead of a blank message entry.  Strictly speaking, this "maintains
    backward compatibility" with 0MQ 2 code -- that doesn't depend on there
    being a blank message entry, and maintains the value of the request ID label
    entry.
    """
    ctx			= zmq.Context()
    ver			= zmq.zmq_version()[:3]

    global port
    port	       += 1

    xrepb		= ctx.socket( zmq.XREP )
    xrepb.bind( "tcp://*:%d" % port )

    xreqa		= ctx.socket( zmq.XREQ )
    xreqa.connect( "tcp://localhost:%d" % port )

    xrepa		= ctx.socket( zmq.XREP )
    xrepa.connect( "tcp://localhost:%d" % port )

    try:
        # From XREQ, XREP(B) will add a source socket ID on recv...
        xreqa.send_multipart( [ '', 'something' ] )
        if ver in ("2.1", "3.1"):
            rx			= xrepb.recv_multipart()
            print "msg: %s" % ", ".join( [ zhelpers.format_part( msg )
                                           for msg in rx ] )
            assert len( rx ) == 3
        elif ver in ("3.0"):
            labels, rx		= xrepb.recv_multipart()
            print "msg: %s" % ", ".join( [ zhelpers.format_part( msg )
                                           for msg in labels + rx ] )
            assert len( labels ) == 1
            assert len( rx ) == 2
        else:
            assert False and "Unknown zmq version"
        
        # Try using that socket ID as a destination for routing to the same XREP(B)
        # from XREP(A)
        poller = zmq.Poller()
        poller.register( xrepb, zmq.POLLIN)
        xrepa.send_multipart( rx )
        socks = dict(poller.poll( timeout=1000 ))
        # Nope.  Different IDs assigned by each connector, for the
        # same remote socket
        assert len( socks ) == 0
        if socks:
            rx2			= xrepb.recv_multipart()

    finally:
        # Hmm.  On Mac, against pyzmq 2.1.9, if we don't close the
        # sockets, python crashes; some kind of mismanagement of
        # Python memory, probably:
        # https://github.com/zeromq/pyzmq/issues/137
        xrepb.close()
        xrepa.close()
        xreqa.close()
        ctx.term()

