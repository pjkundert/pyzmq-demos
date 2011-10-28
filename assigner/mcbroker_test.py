import threading
import zmq
import zhelpers
import mcbroker

broker_urls		= [ mcbroker.BRO_URL ]

def test_client_simple():
    """
    This is a test of a simple client-broker session allocation
    """
    context		= zmq.Context()
    broker_threads	= mcbroker.demo_broker_servers( context, 10 )

    # Connect to all brokers
    broker		= context.socket( zmq.XREQ )
    broker.setsockopt( zmq.RCVTIMEO, 250 )
    for url in broker_urls:
        broker.connect( url )

    # Get a session key and url
    broker.send_multipart( [ '' ] )
    try:
        lbl, rpy 		= broker.recv_multipart()
        assert len( lbl ) == 0
        assert len( rpy ) == 2
    finally:
        broker.send_multipart( [ 'HALT' ] )

        for t in broker_threads:
            t.join()

        broker.close()
        context.term()
