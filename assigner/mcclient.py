#!/usr/bin/env python
# A client for mcbroker.py
# 
# 
# Obtains a session key, sends some commands, and releases the session.

import zmq
import mcbroker
import zhelpers

context         = zmq.Context()

# 1) Obtain a session key and URL from the Broker, w/ an empty
# request.  This command may block for an indeterminate time, awaiting
# an available Server.  Once we have a Session, we are responsible for
# freeing it.  We obtain the session using the 'broker' socket, but we
# release it using the 'session' socket (the Broker might not be
# listening on the 'broker' socket, if no free Servers are available,
# leading to a deadlock).  
broker          = context.socket( zmq.REQ )
try:
    broker.connect( mcbroker.BRO_URL )
    broker.send_multipart( [''] )
    seskey, sesurl = broker.recv_multipart()
    print "%s Cli: Session key: %s, url: %s" % (
        mcbroker.timestamp(),
        zhelpers.format_part( seskey ), sesurl )

    # 2) Attach to the session URL, and send one or more commands.
    # These should be able to service the command within the known
    # maximum response time of a Server.  Ensure we always terminate
    # the session (with an empty session command) and close the
    # session socket.
    session             = context.socket( zmq.REQ )
    try:
        session.setsockopt( zmq.RCVTIMEO, 1250 )
        session.connect( sesurl )
        session.send_multipart( [seskey, 'Hello'] )
        response        = session.recv_multipart()
        print "%s Cli: Response 1: %s" % (
            mcbroker.timestamp(), repr( response ))
        
        session.send_multipart( [seskey, 'Hello'] )
        response        = session.recv_multipart()
        print "%s Cli: Response 2: %s" % (
            mcbroker.timestamp(), repr( response ))
    finally:
        # 3) Release the session key, by sending a 'session' request with
        # no request data (just the session key).
        print "%s Cli: Ending session %s..." % (
            mcbroker.timestamp(), zhelpers.format_part( seskey ))
        session.send_multipart( [seskey] )
        ack             = session.recv_multipart()
        print "%s Cli: Session ended: %s" % (
            mcbroker.timestamp(),
            ", ".join( [zhelpers.format_part( a ) for a in ack ]))
        session.close()
finally:    
    broker.close()
    context.term()



