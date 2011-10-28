#
#   Request-reply service in Python
#   Connects REP socket to tcp://localhost:5560
#   Expects "Hello" from client, replies with "World"
# 
# 
# 
#              (waiting)
#              .       (ready)
#              .       .
#    cli--+    .       .    +--srv
#         |    .       .    |
#    cli--+--> o--bkr--o <--+--svr
#         |\       |        |
#    cli--+ +----> o        +--svr
#                   .  
#                    .
#                     (control)
#                     BRO_IFC
#                     - Session requests
#                     - Control messages, eg "HALT"
#                     - Read only when Server(s) are available
import zmq
import zhelpers
import time
import threading

# The broker socket.  Used by a client to obtain a session key, and
# the address of the broker's session socket.  All brokers are
# listening for requests on their broker socket.  A client may connect
# to many broker sockets.
BRO_IFC			= "tcp://*:5559"
BRO_URL			= "tcp://localhost:5559"

# The session socket is used by the client to process all of the
# commands that are part of the session.  It is used to establish a
# connection from the client to a single broker.
SES_IFC			= "tcp://*:5560"
SES_URL			= "tcp://localhost:5560"

# The server socket is used to communicate with all available servers.
# When a request initially arrives, the socket is placed in the 'idle'
# queue.  When allocated to a client, it is placed in the 'sess' dict.
SVR_IFC			= "tcp://*:5561"
SVR_URL			= "tcp://localhost:5561"

# Administrative commands from the broker to all its servers
ADM_IFC			= "tcp://*:5562"
ADM_URL			= "tcp://localhost:5562"

def mcbroker_routine( context, bro_ifc, ses_ifc, svr_ifc, adm_ifc ):
    """
    Establishes a fixed session between a Client and a Server, for a sequence of
    incoming requests.  A pool of Clients deals with one or more pools of
    Servers behind a Broker.  When a Client requests a new session, this request
    gets passed to a broker (load balanced).

    When the Broker has a Server available, it takes the Client session request
    and passes back a key to the Client, along with an address to connect a
    REP/XREP socket to (multiple Clients may connect to the Broker via this
    address, to access any Servers in the pool behind this Broker).  Once
    connected, future requests on the socket using the key will be persistently
    routed to the assigned Server, 'til the session is terminated (or the Client
    ceases sending keepalive requests).



    The Server signals its readiness for a new Client with an empty request ''
    on the Broker "ready" socket.  The REQ message contains the Server's address
    <svr> and request ID (identified in 0MQ version 3+ by a request sequence
    number <S#1> label; previous versions use an empty '' message) seperator):

        Client              Router                           Server
        ------              ------                           ------
                            (ready)                       -  ''
                            <svr> <S#1> ''             <-'

    The Broker now takes the oldest incoming Client request from the waiting
    pool (if any).  Otherwise, the Router activates polling on the "waiting"
    socket, and waits 'til a Client requests a new session.  The Router assigns
    a Server (if one is ready, and responds to the Client with a Server session
    key, and a Broker request address:

        ''              -    (waiting)
                         `-> <cli> <C#1> ''
                          -  <cli> <C#1> <key> <bkr>
        <key> <bkr>    <-'

        
    The Client now establishes a REQ/XREQ socket connection to the broker
    address (it not already connected), and sends a series of requests using the
    supplied key:

        <key> <req1>    -    (request)
                         `-> <cli> <C#1> <key> <req1>
                             <svr> <S#1> <req1>         -
                                                         `-> <req1>
                                                          -  <rpy1>
                             <svr> <S#2> <rpy1>        <-'
                          -  <cli> <C#1> <key> <rpy1>
        <key> <rpy1>   <-'

    Subsequent Client requests (including empty '' "keepalive" requests) are
    passed through to the allocated Server, using the Broker's request socket.
    If the Broker Server doesn't hear from the Client within a timeout period,
    it assumes that the Client is dead, and abandons it (TODO: :
        
        <key> ''        -    (request)
                         `-> <cli> <C#2> <key> ''
                             <svr> <S#2> <key> ''       -
                                                         `-> <key> ''
                                                          -  <key> ''
                             <svr> <S#3> ''            <-' 
                          -  <cli> <C#2> <key> ''
        <key> ''       <-'

    The Client terminates the session by providing the session key with no
    request (note, this is different than the empty '' "keepalive" request).
    This causes the Server to report on the Broker's ready socket, and is
    returned to the ready pool for the next Client session request:

        <key>           -    (request)
                         `-> <cli> <C#3> <key>
                             <svr> <S#1> ''             -
                                                         `-> <key>
                                                          -  ''
                             <svr> <S#2> ''            <-'
                          -  <cli> <C#3> ''
        ''             <-'

                             (ready)                      -  ''
                             <svr> <S#1> ''            <-'




    Will not take an incoming work request off the incoming xmq.ROUTER (XREP),
    until a server asks for one.  This ensures that the (low) incoming High
    Water Mark causes the upstream xmq.DEALER (XREQ) to distribute work to other
    brokers (if they are keeping their queue clearer).

    We assume that most work is quite quick (<= 1 second), with the occasional
    really long request (seconds to hours).

    Monitors the incoming frontend, which is limited to a *low* High Water Mark
    of (say) 2, to encourage new Hits to go elsewhere when all our threads are
    busy.  However, we'll wake up to check every once in a while; if we find
    something waiting, we'll spool up a new thread to service it.


    For another description of this use case, see:
        http://lists.zeromq.org/pipermail/zeromq-dev/2010-July/004463.html
    """
    # Our main broker request channel.  All Clients connect to all
    # brokers, and request are load-balanced.  We don't poll this
    # unless we have idle servers.
    broker 		= context.socket( zmq.XREP )
    broker.bind( bro_ifc )

    # The session channel; each client connects directly, and only
    # issues requests here for sessions in play.
    session		= context.socket( zmq.XREP )
    session.bind( ses_ifc )

    server		= context.socket( zmq.XREP )
    server.bind( svr_ifc )

    admin		= context.socket( zmq.PUB )
    admin.bind( adm_ifc )
    
    # Available server (route labels); suppresses poll of broker when
    # empty
    idle		= []
    # Sessions in play; indexed by session key
    sess		= {}

    poller = zmq.Poller()
    poller.register(broker, zmq.POLLIN)
    poller.register(session, zmq.POLLIN)
    poller.register(server, zmq.POLLIN)

    try:
        while True:
            socks 	= dict( poller.poll() )

            if socks.get( server ) == zmq.POLLIN:
                route, request = server.recv_multipart()
                print "Rtr: Server reports ready: [%s]" % (
                    ", ".join( [ zhelpers.format_part( msg ) 
                                 for msg in request ] ))
                if not idle:
                    poller.register( broker, zmq.POLLIN )
                idle.append( route )
        
            if socks.get( session ) == zmq.POLLIN:
                labels, request = session.recv_multipart()
                print repr( request )
                print "Rtr: Session request: [%s]" % (
                    ", ".join( [ zhelpers.format_part( msg ) 
                                 for msg in request ] ))
                
            if socks.get( broker ) == zmq.POLLIN:
                labels, request = broker.recv_multipart()
                print "Rtr: Broker request: [%s]" % (
                    ", ".join( [ zhelpers.format_part( msg ) 
                                 for msg in request ] ))
                if request[0] == 'HALT':
                    break
                elif request[0] == 'REPORT':
                    admin.send_multipart( ['REPORT'] )
    finally:
        print "Rtr: halting"
        admin.send_multipart( ['HALT'] )
        broker.close()
        session.close()
        server.close()
        admin.close()

def server_routine( context, svr_url, adm_url ):
    """
    Server thread 'wrk' socket has a unique ID, which the router will
    need to know, in order to arrange for messages to be forwarded to
    it.  Send a message back up to the router, each time we are ready
    for another work unit.  It will then forward this along, back to
    the client, with a copy of the complete address path back to this
    server thread.

    The 'adm' SUB socket  is used to transmit administrative directions
    to the server (eg. 'HALT')
    """
    tid			= threading.current_thread().ident

    wrk 		= context.socket( zmq.REQ )
    wrk.connect( svr_url )

    adm			= context.socket( zmq.SUB )
    adm.connect( adm_url )
    adm.setsockopt( zmq.SUBSCRIBE, '' )

    print "Server %s: Ready" % ( tid )
    wrk.send_multipart( ['READY'] )

    poller = zmq.Poller()
    poller.register(wrk, zmq.POLLIN)
    poller.register(adm, zmq.POLLIN)

    try:
        done		= False
        while not done:
            print "Server: polling"
            for sock, status in poller.poll():
                if sock == wrk and status == zmq.POLLIN:
                    print "Server: receiving command..."
                    multipart 	= wrk.recv_multipart()
                    print "Server %s received request: [%s]" % (
                        tid, ", ".join( [ zhelpers.format_part( msg )
                                          for msg in multipart ] ))
                    time.sleep( 1 )
                    wrk.send_multipart( multipart[:-1] + [ 'World' ] )

                elif sock == adm and status == zmq.POLLIN:
                    print "Server: receiving admin..."
                    multipart 	= adm.recv_multipart()
                    print "Server %s received admin: [%s]" % (
                        tid, ", ".join( [ zhelpers.format_part( msg )
                                          for msg in multipart ] ))
                    if multipart[0] == 'HALT':
                        done = True
                    elif multipart[0] == 'REPORT':
                        print "Server: reporting"

                else:
                    print "Server: unrecognized event! %s" % (
                        repr( status ))
    finally:
        print "Server: halting"
        adm.close()
        wrk.close()
    
def demo_broker_servers( context, servers=5 ):
    """
    Start a demo broker and servers, returning the threads.
    """
    threads 		= []
    t 			= threading.Thread( target=mcbroker_routine,
                                            args=( context,
                                                   BRO_IFC, SES_IFC,
                                                   SVR_IFC, ADM_IFC ))
    t.start()
    threads.append( t )

    for i in range( servers ):
        t 		= threading.Thread( target=server_routine,
                                            args=( context,
                                                   SVR_URL, ADM_URL ))
        t.start()
        threads.append( t )

    return threads


if __name__ == "__main__":
    """
    Start up a test standalone broker and some servers
    """
    context		= zmq.Context()
    threads 		= demo_broker_servers( context )

    # Await interrupt
    broker		= context.socket( zmq.XREQ )
    broker.connect( BRO_URL )
    try:
        time.sleep(100000)
    finally:
        broker.send_multipart( ['HALT'] )
        print 
        for t in threads:
            t.join()
        context.term()
