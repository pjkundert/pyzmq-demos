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

BRO_IFC			= "tcp://*:5559"
BRO_URL			= "tcp://localhost:5559"

BCK_IFC			= "tcp://*:5560"
SVR_URL 		= "tcp://localhost:5560"

WAI_IFC			= "tcp://*:5561"
WAI_URL			= "tcp://localhost:5561"

RDY_IFC			= "tcp://*:5562"
RDY_URL			= "tcp://localhost:5562"

ADM_URL			= ""

CONTEXT			= zmq.Context()

def broker_routine( context, _ifc, back_ifc, waiting_ifc, ready_ifc ):
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

    # Incoming transactions from an REQ/XREQ, for previously set-up
    # server threads.  These requests contain a stack of return path
    # routing added to the request, AND must contain destination
    # routing information (obtained by a previous 'waiting' request),
    # following by work:
    # 
    #     src ... '' dst work
    # 
    # We'll put the 'dst' first, when we sent it out the backend XREP,
    # so it gets routed to the correct server:
    # 
    #     dst src ... '' work
    #     ^^^
    # 
    # Then, when the server sends it back via its XREQ, our XREP will
    # add itself back on:
    #
    #     dst src ... '' response
    #     ^^^
    # 
    # We'll just discard that, and send the response on its way:
    # 
    #     src ... '' response
    #     
    # 
    frontend 		= context.socket( zmq.XREP )
    frontend.setsockopt( zmq.IDENTITY, front_ifc )
    frontend.bind( front_ifc )
    print "Broker Front HWM: %d, ID: %r" % (
        frontend.getsockopt( zmq.HWM ),
        zhelpers.format_part( frontend.getsockopt( zmq.IDENTITY )))
    
    # Outgoing transactions, flowing through to server threads.  These
    # must be addressed
    backend 		= context.socket( zmq.XREP )
    backend.bind( back_ifc )
    print "Broker Back  HWM: %d, ID: %r" % (
        backend.getsockopt(zmq.HWM),
        zhelpers.format_part( backend.getsockopt( zmq.IDENTITY )))
    
    # Idle server threads request new work on 'ready'.  When they ask,
    # we'll start polling for clients 'waiting' wwith new work, and
    # when some arrives, we'll send the 'ready' server's routing
    # packet back to the 'waiting' client.  We don't need to respond
    # back to the server; it'll be eagerly awaiting the first incoming
    # message from the client.
    waiting		= context.socket( zmq.XREP )
    waiting.bind( waiting_ifc )
    ready		= context.socket( zmq.XREP )
    ready.bind( ready_ifc )
    idle		= []		# emptiness suppresses poll of waiting
    
    # Incoming work requests, seeking new server threads
    # ...

    # Initialize poll set
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)
    poller.register(ready, zmq.POLLIN)
    
    # Switch messages between sockets

    while True:

        socks = dict(poller.poll())

        if socks.get( ready ) == zmq.POLLIN:
            multipart = ready.recv_multipart()
            print "Rtr: Server reports ready: [%s]" % (
                ", ".join( [ zhelpers.format_part( msg ) 
                             for msg in multipart ] ))
            if not idle:
                poller.register( waiting, zmq.POLLIN )
            idle.append( multipart[-1] )

        if socks.get( waiting ) == zmq.POLLIN:
            # A client wants work.  Send it to the first waiting
            # server, by responding with its routing information.
            multipart = waiting.recv_multipart()
            print "Rtr: Client waiting: [%s], have: [%s]" % (
                ", ".join( [ zhelpers.format_part( msg ) 
                             for msg in multipart ] ),
                ", ".join( idle ))

            waiting.send_multipart( multipart + [ idle.pop( 0 ) ] )
            if not idle:
                # No idle servers; stop 
                poller.unregister( waiting )

        if socks.get( frontend ) == zmq.POLLIN:
            multipart = frontend.recv_multipart()
            for msg in multipart:
                print "Rtr<<" + zhelpers.format_part( msg )
            if len( multipart):
                if  multipart[-1] == "HALT":
                    # TODO: Shut down cleanly; wait for completion of
                    # all ongoing prototcol sessions
                    break
            # Perform routing; prepend destination for XREP...
            mrk =   multipart.index('')
            ret =   multipart[:mrk+1 ]
            dst = [ multipart[ mrk+1 ] ]
            wrk =   multipart[ mrk+2:]
            svr = dst + ret + wrk
            print "Rtr: Server getting: [%s]" % (
                ", ".join( [ zhelpers.format_part( msg ) 
                             for msg in svr ] ))
            backend.send_multipart( svr )
                    
        if socks.get( backend ) == zmq.POLLIN:
            multipart = backend.recv_multipart()
            for msg in multipart:
                print "Rtr>>" + zhelpers.format_part( msg )
            # Discard routing; drop destination added by XREP...
            frontend.send_multipart(multipart[1:])

    print "Rtr: Halting."
    frontend.close()
    backend.close()


def server_routine( context, work_url, ready_url ):

    # Server thread 'wrk' socket has a unique ID, which the router
    # will need to know, in order to arrange for messages to be
    # forwarded to it.  Send a message back up to the router, each
    # time we are ready for another work unit.  It will then forward
    # this along, back to the client, with a copy of the complete
    # address path back to this server thread.
    tid			= threading.current_thread().ident

    # XREP 'backend' in broker uses a routing message when sending to our XREP,
    # allowing response message to route back via correct XREQ
    wrk 		= context.socket( zmq.XREP )
    # TODO: Don't set an identity; just find out our auto-assigned one...
    wrk.setsockopt( zmq.IDENTITY, str(tid))
    wrk.connect( work_url )

    # XRE 'ready' in broker adds a routing message when sending to
    # our XREP, to route response message back via correct XREQ
    rdy 		= context.socket( zmq.XREQ )
    rdy.connect( ready_url )

    print "RQ Server %d" % ( tid )
    
    while True:
        # Ready for more work!  Get some.  Will block 'til someone
        # wants some work done.   Send along our work socket identity
        # for routing the work to us.
        rdy.send_multipart( ['READY', wrk.getsockopt( zmq.IDENTITY ) ] )
        # No reply; just await work!
        multipart 	= wrk.recv_multipart()
        print "Server %s received request: [%s]" % (
            tid, ", ".join( [ zhelpers.format_part( msg )
                              for msg in multipart ] ))
        for msg in multipart:
            print "Svr>>" + zhelpers.format_part( msg )

        time.sleep( 1 )
        wrk.send_multipart( multipart[:-1] + [ 'World' ] )
    
def main():
    threads 		= []
    t 			= threading.Thread( target=broker_routine,
                                           args=( CONTEXT, BRO_IFC, BCK_IFC,
                                                  WAI_IFC, RDY_IFC ))
    t.start()
    threads.append( t )
    for i in range(5):
        t 		= threading.Thread( target=server_routine,
                                            args=( CONTEXT, SVR_URL, RDY_URL ))
        t.start()
        threads.append( t )

    stop 		= CONTEXT.socket( zmq.XREQ )
    stop.connect( BRO_URL )

    print stop.recv()
    try:
        time.sleep( 999999. )				# Await interrupt
    finally:
        print "Stopping server..."
        stop.send( "HALT" )

    for t in threads:
        t.join()
    stop.close()
    CONTEXT.term()


if __name__ == "__main__":
    main()
