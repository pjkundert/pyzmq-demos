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
#         |        |        |
#    cli--+        o        +--svr
#                   .  
#                    .
#                     (control)
#                     BRO_IFC
#                     Control messages, eg "HALT"
# 

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
    incoming requests.  Assumes that one pool of Clients deals with a single
    pool of Servers, via a single Broker.

    Since we cannot route "outgoing" requests via a single REQ/XREP connection
    to all routers (only the returning replies follow routing labels), we cannot
    connect one Client to several Routers, and have the least busy one take the
    next request!

    Well, we can -- but getting the subsequent request to go to the same Router
    is not possible.  We would need to use a separate socket to each router to
    transport the subsequent requests; we would use a socket shared by all
    routers to establish the request. 


    Therefore, we are limited to connecting all Clients to the same Router (on
    one network interface).  The Router could, however, allow connections from
    Servers on many hosts.  This is likely the most fruitful scaling "axis"
    anyway, as the backend Python/Oracle requests are much heavier than the
    frontend web HTTP request processing and page rendering.

    


    The Server signals its readiness for a new Client with an empty request
    (identified in 0MQ version 3+ by a server request sequence number <S#1>;
    previous versions use an empty '' route/request seperator):

        Client              Router                           Server
        ------              ------                           ------
                                                          -  ''
                            <svr> <S#1> ''             <-'

    The Router now takes the oldest incoming Client request from the waiting
    pool (if any).  Otherwise, the Router waits 'til a Client requests a new
    session with a request with an empty key.  The Router assigns a Server (if
    one is ready), sends the request, and then responds to the Client with a
    Server session key:

        ''    <req1>    -
                         `-> <cli> <C#1> '' <req1>
                             <svr> <S#1> <req1>         -
                                                         `-> <req1>
                                                          -  <rpy1>
                             <svr> <S#2> <rpy1>        <-'
                          -  <cli> <C#1> <key> <rpy1>
        <key> <rpy1>   <-'

    Subsequent Client requests (including empty '' "keepalive" requests) are passed
    through to the allocated Server:
        
        <key> <req2>    -
                         `-> <cli> <C#2> <key> <req2>
                             <svr> <S#2> <req2>         -
                                                         `-> <req2>
                                                          -  <rpy2>
                             <svr> <S#3> <rpy2>        <-' 
                          -  <cli> <C#2> <key> <rpy2>
        <key> <rpy2>   <-'

    The Client terminates the session by providing the session key with no
    request (note, this is different than the empty '' "keepalive" request):

        <key>           -
                         `-> <cli> <C#3> <key>
                          -  <cli> <C#3> ''
        ''             <-'

    For the duration of the session, the key is expected, followed by each Client
    request payload.  The request is routed

    A keepalive signal is expected from the Client every few seconds.  If missed,
    the client session is 

    Distributes a Client's work request to a fixedbackend Server thread, when it
    asks for one.  Will not take an incoming work request off the
    incoming xmq.ROUTER (XREP), until a server asks for one.  This
    ensures that the (low) incoming High Water Mark causes the
    upstream xmq.DEALER (XREQ) to distribute work to other brokers (if
    they are keeping their queue clearer).

    We assume that most work is quite quick (<= 1 second), with the
    occasional really long request (seconds to hours).

    Monitors the incoming frontend, which is limited to a *low* High
    Water Mark of (say) 2, to encourage new Hits to go elsewhere when
    all our threads are busy.  However, we'll wake up to check every
    once in a while; if we find something waiting, we'll spool up a
    new thread to service it.


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
