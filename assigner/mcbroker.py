#!/usr/bin/env python
# 
# Session-oriented Request-reply service in Python
#   
# Expects "Hello" from client, replies with "Hello" "World" from the same Server
# for the duration of a Session.  The following illustrates the simplest case of
# a single Broker shared by all Clients and Servers.  A Client may connect to
# multiple Brokers, each with its own pool of Servers; a Session will still be
# persistent between the Client and the same Server until the Session is
# terminated.
# 
# 
#              - Client connects separate 'session' sockets to each Broker
#              - REQ-XREP
#              (session)
#              .
#              .       (server): XREP-REP
#              .       . 
#    cli--+    .       .    +--srv
#         |    .       .    |
#    cli--+--> o--bkr--o <--+--svr
#         |\      | |      /|
#    cli--+ +> o--+ +--o <+ +--svr
#              .       .
#              .       .
#              .       (admin): PUB-SUB
#              .      
#              (broker)
#              - REQ-XREP
#              - Session requests
#              - Control messages, eg "HALT"
#              - Read only when Server(s) are available
#              - Client can connect its 'broker' socket to multiple Brokers
# 
# 
import zmq
import zhelpers
import time
import threading
import collections
import json

import time
import timeit
timer = timeit.default_timer


# The broker socket.  Used by a client to obtain a session key, and
# the address of the broker's session socket.  All brokers are
# listening for requests on their broker socket.  A client may connect
# to many broker sockets.
BRO_IFC                 = "tcp://*:5559"
BRO_URL                 = "tcp://localhost:5559"

# The session socket is used by the client to process all of the
# commands that are part of the session.  It is used to establish a
# connection from the client to a single broker.
SES_IFC                 = "tcp://*:5560"
SES_URL                 = "tcp://localhost:5560"

# The server socket is used to communicate with all available servers.
# When a request initially arrives, the socket is placed in the 'idle'
# queue.  When allocated to a client, it is placed in the 'sess' dict.
SVR_IFC                 = "tcp://*:5561"
SVR_URL                 = "tcp://localhost:5561"

# Administrative commands from the broker to all its servers
ADM_IFC                 = "tcp://*:5562"
ADM_URL                 = "tcp://localhost:5562"


def timestamp( now=None ):
    if now is None:
        now = timer()
    timestamp           = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime( now ))
    millisecs           = "%.3f" % ( now % 1.0, )
    return timestamp + millisecs[1:]
    
def log_request( format, route, request ):
    """
    Expects 2 %s formatting entries in the format string, to 
    log the route and request.  Also prepends a high-resolution 
    system time to the output.
    """
    print timestamp( timer() ) + ' ' + format % (
        ", ".join( [ zhelpers.format_part( m ) for m in route ] ),
        ", ".join( [ zhelpers.format_part( m ) for m in request ] ))


def mcbroker_routine( context, ses_url, bro_ifc, ses_ifc, svr_ifc, adm_ifc ):
    """
    Establishes a fixed session between a Client and a Server, for a sequence of
    incoming requests.  A pool of Clients deals with one or more pools of
    Servers, each pool behind a Broker.  When a Client requests a new session,
    this request gets passed to a broker (load balanced).  The Client must
    connect to all Brokers.

    When a Broker has a Server available, it takes the Client session request
    and passes back a key to the Client, along with an address to connect a
    direct REP/XREP socket to, to directly access the Broker (so all subsequent
    requests will reach the same Broker).  Multiple Clients may connect directly
    to this Broker via this address, to access any Servers in the pool behind
    this Broker.  Once connected, future requests on the socket using the key
    will be persistently routed to the assigned Server, 'til the session is
    terminated (or the Client ceases sending keepalive requests, indicating that
    it has died, and its Server session is terminated and the Server is returned
    to the pool).



    0) The Server signals its readiness for a new Client with an empty request
    '' on the Broker's 'server' socket.  The REQ message contains the Server's
    address <svr> and a request ID (identified in 0MQ version 3+ by a request
    sequence number <S#1> label; previous versions use an empty '' message).
    The Broker activates polling on the 'broker' socket when the 'idle' Server
    queue was empty (1 Server is now available):

        tx/rx at                tx/rx at                         tx/rx at
        Client                  Router                           Server
        ------                  ------                           ------
  t |                           (server)                      -  ''
    v                           <svr> <S#1> ''             <-'
                                # put Server in 'idle', start 'broker' polling

    1) The Broker waits 'til a Client requests a new Session on its 'broker'
    socket.  The Broker assigns a Server (disabling polling on 'broker' socket
    if 'idle' pool now empty) and responds to the Client with a Server session
    key (a random token created by the Broker), and a Broker session socket
    address for the Client to use for future commands using the Session:

                         broker
                         req/xrep
        ''                  -
                             `-> <cli> ''
                                 # get Server from 'idle', stop 'broker' poll if empty    
                              -  <cli> <key> <addr>
        <key> <addr>       <-'

        
    2) The Client now establishes a REQ/XREQ 'session' socket connection to the
    Broker's Session address (if not already connected), and sends a series of
    requests using the supplied Session key, which is carried right through to
    the Server:

                        session                          server
                        xreq/xrep                        xrep/xreq

        <C#1> <key> <req1>  -
                             `-> <cli> <C#1> <key> <req1..> 
                                 <svr> <S#1> <key> <req1..> -
                                                             `-> <S#1> <key> <req1>
                                                              -  <S#1> <key> <rpy1>
                                 <svr> <S#2> <key> <rpy1>  <-'
                              -  <cli> <C#1> <key> <rpy1>
        <C#1> <key> <rpy1> <-'

    Subsequent Client requests on the 'session's socket (including empty ''
    "keepalive" requests) are passed through to the allocated Server, using the
    Broker's request socket.  If the Broker Server doesn't hear from the Client
    within a timeout period, it assumes that the Client is dead, and abandons it
    (TODO: discard an outstanding Server response (if any), finish the Session
    and restore the Server to the idle pool):
        
        <C#2> <key> ''      -
                             `-> <cli> <C#2> <key> ''
                                 <svr> <S#2> <key> ''       -
                                                             `-> <key> ''
                                                              -  <key> ''
                                 <svr> <S#3> ''            <-' 
                              -  <cli> <C#2> <key> ''
        <C#2> <key> ''     <-'

    3) The Client terminates the session by providing the session key with no
    request (note, this is different than the empty '' "keepalive" request).
    This causes the Broker to return the Server to the 'idle' pool, after it
    responds, for the next Client session request (enabling polling on the
    'broker' socket if 'idle' was empty):


        <C#3> <key>         -
                             `-> <cli> <C#3> <key>
                                 <svr> <S#3> <key>          -
                                                             `-> <S#1> <key>
                                                              -  <S#1> <key>
                                 <svr> <S#4> <key>         <-'
                                 # return Server to 'idle', start 'broker' polling
                              -  <cli> <C#3> <key>
        <C#3> <key>        <-'





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
    broker              = context.socket( zmq.XREP )
    broker.bind( bro_ifc )

    # The session channel; each client connects directly, and only
    # issues requests here for sessions in play.
    session             = context.socket( zmq.XREP )
    session.bind( ses_ifc )

    server              = context.socket( zmq.XREP )
    server.bind( svr_ifc )

    admin               = context.socket( zmq.PUB )
    admin.bind( adm_ifc )
    
    # Available server (route labels); suppresses poll of broker when
    # empty
    idle                = []

    # Sessions (routes to Servers) in play; indexed by session key
    sess                = {}

    sess_routes         = collections.namedtuple(
        'Session', [
            'client',
            'server'
            ] )

    poller              = zmq.Poller()
    poller.register(session, zmq.POLLIN)
    poller.register(server, zmq.POLLIN)
    
    KEY                 = 1

    try:
        done            = False
        while not done:
            for sock, status in poller.poll():

                if sock == server and status == zmq.POLLIN:
                    # A Server reporting in or delivering reply to Client;
                    # Forward reply, saving route to forward a later incoming
                    # Client session command; this blocks the Server -- the
                    # response it gets will be the next Client request after
                    # this Server is assigned to a session.
                    msg		= server.recv_multipart()
                    print "%s Rtr: Received [%s] from Server" % (
                        timestamp(), ", ".join( [ zhelpers.format_part( m )
                                                  for m in msg ] ))
                    sep		= msg.index( '' )
                    svr		= msg[:sep]
                    request	= msg[sep+1:]

                    sesskey     = request[0]
                    sessobj     = sess.get( sesskey )
                    if sessobj and len( request ) > 1:
                        # <key> <rsp..>
                        # 
                        # Response to an oustanding Client request! Send it back
                        # to Client, update 'sess' entry with fresh server
                        # response ID.  The session key supplied *must* be the
                        # same as the Server's address, or there is something
                        # seriously wrong with the Server implementation
                        log_request( "Rtr: Server [%s] response: [%s]",
                                     svr, request )
                        if sesskey != svr[0]:
                            # Key returned by Server doesn't match Server ID!
                            # 
                            # TODO: Must tear down Client connection, throw away Server.
                            print "%s Rtr: INVALID Server response (bad key)" % (
                                timestamp())
                        cli     = sessobj.client
                        print "%s Rtr: Sending to [%s]" % ( timestamp(),
                            ", ".join( [ zhelpers.format_part( m )
                                         for m in cli ] ))
                        sess[sesskey] = sess_routes( client=None, server=svr )
                        session.send_multipart( request, prefix=cli+[''] )
                    else:
                        # Handle end session, new server; idle Server.
                        if sessobj:
                            # <key>
                            # 
                            # End of session response from Server; send along to
                            # Client.  Put back in idle pool and deallocate a
                            # session key.  Must use the fresh Server route
                            # (incl. latest response ID#)
                            del sess[sesskey]
                            print "%s Rtr: Server session %s is deleted:  %s" % (
                                timestamp(), repr( request ), repr( sess ))
                            session.send_multipart( request,
                                                    prefix=sessobj.client+[''] )

                        elif sesskey:
                            # <???>
                            # 
                            # Unrecognized key.  
                            # 
                            # TODO: Must tear down Client connection, throw away Server.
                            print "%s Rtr: INVALID Server response (not key)" % (
                                timestamp())
                        else:
                            # ''
                            #
                            # Must be a fresh server; drop its route into idle list
                            log_request( "Rtr: Server [%s] ready: [%s]",
                                         svr, request )

                        if not idle:
                            print "%s Rtr: Now has spare servers" % timestamp()
                            poller.register( broker, zmq.POLLIN )
                        idle.append( svr )

            
                elif sock == session and status == zmq.POLLIN:
                    # A Client request for one of the assigned session's Servers.
                    # Always contains a session key as its first value.  This
                    # will be forwarded to the Server, as the 'response' that
                    # the Server is blocked awaiting.
                    # 
                    # A request with only a session key (no other request data)
                    # indicates the end of the session; 'idle' Server
                    msg		= session.recv_multipart()
                    print "%s Rtr: Received [%s] from Session" % (
                        timestamp(), ", ".join( [ zhelpers.format_part( m )
                                                  for m in msg ] ))
                    sep		= msg.index( '' )
                    cli		= msg[:sep]
                    request	= msg[sep+1:]

                    sesskey     = request[0]
                    sessobj     = sess.get( sesskey )
                    if sessobj:
                        log_request( "Rtr: Session request from [%s]: [%s]",
                                     cli, request)
                    else:
                        log_request( "Rtr: Session UNKNOWN from [%s]: [%s]",
                                     cli, request)
                        session.send_multipart( ['ERROR: invalid session key'],
                                                prefix=cli+[''] )
                        continue

                    svr         = sessobj.server
                    if sessobj.client:
                        # If we detect multiple incoming client requests with
                        # the same key, before a server response is issued, the
                        # client is implemented incorrectly; log and ignore.
                        log_request(
                            "Rtr: Client tx from: [%s] before rx: ignoring [%s]",
                            cli, request )
                        continue
                    sess[sesskey]       = sess_routes( client=cli, server=svr )
                    server.send_multipart( request, prefix=svr+[''] )

                elif sock == broker and status == zmq.POLLIN:
                    # Requests for new sessions arrive on the 'broker' socket.
                    # Only polled when there are available Servers; 'idle' is
                    # not empty.
                    msg		= broker.recv_multipart()
                    print "%s Rtr: Received [%s] from Client" % (
                        timestamp(), ", ".join( [ zhelpers.format_part( m )
                                                  for m in msg ] ))
                    sep		= msg.index( '' )
                    cli		= msg[:sep]
                    request	= msg[sep+1:]

                    log_request( "Rtr: Broker request: from [%s]: [%s]",
                                 cli, request )
                    if not request[0]:
                        # '' 
                        # 
                        # Allocate a session key.  Can only occur when 'idle' is
                        # not empty (socket being polled.)
                        route   = idle.pop( 0 )
                        if not idle:
                            print "%s Rtr: No more spare servers" % timestamp()
                            poller.unregister( broker )
                        print "%s Rtr: Allocating new session from client: %r, to server: %r" % (
                            timestamp(), cli, route )
                        sesskey = route[0]   # use Server route ID as session key
                        broker.send_multipart( [sesskey, ses_url], prefix=cli+[''] )
                        sess[sesskey] = sess_routes( client=None, server=route )
                        print "%s Rtr: Sessions: %r" % ( timestamp(), sess )
                    elif request[0] == 'HALT':
                        done            = True
                        broker.send_multipart( [''], prefix=cli+[''] )
                    elif request[0] == 'REPORT':
                        admin.send_multipart( ['REPORT'] )
                        broker.send_multipart( [''], prefix=cli+[''] )
                    else:
                        # Not a recognized broker request!
                        print "%s Rtr: Unrecognized request!" % ( timestamp() )
                        broker.send_multipart( [''], prefix=cli+[''] )
                else:
                    print "%s Rtr: Unrecognized poll status: %s" % (
                        timestamp(), repr( status ))
    finally:
        print "%s Rtr: halting" % ( timestamp() )
        # Send a 'HALT' to all Servers.  Then, send an empty response to all the
        # Servers in the idle list or allocated to a session, to awaken them
        # (they are blocked, awaiting incoming commands on their work sockets)
        admin.send_multipart( ['HALT'] )
        for route in idle + [ s.server for s in sess.values() ]:
            server.send_multipart( [''], prefix=route+[''] )
        broker.close()
        session.close()
        server.close()
        admin.close()
    print "%s Rtr: exiting" % timestamp()

def server_routine( context, svr_url, adm_url, server ):
    """
    Server thread 'wrk' socket has a unique ID, which the router will need to
    know, in order to arrange for messages to be forwarded to it.  Send a
    message back up to the router, each time we are ready for another work unit.
    It will then forward this along, back to the client, with a copy of the
    complete address path back to this server thread.

    The 'adm' SUB socket is used to transmit administrative directions to the
    server (eg. 'HALT')


    The 'server' object is acted upon by any remote JSON-RPC requests.
    """
    tid                 = threading.current_thread().ident

    wrk                 = context.socket( zmq.REQ )
    wrk.connect( svr_url )

    adm                 = context.socket( zmq.SUB )
    adm.connect( adm_url )
    adm.setsockopt( zmq.SUBSCRIBE, '' )

    # A Server reports for duty with an empty request.  The next response will
    # be the first Client request; it will *always* contain a Session key and
    # this key must be used in the subsequent request (which actually carries
    # the reply to the Client.)
    # 
    # zmq.REQ automatically sends a '' separator prefix, so that the routing
    # information can be separated from the message.  The Broker also
    # (explicitly) does so using its zmq.XREP, but all the routing information
    # and the separator is stripped off by zmq.REQ, so we'll just receive the
    # message.
    print "%s Svr: %s: Ready" % ( timestamp(), tid )
    wrk.send_multipart( [''] )

    poller = zmq.Poller()
    poller.register(wrk, zmq.POLLIN)
    poller.register(adm, zmq.POLLIN)

    try:
        done            = False
        while not done:
            print "%s Svr: polling" % timestamp()
            for sock, status in poller.poll():

                if sock == wrk and status == zmq.POLLIN:
                    request   = wrk.recv_multipart()
                    # We have received a work unit; if it is empty, it's a ping.
                    print "%s Svr: Thread %s received request: [%s]" % (
                        timestamp(), tid,
                        ", ".join( [ zhelpers.format_part( m )
                                     for m in request ] ))
                    key = request[:1]
                    cmd = request[1:]
                    res = []
                    if not cmd:
                        # <key>
                        # End of Session; no command.  Just return key.
                        print "%s Svr: Thread %s end of session [%s]" % (
                            timestamp(), tid,
                            ", ".join( [ zhelpers.format_part( m )
                                         for m in key ] ))
                        pass
                    elif type( cmd[0] ) is str and cmd[0].lower().endswith("application/json"):
                        # <key> "content-type: application/json" "<JSON-RPC request>"
                        rpcid		= None
                        try:
                            rpc		= json.loads( cmd[1] )
                            assert type( rpc ) is dict, "JSON-RPC must be a dict"
                            rpcver	= rpc.get( 'jsonrpc', rpc.get( 'version', 0.0 ))
                            assert float( rpcver >= 1.0 )
                            rpcid	= rpc.get( 'id', None )

                            res 	= [
                                json.dumps(
                                    getattr( server, rpc.get( 'method' ))(
                                        *rpc.get( 'params' ))) ]

                        except Exception, e:
                            res = [ json.dumps( {
                                        'jsonrpc':	'2.0',
                                        'error':	{
                                            'code':	-1,
                                            'message':	str( e ),
                                            },
                                        'id':		rpcid,
                                        } ) ]
                    else:
                        # <key> <cmd...>
                        # Work.  Just add some extra work to the command.
                        print "%s Svr: Thread %s cmd on session [%s]: [%s]" % (
                            timestamp(), tid,
                            ", ".join( [ zhelpers.format_part( m )
                                         for m in key ] ),
                            ", ".join( [ zhelpers.format_part( m )
                                         for m in cmd ] ))

                        time.sleep( 1 )
                        res += cmd + [ 'World' ]
                    wrk.send_multipart( key + res )

                elif sock == adm and status == zmq.POLLIN:
                    print "%s Svr: receiving admin..." % timestamp()
                    multipart   = adm.recv_multipart()
                    print "%s Svr: %s received admin: [%s]" % (
                        timestamp(), tid, 
                        ", ".join( [ zhelpers.format_part( m )
                                     for m in multipart ] ))
                    if multipart[0] == 'HALT':
                        done = True
                    elif multipart[0] == 'REPORT':
                        print "%s Svr: reporting" % timestamp()
                else:
                    print "%s Svr: unrecognized event! %s" % (
                        timestamp(), repr( status ))
    finally:
        print "%s Svr: halting" % timestamp()
        adm.close()
        wrk.close()
    print "%s Svr: exiting" % timestamp()
    
def demo_broker_servers( context, servers=5 ):
    """
    Start a demo broker and a number of servers, returning the threads.
    """
    threads             = []
    t                   = threading.Thread( target=mcbroker_routine,
                                            args=( context, SES_URL,
                                                   BRO_IFC, SES_IFC,
                                                   SVR_IFC, ADM_IFC ))
    t.start()
    threads.append( t )

    class server_target( object ):
        def hello( self ):
            return "JSON World %s" % repr( self )

    for i in range( servers ):
        t               = threading.Thread( target=server_routine,
                                            args=( context,
                                                   SVR_URL, ADM_URL,
                                                   server_target() ))
        t.start()
        threads.append( t )

    return threads


if __name__ == "__main__":
    import signal
    """
    Start up a test standalone broker and some servers
    """
    context             = zmq.Context()
    threads             = demo_broker_servers( context )

    # Await interrupt
    broker              = context.socket( zmq.XREQ )
    broker.connect( BRO_URL )
    try:
        signal.pause()
    finally:
        broker.send_multipart( ['HALT'], prefix=[''] )
        broker.recv_multipart()
        for t in threads:
            t.join()

        broker.close()
        context.term()
