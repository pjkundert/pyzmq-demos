# 
# zmqjsonrpc.py
# 
#     An RPC framework using 0MQ for transport, following the JSON-RPC standards:
# 
#          http://jsonrpc.org/spec.html,
# 
#     Originally intended to be based on the reference Python JSON-PRC
# implementation from http://json-rpc.org, but instead provides some basic
# functionality for implementing RPC-like client proxies and servers.  Not close
# to plug-in compatible with json-rpc.
# 
#     The architectural difference with 0MQ is that it is not connection
# oriented in the traditional TCP/IP or even UDP/IP sense; with some
# configuration changes, 0 or more client 0MQ REQ sockets may be connected to 0
# or more server 0MQ REP sockets, with or without intervening 0MQ router/dealer
# brokers in between.  Furthermore, there is no notification when clients and/or
# servers appear or disappear, making some traditional RPC requirements
# (eg. failure on connection disappearance) impossible.  Basically, as the 0MQ
# designers say "RPC is a leaky abstraction".
# 
#     You may *believe* that an interface using a network connected RPC
# interface is "just like" your original in-process procedure call based
# implementation -- but it is *not*, in very serious ways, which you *must*
# handle, by design.  (eg. timing, failure modes, session <-> thread assignment,
# etc., etc.)
# 
#     So, instead of providing a transparent, proxy-based RPC interface built on
# 0MQ, we'll implement several useful features which you can use to implement
# robust and performant remote communications using 0MQ, which you may embed in
# a method-call based interface wrapper, if you wish.
# 
#     Some of the capabilities:
# 
# o Support multiple clients, connecting to multiple servers.
# 
#     If multiple addresses are configured, then each client will explicitly
#     connect its request port to all of them.  Each request will be received
#     and processed by an arbitrary server, in a round-robin fashion.  0MQ will
#     route the request's response back to the correct client.
# 
# o Detect server liveness sending (and responding to) pings on server socket
# 
#     The main server request socket may be used for RPC calls, pings and to
#     request session keys.  If a server ceases to respond to pings (or ceases
#     to send ping requests), then it is declared dead.  In additions, all
#     outstanding RPC requests using sessions allocated by server are also
#     abandoned.
# 
# o Supports return of arbitrarily large values as a stream of blocks.
# 
#     Instead of collecting the complete result, and attempting to marshall,
#     transport and unmarshall it, the API may serially collect blocks of data
#     and transport them, while the client either collects them up and returns
#     them, or processes them in a streaming manner if more appropriate.
# 
# o Exceptions in the server may be handled arbitrarily on the server side 
# 
#     May be logged, or details stored for later access, or simplified and
#     returned (exception call-stack detail discarded)
# 
# o Session oriented protocols
# 
#     A series of invocations may need to be processed in order by the same
#     remote server thread.  Supports the acquisition of a server and allocation
#     of a session key, and creation of a separate channel to that server for
#     processing of subsequent requests within that session.  This allows all
#     requests to hit the same server thread, and allows that server pool to
#     ensure that the same server thread remains allocated to processes all
#     requests for that session key, until the server is released to the pool.
# 
# o Broadcast requests
# 
#     It is sometimes necessary to collect a response from all servers; for
#     example, to collect statistics or update some server state globally across
#     all instances.  Means are provided to send requests to all available
#     servers, and collect responses from all of them, blocking 'til the last
#     (surviving) one responds.
#     
#   - Enumerate all available servers (from original server address list)
#   - Send requests to each
#   - Block on all servers, while sending/receiving pings to ensure liveness
# 
# 
# client( socket, name )
# 
#     Creates a proxy object for the named remote method (optional; default
# name="" invokes only "global" methods (methods directly in the 'root' dictionary
# supplied to the server).  Invoking:
# 
#          <proxy>.method( <params> )
# 
#     Sends a 0MQ message containing a JSON-RPC 2.0 encoded remote method
# invocation of:
#     
#          [name.]method( <params> )
# 
#     The result (or exception, raised as a JSONRPCException) is returned via
# JSON-RPC encoding.
# 
# 0MQ JSON-RPC Protocol
# 
#     The zmqjsonrpc's client object expects a zmq.REQ type socket, where
# .recv_multipart yields exactly the original list passed to .send_multipart:
# 
#         Client                           Server
#         zmq.REQ                          zmq.REP
#         -------                          ------
#         '{"jsonrpc":...}'           -
#                                      `-> '{"jsonrpc":...}'
#                                       -  '{"result":...}'
#         '{"result":...}'           <-'
# 
#     This simple 0MQ REQ/[X]REP socket pair can support simple, traditional 1:1
# RPC.  As expected, it is the simplest to set up and use.  It has certain
# limitations:
# 
#     o Since 0MQ doesn't report socket disconnections, it is not possible for
#       the client to detect that a server has died after sending it a request.
#       If requests have predeterminable time limits, a timeout on the Client
#       zmq.REQ socket may be appropriate to detect dead/hung servers.
# 
#     o Subsequent RPC invocations may be routed to *any* server(s) that the
#       Client zmq.REQ socket is connected to.
# 
# 
#     For other zmq socket types, 0MQ adds additional routing information, which
# must be handled:
# 
#         zmq.REQ                          zmq.XREP
#         -------                          ------
#         '{"jsonrpc":...}'           -
#                                      `-> <cli> '' '{"jsonrpc":...}'
#                                       -  <cli> '' '{"result":...}'
#         '{"result":...}'           <-'
# 
#     This supports an M:1 RPC scenario, where multiple clients make requests to
# a single server, and each request must supply the routing information to carry
# the corresponding reply back to the correct client.
# 
# 
# Multiple Independent Client Requests with Same Server
# 
#     There may be reason to add yet further data to the messages transmitted
# over the socket's stream; to assist in establishing independent client
# sessions (eg. if the destination socket is shared by multiple separate
# sessions, or sequential threads invoking RPCs, within the same client).  This
# could be accomblished by modifying the client to transmit a session key with
# each request, and modifying the server to remember and return the session key,
# and even to multiplex incoming requests to separate threads if desired:
# 
#         zmq.REQ                          zmq.XREP
#         -------                          ------
#         <s#1> '{"jsonrpc"...}'      -
#                                      `-> <cli> '' <s#1> '{"jsonrpc"...}'
#                                       -  <cli> '' <s#1> '{"result"...}'
#         <s#1> '{"result"...}'      <-'
# 
#     This would support an X*M:1 scenario, where X sessions (threads?), in each
# of M clients, can send RPC requests to 1 server, and the results are sent back
# to the correct client, and then to the correct session/thread within the
# client.  This also supports simple X*M:N RPC, if the client connects its
# zmq.REQ socket to multiple server zmq.XREP sockets; however.  Each separate
# RCP request would be routed to a random server, which is satisfactory for many
# applications.
# 
# 
# Sessions with Server Thread Assignment
# 
#     To support a more strict X*M:N scenario (multiple sessions per client,
# multiple clients, and multiple servers) with strict session and server-thread
# allocation, another layer is required.  The Client must request a server
# assignment using an M:N channel (eg. REQ/REP, where the client REQ binds, and
# all the server REPs connect, or where the client REQ connects to each of the
# multiple server REP socket bind addresses, or an intervening Router/Dealer
# distributes all incoming connected client requests to all available connected
# servers).  The client requests and obtains a monitor socket address and a
# server socket address:
# 
#         zmq.REQ                          zmq.XREP
#         -------                          ------
#         ''                          -
#                                      `-> <cli> '' ''
#                                       -  <cli> '' <mon> <svr>
#         <mon> <svr>                <-'
# 
# The client then creates a new zmq.REQ socket connected to the provided addresses
# (if one not already existing), and then uses it to perform 1:1 RPC.  The
# supplied session key is used to associate all client session requests with the
# same server session/thread.
#                                          
#         zmq.REQ (to <svr>)               zmq.XREP (<svr>)
#         -------                          ------
#         '{"jsonrpc"...}'            -
#                                      `-> <cli> '' '{"jsonrpc"...}'
#                                       -  <cli> '' '{"result"...}'
#         '{"result"...}'            <-'
# 
# The end of the session is denoted with an request to .release on the monitor
# socket:
#                                          
#         zmq.REQ (to <mon>)               zmq.XREP (<mon>)
#         -------                          ------
#         '{"jsonrpc"...}'            -
#                                      `-> <cli> '' '{"jsonrpc"...}'
#                                       -  <cli> '' '{"result"...}'
#         '{"result"...}'            <-'
# 
# allowing the server pool's monitor to return the session's resources (the
# assigned server thread) to the pool.
# 


import json
import threading
import traceback
import zhelpers
import zmq
import random

JSONRPC_VERSION                 = "2.0"

JSON_OPTIONS                    = {
    "indent":           None,           # Not pretty-printed
    "separators":       (',', ':'),     # Most compact representation
    "allow_nan":        True,           # Allow nan/inf to be encoded
    }

PARSE_ERROR			= -32700
INVALID_REQUEST			= -32600
METHOD_NOT_FOUND		= -32601
INVALID_PARAMS			= -32602
INTERNAL_ERROR			= -32603
SERVER_ERROR			= -32000

MESSAGES 			= {	
    PARSE_ERROR:	"Parse error",
    INVALID_REQUEST:	"Invalid Request",
    METHOD_NOT_FOUND:	"Method not found",
    INVALID_PARAMS:	"Invalid params",
    INTERNAL_ERROR:	"Internal error",
    SERVER_ERROR:	"Server error",
}

class Error( Exception ):
    """
    Errors transported from the remote side include an error code and message.
    Error.__dict__ is appropriate for directly encoding as the 'error': value in
    a JSON-RPC response:
    
        >>> zmqjsonrpc.Error(-32700).__dict__
        {'code': -32700, 'message': 'Parse error'}

    If desired, an additional optional property 'data' may be defined,
    containing additional details of the error.
    """
    def __init__( self, code, message=None, data=None ):
        self.code		= code
        if message is None:
            message		= MESSAGES.get( code, "Unknown error code" )
        self.message		= message
        if data:
            self.data		= data

    def __str__( self ):
        if hasattr( self, 'data' ) and self.data:
            return "%d: %s; %s" % ( self.code, self.message, self.data )
        return "%d: %s" % ( self.code, self.message )

# 
# 0MQ JSON-RPC
# 
# client		-- zmq.REQ basic blocking client
# server		-- zmq.XREP basic blocking server
# server_thread		-- a stoppable Thread implementing a server
# 
# client_session	-- allocates a session for duration of client
# server_session	-- provides sessions over a pool of servers
# server_session_thread	-- a stoppable Thread implement a server_session
# 

class client( object ):
    """
    Proxy object to initiate RPC invocations to any non-'_...' method; blocking,
    no timeouts.  Default _transport assumes that the provided 0MQ socket strips
    off any routing prefix before delivering payload.

    Not threadsafe; all invocations are assumed to be serialized.

    EXAMPLE

        # Create 0MQ transport
        context		= zmq.Context()
        socket		= context.socket( zmq.REQ )
        socket.connect( "<server-address>" )
        remote		= client( socket=socket, name="boo" )

        # Create callable to method "first" and invoke; then "second"
        result1		= remote.first( "some", "args" )
        result2		= remote.second( "yet", "others" )

        # Clean up; destroy proxy (closes socket), terminate context
        del remote
        context.term()

    Any instance attribute not found is assumed to be a remote method, and
    returns a callable which may be used (repeatedly, if desired) to invoke the
    remote method.

    No public methods (all methods invoked are assumed to be remote) Override
    non-public '_...' methods to alter basic default behaviour.
    """

    def __init__( self, socket, name="" ):
        """
        Make a proxy for the object "name" on the other end of "socket".
        """
        self.socket		= socket
        self.name		= name
        self._request_id	= 0

    def __del__( self ):
        self._cleanup()

    def _cleanup( self ):
        if self.socket:
            self.socket.close()
            self.socket		= None

    def _request( self ):
        """
        Always return a unique request ID for anything using this instance.
        """
        self._request_id        += 1
        return self._request_id

    def _transport( self, request ):
        """
        Marshall and send JSON request, blocking for result.  This is the
        transport underlying the callables produced.  Override this to implement
        different RPC transports or encodings.
        """
        self.socket.send_multipart( [ json.dumps( request ) ] )
        response		= self.socket.recv_multipart()
        assert len( response ) == 1
        return json.loads( response[0] )
                
    def __getattr__( self, method ):
        """
        Prepare to invoke name.method.  Returns callable bound to remote method,
        using our transport and sequence of unique request IDs.   
        """
        print "__getattr__: %s" % ( method )
        return self._remote(
            method='.'.join( [ n for n in [ self.name, method ] if n ] ),
            transport=self._transport, request=self._request )

    class _remote( object ):
        """
        Capture an RPC API method as a callable.  When invoked, arguments are
        encoded using JSON-RPC conventions; transports request and collects
        result, raises Error exception on any remote error.

        Generally meant to be invoked once, but could be bound to a local and
        invoked repeatedly; generates appropriate sequence of unique request ids
        for each invocation.
        """
        def __init__( self, method, transport, request ):
            self.method		= method
            self.transport	= transport
            self.request	= request

        def __call__( self, *args ):
            """
            Encodes the call into a JSON-RPC style request, transports it,
            ensuring result has matching request id.
            """
            request		= {
                'jsonrpc':	"2.0",
                'id':		self.request(),
                'method': 	self.method,
                'params':	args,
                } 
            reply		= self.transport( request )
            if reply['error']:
                raise self.exception( reply )
            assert reply['id'] == request['id']
            return reply['result']

        def exception( self, reply ):
            """
            Handles a JSON-RPC reply 'error': value, and returns the appropriate
            local Exception instance.
            """
            error		= reply['error']
            return Error( error['code'], error.get('message'), error.get('data'))


class server( object ):
    """
    Terminates RPC invocations to the subset of methods on the provided 'root'
    dictionary of objects (eg. pass globals(), if the caller and network is
    guaranteed to be secure!); the target object and methods are simply found by
    name, by searching through the objects found in 'root'.

    In the default implementation, all callable attributes in a target object
    returned by 'dir' and not beginning with '_' are considered public (see
    multiprocessing, managers.py).

    A target object may customize __dir__ to limit method access.

    Alternatively, you may override and customize 'find_object', 'all_methods',
    'public_methods' or 'filter_method' to enforce some other object/method
    access control methodology.
    """
    def __init__( self, root, socket=None, latency=None ):
        assert type( root ) is dict
        self.root		= root
        self.poller		= zmq.core.poll.Poller()
        self.socket		= socket
        if self.socket:
            self.register( self.socket )
        self.latency		= float( latency or 0.25 )
        self.cache		= {}

    def __del__( self ):
        self.cleanup()

    def cleanup( self ):
        """
        Clean up anything that may prevent tidy shutdown (eg. prevent the Thread
        from terminating, such as open 0MQ sockets or contexts).  Any socket
        passed to constructor is assumed to be owned by the server, and is
        closed; use 'register' to add externally managed sockets.  Ensure safe
        and repeatable behaviour, because it may be invoked multiple times.
        """
        self.poller = None
        if self.socket:
            self.socket.close()
            self.socket = None

    def register( self, socket ):
        """
        Add another socket that we can receive incoming RPC requests on.  These
        sockets' scopes are assumed to be manage externally.
        """
        self.poller.register( socket, zmq.POLLIN )

    def filter_method( self, name ):
        """
        Detect unacceptable (non-public) methods; starting with '_'
        """
        return name[0] == '_'

    def public_methods( self, obj ):
        """
        Return a list of names of public methods of `obj`.
        (from multiprocessing, managers.py)
        """
        return [ name for name in self.all_methods( obj )
                 if not self.filter_method( name ) ]

    def all_methods( self, obj ):
        """
        Return a list of names of methods of `obj` (from multiprocessing,
        managers.py).  Uses dir() 
        """
        temp 			= []
        for name in dir( obj ):
            func 		= getattr( obj, name )
            if hasattr( func, '__call__' ):
                temp.append( name )
        return temp

    def find_object( self, names, root ):
        """
        Find the object identified by the list 'names', starting with the given root
        {'name': object} dict, and searching down through the dir() of each named
        sub-object.  Returns the final target object.
        """
        assert type( root ) is dict
        attrs			= root.keys()
        obj			= None
        print "finding '%s' in [%s]" % ( '.'.join( names ), ', '.join( attrs ))
        for name in names:
            if not obj:
                # No object yet; use root dictionary provided
                obj		= root.get( name )
            else:
                if name in dir( obj ):
                    obj		= getattr( obj, name )
            if not obj:
                break
        return obj

    def resolve( self, target, root ):
        """
        Validate and locate method targets in the form (name.)*method, returning
        the named bound method, or None if no method found.

        The first time a "name.method" is encountered, all available methods in
        "name.*" are found and cached.  This also results in reference(s) to the
        object being created, incrementing its refcount and preventing it from
        disappearing.

        We handle finding bound methods on objects, or simple functions defined
        in the supplied root namespace, and avoiding re-searching the object
        heirarchy every time an invalid method is requested.
        """
        method			= self.cache.get( target )
        if not method:
            # All names before the last one identify a chain of objects
            terms		= target.split('.')
            path		= terms[:-1]
            pathstr		= '.'.join( path )
            name		= terms[-1]
            if path:
                # A path; Must be a bound method of an object
                if pathstr not in self.cache:
                    # ...and we haven't previously searched this object's methods.
                    self.cache[pathstr] = None
                    obj		= self.find_object( path, root )
                    if obj:
                        for m in self.public_methods( obj ):
                            self.cache[pathstr + '.' + m] = getattr( obj, m )
                        method	= self.cache.get( target )
            else:
                # No path; Must be a simple function in the root scope
                if not self.filter_method( name ):
                    method	= root.get( name )
            if not method:
                # Still not found; memoize as invalid method
                self.cache[target] = None
        print "server.resolve: %s ==> %s" % (
            target, repr( method ) if method else "None, in " + repr( root.keys() ))
        return method

    def error( self, exception, request ):
        """
        Handle arbitrary server-side method exception 'exc', produced by the
        encoded request 'req', returning an appropriate Error exception.

        Override if further exception information must be retained and/or logged
        (eg. for subsequent calls to interrogate the exception details)
        """
        if isinstance( exception, Error ):
            return exception
        return Error( SERVER_ERROR, data="%s ==> %s" % (
                request['method'], str( exception )))

    def process( self, socket ):
        """
        Simulate a simple JSON-RPC style server on the given socket.  The
        JSON-RPC spec allows a list of 1 or more requests to be transported,
        which maps nicely onto the 0MQ multipart messages.  Assumes that 0MQ
        routing prefix (eg. from zmq.XREP) will be prepended to a list of
        JSON-RPC requests.

        Must trap all Exceptions, and return an appropriate reply message.
        """
        replies			= []
        received		= socket.recv_multipart()
        separator		= received.index('')
        prefix			= received[:separator+1]
        print "Server rx: [%s]" % (
            ", ".join( [ zhelpers.format_part( m )
                         for m in received ] ))
        for request in received[separator+1:]:
            rpy			= {
                'id':		None,
                'jsonrpc':	JSONRPC_VERSION,
                'result':	None,
                'error':	None,
                }
            try:
                # Attempt to un-marshall the JSON-RPC
                try:
                    req 		= json.loads( request )
                    assert req['jsonrpc'] == JSONRPC_VERSION
                    rpy['id']		= req['id']
                except Exception, e:
                    raise Error( INVALID_REQUEST,
                                 data="Bad JSON-RPC: " + str(e))

                if type(req['params']) is not list:
                    raise Error( INVALID_PARAMS, data="Must be a list" )
                
                # Valid JSON-RPC data.  Attempt to resolve method
                method		= self.resolve( req['method'], self.root )
                if not method:
                    raise Error( METHOD_NOT_FOUND,
                                 data="No method named '%s'" % ( req['method'] ))
                
                # Attempt to dispatch method; *arbitrary* exceptions!
                rpy['result']		= method( *req['params'] )
            except Exception, e:
                # The Error Exception is designed so it's dict contains JSON-RPC
                # appropriate attributes!  All other rpy items are correct (eg.
                # 'result' will retail None, 'til method is dispatched without
                # exception!
                rpy['error']		= self.error( e, req ).__dict__

            if rpy['id']:
                # Not a JSON-RPC Notification; append response for this request
                replies.append( json.dumps( rpy, **JSON_OPTIONS ))

        print "Server tx: [%s]" % (
            ", ".join( [ zhelpers.format_part( m )
                         for m in prefix + replies ] ))
        socket.send_multipart( replies, prefix=prefix )

    def receive( self ):
        """
        Receives and processes incoming requests until the predefined timeout
        has passed with no activity.
        """
        for socket, _ in self.poller.poll( timeout=self.latency ):
            self.process( socket )
        

class stoppable( threading.Thread ):
    """
    Supports external thread-safe Thread stop signalling.
    """
    def __init__( self, *args, **kwargs ):
        self.__stop	 	= threading.Event()
        super( stoppable, self ).__init__( *args, **kwargs )
        print "%s.__init__" % ( self )

    def stop( self ):
        self.__stop.set()
        print "%s.stop" % ( self )

    def stopped( self ):
        return self.__stop.is_set()

    def join( self, *args, **kwargs ):
        self.stop()
        super( stoppable, self ).join( *args, **kwargs )


class server_thread_base( stoppable ):
    """
    If combined with a server-derived class, this method will process server's
    incoming RPC requests 'til stopped.
    """
    def __init__( self, **kwargs ):
        stoppable.__init__( self, **kwargs )

    def run( self ):
        print "%s.run w/ latency=%s" % ( self, self.latency )
        while not self.stopped():
            self.receive()
        print "%s.run complete" % ( self )
        self.cleanup()


class server_thread( server, server_thread_base ):
    """
    A JSON-RPC server Thread, which receives requests, and stops (within the
    server's predefined 'latency') when joined.
    """
    def __init__( self, root, socket=None, latency=None, **kwargs ):
        server.__init__( self, root=root, socket=socket, latency=latency )
        server_thread_base.__init__( self, **kwargs )


class client_session( client ):
    """
    Session allocating client.  At creation, one (of a possible pool of) remote
    server_session is queried, and a session-specific socket are created, which
    will be used for all RPC calls executed during the life of this object.  All
    requests are serviced by a single thread on one of the remote servers.

    Three sockets are used to establish, maintain and process RPCs.  First, we
    request a session to be allocated, using the session_pool socket:

        1) request a session from 1:M REQ:REP pool of server_session

    We obtain a server socket that is connected to a remote server.  This is
    used to verify the liveliness of that server(running in
    a single thread), before returning our first 'client._remote' proxy
    instance, and deallocate that remote server it at time of destruction.


        2) receive address to establish server socket (for heartbeats,
           management, server admin RPC), and session socket (for session RPC)

        3) Obtain a XREQ socket connected to the server (if not already
           connected), and proceed with heartbeats, admin RPC.

        4) Obtain session XREQ socket (if not already connected), and proceed
           with session RPC.
           
        
    """
    # All client object's session pool monitors, keyed by monitor address.
    _pools			= {}

    def __init__( self, socket, name="", context=None ):
        """
        This master socket is used only to request and establish the session's
        server socket details.  This 1:M request may be received by 1 of many
        server_sessions, who will in turn allocate a server for the exclusive
        use of the client_session, 'til it is released, and return the details
        required to establish a connection to it.

        Since we cannot guarantee being able to ever reach the same peer using
        the session socket, it is used exactly once; all further management of
        the server and session is performed using the server or session
        socket(s).
        """

        # Allocate a server session for this client, blocking 'til available.
        # This is a one-shot request; client is discarded immediately after use,
        # and socket is never used again.
        monitor, session	= client( socket=socket, name="self" ).allocate()

        # Open the socket to the given server session allocated to this client.
        # Ensure object is fully initialized, so __getattr__ operates, before
        # using any object attributes!
        if context is None:
            context		= zmq.Context()
        sess_sock		= context.socket( zmq.REQ )
        sess_sock.connect( session )
        super( client_session, self ).__init__( socket=sess_sock, name=name )

        self._session		= session

        # Open a socket to the monitor, if none already exists.

        # TODO: clients sharing the same context share a common set of monitors.
        moni			= self._pools.get( monitor )
        if not moni:
            moni_sock	        = context.socket( zmq.REQ )
            moni_sock.connect( monitor )
            self._pools[monitor]= client( socket=moni_sock, name="self" )

        self._monitor		= monitor

        # TODO create a ping thread to ping the monitor, to track the health of
        # this session's server.

    def __del__( self ):
        """
        Ensure that the remote session pool is allowed to recycle the session.
        """
        moni			= self._pools.get( self._monitor )
        if moni:
            moni.release( self._session )



def logcall( method ):
    def wrapper( *args, **kwargs ):
        print "calling %s( %s )" % ( method.__name__, ", ".join(
                [ str( a ) for a in args ]
                + [ "%s=%s" % ( k, str( v )) for k, v in kwargs.items() ] ))
        return method( *args, **kwargs )
    return wrapper

# 
# server_session_monitor
# server_session_monitor_thread
# 
#     Remote validation of health of a server_session pool.
# 
# __init__:
#     pool      -- the server_session pool to monitor
# 
# ping		-- RPC API; returns None if OK, exception on pool failure
# 
class server_session_monitor( server ):
    """
    This RPC server responds to ping requests, monitoring a server_session's
    pool, to reassure the client_session that this server_session's session pool
    is still active.  Cessation of pings will result in the harvesting and
    reassignment of all sessions belonging to that client; ceasing to respond to
    pings will result in the client_session aborting all sessions with this
    server_session.
    """
    def __init__( self, pool, socket=None, latency=None ):
        """
        Remember the pool in an inaccessible member variable, and allow RPC
        access only to local 'self'.
        """
        self.pool		= pool
        super( server_session_monitor, self ).__init__(
            root={ 'self': self }, socket=socket, latency=latency )

    def __dir__( self ):
        return [ 'ping', 'release' ]

    def ping( self ):
        print "ping: %s" % repr( self._pool )

    def release( self, session ):
        """
        Explicit release of a session from a client.  Pass along to the pool.
        """
        self.pool.release( session )

class server_session_monitor_thread( server_session_monitor, server_thread_base ):
    def __init__( self, pool, socket=None, latency=None, **kwargs ):
        server_session_monitor.__init__(
            self, pool=pool, socket=socket, latency=latency )
        server_thread_base.__init__( self, **kwargs )

# 
# server_session
# server_session_thread
# 
# __init__
# 

class server_session( server ):
    """
    Wait for session allocate() requests, assign a server, and return the tuple
    containing: 
      - the direct socket address to monitor this server_session
      - the direct socket address to access the session's server
    """
    def __init__( self, root, context=None, socket=None, latency=None,
                  pool=5, iface="localhost", port=None ):
        super( server_session, self ).__init__( 
            root={ 'self': self }, socket=socket, latency=latency )
        
        # The _monitor socket (bound on iface:port) is used for direct
        # connection.  An integer port number is required, and all sockets will
        # begin at that port number.
        self._context		= context or zmq.Context()
        self._monitor_addr	= "tcp://%s:%d" % ( iface, port )	
        socket			= self._context.socket( zmq.XREP )
        socket.bind( self._monitor_addr )
        self._monitor		= server_session_monitor_thread(
            pool=self, socket=socket, latency=latency )
        self._monitor.start()
        self._idle		= {}

        # Session server threads on subsequent ports, all with access to the
        # 'root' dict of objects.
        for n in xrange( 0, pool ):
            socket		= self._context.socket( zmq.XREP )
            addr		= "tcp://%s:%d" % ( iface, port + 1 + n )
            socket.bind( addr )
            self._idle[addr]	= server_thread( 
                root=root, socket=socket, latency=latency )
            self._idle[addr].start()
        socket			= None

    def threads( self ):
        return [ self._monitor ] + self._idle.values()

    def join( self, *args, **kwargs ):
        """
        Will also stop ourself, and all the threads we control
        """
        for t in self.threads():
            t.join()
        super( server_session, self ).join( *args, **kwargs )

    # The server_session itself only responds to RPC requests to allocate new
    # sessions.  The corresponding _release is issued (locally) by the session
    # server thread, to put itself back in the pool.
    def __dir__( self ):
        """
        Return only publicly accessible RPC methods via dir(...).
        """
        return [ "allocate", "release" ]

    @logcall
    def allocate( self ):
        """
        Return the 0MQ socket address of the server_session pool monitor, and
        the specific session server allocated.
        """
        return self._monitor_addr, random.choice( self._idle.keys() )

    @logcall
    def release( self, session ):
        print "Releasing session: %s" % ( session )
        pass

class server_session_thread( server_session, server_thread_base ):
    def __init__( self, root, socket=None, latency=None,
                  pool=5, iface=None, port=None,  **kwargs ):
        server_session.__init__(
            self, root=root, socket=socket, latency=latency,
            pool=pool, iface=iface, port=port)
        server_thread_base.__init__( self, **kwargs )
