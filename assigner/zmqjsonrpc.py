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
#     The problem with 0MQ is that it is not connection oriented in the
# traditional TCP/IP or even UDP/IP sense; with some configuration changes, 0 or
# more client 0MQ REQ sockets may be connected to 0 or more server 0MQ REP
# sockets, with our without intervening 0MQ router/dealer brokers in between.
# Furthermore, there is no notification when clients and/or servers appear or
# disappear, making some traditional RPC requirements (eg. failure on connection
# disappearance) possible.  Basically, as the 0MQ designers say "RPC is a leaky
# abstraction".  
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
#     route the request back to the correct client.
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
#     requests to hit the same server, and allows that server to ensure that the
#     same thread processes all requests with that session key.
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
# ServiceProxy( socket[, serviceName[, requestId )
# 
#     Creates a proxy for the named object (optional; default None
#     invokes global methods).  Invoking:
# 
#          <proxy>.method(<params>)
# 
#     Sends the 0MQ 2-part message: 'content-type: application/json',
# followed by a JSON-RPC 2.0 encoded remote method invocation of:
#     
#          [serviceName.]method(<params>)
# 
#     The result (or exception, raised as a JSONRPCException) is
# returned via JSON-RPC encoding.
# 
# 0MQ JSON-RPC Protocol
# 
#     The zmqjsonrpc's ServiceProxy expects a zmq.REQ/REP type socket, where
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
# This simple 0MQ REQ/REP socket pair can support simple, traditional 1:1 RPC.
# As expected, it is the simplest to set up and use.  It has certain limitations:
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
#     There may be reason to add yet further data to the messages transmitted
# over the socket's stream; to assist in establishing sessions (eg. if the
# destination socket is shared by multiple separate sessions within the same
# client):
# 
#         zmq.REQ                          zmq.XREP
#         -------                          ------
#         <s#1> '{"jsonrpc"...}'      -
#                                      `-> <cli> '' <s#1> '{"jsonrpc"...}'
#                                       -  <cli> '' <s#1> '{"result"...}'
#         <s#1> '{"result"...}'      <-'
# 
# Now, an X*M:1 scenario is supported, where X sessions (threads?), in each of M
# clients, can send RPC requests to 1 server, and the results are sent back to
# the correct client, and then to the correct session/thread within the client.
# This also supports simple X*M:N RPC, if the client connects its zmq.REQ socket
# to multiple server zmq.XREP sockets; however.  Each request would be routed to
# a random server, which is satisfactory for many applications.
# 
# 
#     To support a more strict X*M:N scenario (multiple sessions/client,
# multiple clients, and multiple servers) with strict session/server-thread
# allocation, another layer is required.  The Client must request a server,
# using a 1:N channel (eg. REQ/REP, where the client REQ binds, and all the
# server REPs connect, or where the client REQ connects to each of the multiple
# server REP socket bind addresses).  The client then obtains a session ID and
# socket address:
# 
#         zmq.REQ                          zmq.XREP
#         -------                          ------
#         ''                          -
#                                      `-> <cli> '' ''
#                                       -  <cli> '' <key> <addr>
#         <key> <addr>               <-'
# 
# The client then creates a new zmq.REQ socket connected to the provided address
# (if one not already existing), and then uses it to perform X*M:1 RPC.  The
# supplied session key is used to associate all client session requests with the
# same server session/thread.
# 
#         zmq.REQ                          zmq.XREP
#         -------                          ------
#         <key> '{"jsonrpc"...}'      -
#                                      `-> <cli> '' <key> '{"jsonrpc"...}'
#                                       -  <cli> '' <key> '{"result"...}'
#         <key> '{"result"...}'      <-'
# 
# The end of the session is denoted with an empty request:
# 
#         <key>                       -
#                                      `-> <cli> '' <key>
#                                       -  <cli> '' <key>
#         <key>                      <-'
# 
# allowing the server to return the session's resources (eg. thread) to the
# pool.
# 


import json
import threading
import traceback
import zhelpers
import zmq

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
    Errors transported from the remote side include an error code.  The __dict__
    is appropriate for directly encoding as the error: value in a JSON-RPC
    response:
    
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
        remote		= client( socket=socket, name="boo" )

        # Create callable to method "first" and invoke; then "second"
        result1		= remote.first( "some", "args" )
        result2		= remote.second( "yet", "others" )

        # Clean up; destroy proxy, then close sockets, terminate context
        del remote
        socket.close()
        context.term()

    Any instance attribute not found is assumed to be a remote method, and
    returns a callable which may be used (repeatedly, if desired) to invoke the
    remote method.

    No public methods (all methods invoked are assumed to be remote) Override
    non-public '_...' methods to alter basic default behaviour.
    """

    def __init__( self, socket, name ):
        """
        Make a proxy for the object "name" on the other end of "socket".
        """
        self.socket		= socket
        self.name		= name
        self._request_id	= 0

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
        return self._remote( method=self.name + '.' + method,
                             transport=self._transport, request=self._request )

    class _remote( object ):
        """
        Capture an RPC request as a callable, encoded using JSON-RPC
        conventions; transport and collect result, raises generic Exception on
        any remote error.

        Generally meant to be invoked once, but could be bound to a local and
        invoked repeatedly; generates appropriate sequence of unique request ids
        for each subsequent invocation.
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
            Handles a reply error, and returns the appropriate local Exception.
            """
            error		= reply['error']
            return Error( error['code'], error.get('message'), error.get('data'))


def find_object( names, root ):
    """
    Find the object identified by the list 'names', starting with the given root
    {'name': object} dict, and searching down through the dir() of each named
    sub-object.  Returns the final target object.
    """
    assert type( root ) is dict
    attrs			= root.keys()
    obj				= None
    # print "finding '%s' in [%s]" % ( '.'.join( names ), ', '.join( attrs ))
    for name in names:
        if not obj:
            # No object yet; use root dictionary provided
            obj			= root.get( name )
        else:
            if name in dir( obj ):
                obj		= getattr( obj, name )
        if not obj:
            break
    return obj

def all_methods(obj):
    """
    Return a list of names of methods of `obj` (from
    multiprocessing/managers.py)
    """
    temp = []
    for name in dir(obj):
        func = getattr(obj, name)
        if hasattr(func, '__call__'):
            temp.append(name)
    return temp

class server( object ):
    """
    Terminates RPC invocations to the subset of methods on the 'root' dictionary
    of objects (eg. pass globals(), if you wish, and caller is guaranteed to be
    secure!); the target object and methods are simply found by name.

    In the default implementation, all methods in a target object not beginning
    with '_' are considered public (see multiprocessing/managers.py's
    public_methods).  

    An object may customize __dir__ to limit method access, or you may override
    and customize 'validate' to enforce some other access methodology.
    """
    def __init__( self, root=None, socket=None, latency=None ):
        self.root		= root
        self.poller		= zmq.core.poll.Poller()
        if socket:
            self.register( socket )
        self.latency		= float( latency or 0.25 )

        self.cache		= {}

    def register( self, socket ):
        """
        Add another socket that we can receive incoming RPC requests on.
        """
        self.poller.register( socket, zmq.POLLIN )


    def public_methods( self, obj ):
        '''
        Return a list of names of methods of `obj` which do not start with '_'
        (from multiprocessing/managers.py)
        '''
        return [name for name in all_methods(obj) if name[0] != '_']

    def resolve( self, target, root ):
        """
        Validate and locate method targets in the form (name.)*method, returning
        the named bound method.

        The first time a "name.method" is encountered, and all available methods
        are found and cached.  This also results in reference(s) to the object
        being created, incrementing its refcount and preventing it from
        disappearing.
        """
        method			= self.cache.get( target )
        if not method:
            terms		= target.split('.')
            # Assume all names before the last identify a chain of objects
            path		= terms[:-1]
            name		= terms[-1]
            obj			= find_object( path, root )
            if obj:
                methods		= self.public_methods( obj )
                for m in methods:
                    self.cache['.'.join( path + [ m ])] = getattr( obj, m )
                method		= self.cache.get( target )

        return method

    def stopped( self ):
        """
        Use a superclass 'stopped' method if available, otherwise never stop.
        """
        try:
            super( server, self ).stopped()
        except:
            pass
        return False

    def run( self ):
        """
        If combined with a threading.Thread (eg. stoppable) in a derived class,
        this method will process incoming RPC requests 'til stopped.
        """
        while not self.stopped():
            for socket, _ in self.poller.poll( timeout=self.latency ):
                self.process( socket )

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
        print "Server rx. [%s]" % (
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

        print "Server tx. [%s]" % (
            ", ".join( [ zhelpers.format_part( m )
                         for m in prefix + replies ] ))
        socket.send_multipart( replies, prefix=prefix )
                

class stoppable( threading.Thread ):
    """
    Supports external thread-safe Thread stop signalling.
    """
    def __init__( self, *args, **kwargs ):
        self.__stop	 	= threading.Event()
        super( stoppable, self ).__init__( *args, **kwargs )

    def stop( self ):
        self.__stop.set()

    def stopped( self ):
        return self.__stop.is_set()

    def join( self, *args, **kwargs ):
        self.stop()
        super( stoppable, self ).join( *args, **kwargs )

class server_thread( server, stoppable ):
    def __init__( self, root=None, socket=None, latency=None, **kwargs ):
        server.__init__( self, root=root, socket=socket, latency=latency )
        stoppable.__init__( self, **kwargs )

# Obsolete

CONTENT_TYPE                    = "content-type: application/json"
IS_SERVICE_METHOD		= "_is_service_method"

def request_id( initial=1 ):
    """
    JSON-RPC request 'id' enumerator generator.  By default, returns a
    monotonically increasing integer value.  May be safely shared by
    the many ServiceProxy objects representing the methods being, but
    is not thread-safe.
    """
    while True:
        yield initial
        initial                += 1


class session_socket( object ):
    """
    Transparently supports session ID lists and/or 0MQ routing
    prefixes on transported 0MQ message lists, while presenting the
    basic 0MQ send_/recv_multipart interface.

    Encapsulates a zmq.XREQ or zmq.XREP socket endpoint,
    adding/stripping optional session ID and/or routing label prefix
    lists on each send_/recv_multipart invocation.
    """
    def __init__( self, socket, sessid, prefix=None ):
        self.__socket           = socket
        assert sessid is None or type( sessid ) is list
        self.__sessid           = sessid or []
        assert prefix is None or type( prefix ) is list
        self.__prefix           = prefix or []

    def send_multipart( self, data ):
        """
        Sends data list with the supplied routing prefix (if any), and
        prepends any session id before the supplied data.
        """
        prefix                  = self.__prefix
        assert type( data ) is list
        payload                 = self.__sessid + data
        print "Sending (to %s): %s" % (
            ", ".join( [ zhelpers.format_part( m )
                         for m in prefix ] ),
            ", ".join( [ zhelpers.format_part( m )
                         for m in payload ] ))
        self.__socket.send_multipart( payload, prefix=prefix )

    def recv_multipart( self ):
        """
        Removes any leading prefix or session id from the result; fails if no
        session id is present.  If a __prefix is specified, then we expect that
        prefix on messages received; the message must contain a '' separator.
        """
        receive                 = self.__socket.recv_multipart()
        if self.__prefix:
            sep			= receive.index( '' )
            prefix              = receive[:sep]
            payload             = receive[sep+1:]
        else:
            prefix		= []
            payload		= receive
        if prefix and prefix != self.__prefix:
            raise Exception( "Expected routing prefix: [%s], received [%s]" % (
                    ", ".join(  [ zhelpers.format_part( m )
                                  for m in self.__prefix ] ),
                    ", ".join(  [ zhelpers.format_part( m )
                                  for m in prefix ] )))
            
        print "Receive (fr %s): %s" % (
            ", ".join( [ zhelpers.format_part( m )
                         for m in prefix ] ),
            ", ".join( [ zhelpers.format_part( m )
                         for m in payload ] ))
        if self.__sessid:
            if payload[:len(self.__sessid)] == self.__sessid:
                payload          = payload[len(self.__sessid):]
            else:
                raise Exception( "Expected session ID: [%s], received [%s]" % (
                    ", ".join(  [ zhelpers.format_part( m )
                                  for m in self.__sessid ] ),
                    ", ".join(  [ zhelpers.format_part( m )
                                  for m in payload[:len(self.__sessid)]] )))
        return payload

# 
# The ServiceProxy (RPC Client), and ServiceHandler (RPC
# Server) interfaces, minimally modified to accept a 0MQ socket,
# instead of a (HTTP) URL to access the service endpoint.
# 

# 
# The 0MQ JSON-RPC Client interface:  
# 
class JSONRPCException( Exception ):
    """
    The Exception raised if an non-None 'error' is returned via the
    JSON-RPC response.
    """
    def __init__(self, rpcError):
        Exception.__init__(self)
        self.error = rpcError

class ServiceProxy( object ):
    """
    Implements remote invocation of methods via JSON-RPC conventions.
    The 0MQ socket provided is returned in many ServiceProxy objects,
    so ensure that they are not shared between threads, as 0MQ socket
    objects are not thread-safe.

    Expects the 0MQ socket to return only the response data (not any
    routing prefix information); encapsulate other 0MQ socket types
    (eg. zmq.XREP, which return (labels, messages) tuples from
    recv_multipart()), so they handle the routing prefix labels, etc.

    TODO
    
    Support timeouts and tx/rx of keepalive heartbeats, to ensure
    remote service remains alive on long-lived requests.  This
    requires handling of 0MQ timeouts on recv_multipart, and
    transmission of appropriate serialized keepalive requests,
    reception of keepalive responses.  The detection of no response
    within a certain period must trigger an appropriate Exception
    indicating failure of the service.

    No multithreading is required on this side, as the keepalive
    heartbeat may be transmitted (and detection of lack of response
    computed) on timeout of recv_multipart.  On the remote
    (ServiceHandler) end, however, an asynchronous thread may be
    required to service the keepalive heartbeat responses in a timely
    manner.
    """
    def __init__(self, socket, serviceName=None, reqId=None):
        self.__socket           = socket
        self.__name             = serviceName
        self.__reqid            = reqId if reqId is not None else request_id()

    def __getattr__( self, name ):
        if self.__name != None:
            name = "%s.%s" % (self.__name, name)
        return ServiceProxy( self.__socket, serviceName=name, reqId=self.__reqid )

    def __call__( self, *args ):
        """
        Invoke the method via JSON-RPC, via the 0MQ socket, and return
        the result if successful.
        """
        method                  = unicode( self.__name )
        reqid                   = unicode( self.__reqid.next() )
        data                    = json.dumps( {
                                      "jsonrpc":        JSONRPC_VERSION,
                                      "method":         method,
                                      "params":         args,
                                      "id":             reqid,
                                      }, **JSON_OPTIONS )
        self.__socket.send_multipart( [CONTENT_TYPE, data] )

        # Receive and decode JSON response data.  May raise ValueError,
        # etc. exception, if not valid JSON.  Will raise AttributeError
        # exceptions if response dict doesn't contain required 'error',
        # 'result', 'id' fields, and AssertionError if values are not as
        # expected.  See jsonrpc/proxy.py for reference implementation.
        # Handle restarts
        respdata                = self.__socket.recv_multipart()
        resp                    = json.loads( respdata[0] )
        assert resp['id'] == reqid, \
            "Invalid JSON-RPC id '%s'; expected '%s'"  % ( resp['id'], reqid )
        if resp['error'] != None:
            raise jsonrpc.JSONRPCException(resp['error'])
        return resp['result']
         


# 
# The 0MQ JSON-RPC Server interface
# 
def ServiceMethod( function ):
    """
    Decorator for functions/methods intended to be remotely accessible.
    """
    setattr( function, IS_SERVICE_METHOD, True )
    return function


def ServiceException( Exception ):
    pass

def ServiceRequestNotTranslatable( ServiceException ):
    pass

def BadServiceRequest( ServiceException ):
    pass

def ServiceMethodNotFound( ServiceException ):
    pass


class ServiceHandler( object ):
    def __init__( self, service ):
        self.__service		= service

    def trap_exception( self, exc ):
        """
        An exception has occured; trap, log, etc. any of the exception data here, if desired.
        """
        pass

    def handleRequest( self, data ):
        """
        
        """
        error			= None
        result			= None
        reqid			= None

        try:
            # Decode request.  May result in ValueError, KeyError,
            # AssertionError exceptions if request is invalid.
            try:
                request		= json.loads( data )
            except Exception, e:
                raise ServiceRequestNotTranslatable(
                    str(e) + "; JSON-RPC request invalid" )

            try:
                version		= request['jsonrpc']
                reqid		= request['id']
                method		= request['method']
                params		= request['params']
                assert type( params ) is list
            except Exception, e:
                raise BadServiceRequest(
                    str(e) + "; JSON-RPC request bad/missing parameter" )

            # Locate method.  May result in ServiceMethodNotFound exception.
            handle		= getattr( self.__service, method, None )
            if not handle or not hasattr( handle, IS_SERVICE_METHOD ):
                raise ServiceMethodNotFound(
                    "%s; JSON-RPC method not %s" % (
                        method, "allowed" if handle else "found" ))

            # Invoke method, producing result.  May result in
            # arbitrary exceptions.
            result		= method( *params )
        except Exception, e:
            # Exception encountered while unmarshalling parameters or invoking
            # target method.  Capture all exception detail, including the args
            # (if they can be encoded)
            exc_type		= "%s.%s" % ( type(e).__module, type(e).__name )
            exc_mesg		= traceback.format_exception_only( type(e), e )[-1].strip()
            error 		= {
                "name": 	exc_type,
                "message":	exc_mesg,
                }
            try:
                json.encode({'args': exc.args})
            except TypeErorr:
                pass
            else:
                error["args"]	= e.args


        # At this point, either result (and reqid) OR error is set; not both.
        # Attempt to encode and return JSON-RPC result; on failure, fall thru
        # and return error.
        if not error:
            try:
                return json.dumps( {
                        "id":		reqid,
                        "result":	result,
                        "error": 	None,
                        } )
            except Exception, e:
                error		= {
                    "name":	"JSONEncodeException",
                    "message":	str(e) + "; JSON-RPC result not serializable"
                    }

        # No result, or attempt to encode it must have failed; encode
        # and return JSON-RPC encoded error response.
        return json.dumps( {
                "id":		reqid,
                "result":	None,
                "error": 	error,
                })
            
        

            
