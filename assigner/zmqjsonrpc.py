# 
# zmqjsonrpc.py
# 
# A JSON-RPC Service Proxy interface using 0MQ for transport
# 
#     Based on the reference Python JSON-PRC implementation from
# http://json-rpc.org Presents an interface similar to (close to
# plug-in compatible with) json-rpc.
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
#     The zmqjsonrpc's ServiceProxy expects a zmq.REQ/REP type socket,
# where .recv_multipart yields exactly the original list passed to
# .send_multipart:
# 
#         Client                           Server
#         zmq.REQ                          zmq.REP
#         -------                          ------
#         '{"jsonrpc"...}'            -
#                                      `-> '{"jsonrpc"...}'
#                                       -  '{"result"...}'
#         '{"result"...}'            <-'
# 
# This simple 0MQ REQ/REP socket pair can support simple, traditional
# 1:1 RPC.  
# 
# 
#     For other zmq socket types, 0MQ adds additional routing
# information, which must be handled:
# 
#         zmq.REQ                          zmq.XREP
#         -------                          ------
#         '{"jsonrpc"...}'            -
#                                      `-> <cli> '' '{"jsonrpc"...}'
#                                       -  <cli> '' '{"result"...}'
#         '{"result"...}'            <-'
# 
# This supports an M:1 RPC scenario, where multiple clients make
# requests to a single server, and each request must supply the
# routing information to carry the corresponding reply back to the
# correct client.
# 
# 
#     There may be reason to add yet further data to the messages
# transmitted over the socket's stream; to assist in establishing
# sessions (eg. if the destination socket is shared by multiple
# separate sessions within the same client):
# 
#         zmq.REQ                          zmq.XREP
#         -------                          ------
#         <s#1> '{"jsonrpc"...}'      -
#                                      `-> <cli> '' <s#1> '{"jsonrpc"...}'
#                                       -  <cli> '' <s#1> '{"result"...}'
#         <s#1> '{"result"...}'      <-'
# 
# Now, an X*M:1 scenario is supported, where X sessions (threads?), in
# each of M clients, can send RPC requests to 1 server, and the
# results are sent back to the correct client, and then to the correct
# session/thread within the client.  This also supports simple X*M:N
# RPC, if the client connects its zmq.REQ socket to multiple server
# zmq.XREP sockets; however.  Each request would be routed to a random
# server, which is satisfactory for many applications.
# 
# 
#     To support a more strict X*M:N scenario (multiple
# sessions/client, multiple clients, and multiple servers) with strict
# session/server allocation, another layer is required.  The Client
# must request a server, using a 1:N channel (eg. REQ/REP, where the
# client REQ binds, and all the server REPs connect, or where the
# client REQ connects to each of the multiple server REP socket bind
# addresses).  The client then obtains a session ID and socket address:
# 
#         zmq.REQ                          zmq.XREP
#         -------                          ------
#         ''                          -
#                                      `-> <cli> '' ''
#                                       -  <cli> '' <key> <addr>
#         <key> <addr>               <-'
# 
# The client then creates a new zmq.REQ socket to the address (if not
# already existing), and then uses it to perform X*M:1 RPC
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
# allowing the server to return the session's resources (eg. thread)
# to the pool.
# 


import zmq
import zhelpers
import json


VERSION                         = "2.0"
CONTENT_TYPE                    = "content-type: application/json"
JSON_OPTIONS                    = {
    "indent":           None,           # Not pretty-printed
    "separators":       (',', ':'),     # Most compact representation
    "allow_nan":        True,           # Allow nan/inf to be encoded
    }
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
            ", ".join( [ zhelpers.format_part( lbl )
                         for lbl in prefix ] ),
            ", ".join( [ zhelpers.format_part( msg )
                         for msg in payload ] ))
        self.__socket.send_multipart( payload, prefix=prefix )

    def recv_multipart( self ):
        """
        Removes any leading session id from the result; fails if no
        session id is present.
        """
        result                  = self.__socket.recv_multipart()
        if type( result ) is tuple:
            prefix, payload     = result
        else:
            prefix              = []
            payload             = result
        print "Receive (fr %s): %s" % (
            ", ".join( [ zhelpers.format_part( lbl )
                         for lbl in prefix ] ),
            ", ".join( [ zhelpers.format_part( msg)
                         for msg in payload ] ))
        if self.__sessid:
            if result[:len(self.__sessid)] == self.__sessid:
                result          = result[len(self.__sessid):]
            else:
                raise Exception( "Expected session ID: %s" % (
                        ", ".join( self.__sessid )))
        return result

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
                                      "jsonrpc":        VERSION,
                                      "method":         method,
                                      "params":         args,
                                      "id":             reqid,
                                      }, **JSON_OPTIONS )
        self.__socket.send_multipart( [CONTENT_TYPE, data] )

        # Receive and decode JSON response data.  May raise
        # ValueError, etc. exception, if not valid JSON.  Will raise
        # AttributeError exceptions if response dict doesn't contain
        # required 'error', 'result', 'id' fields, and AssertionError
        # if values are not as expected.  See jsonrpc/proxy.py for
        # reference implementation.
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
                    e.message + "; JSON-RPC request invalid" )

            try:
                version		= request['jsonrpc']
                reqid		= request['id']
                method		= request['method']
                params		= request['params']
                assert type( params ) is list
            except Exception, e:
                raise BadServiceRequest(
                    e.message + "; JSON-RPC request bad/missing parameter" )

            # Locate method.  May result in ServiceMethodNotFound exception.
            handle		= getattr( self.service, method, None )
            if not handle or not hasattr( handle, IS_SERVICE_METHOD ):
                raise ServiceMethodNotFound(
                    "%s; JSON-RPC method not %s" % (
                        method, "allowed" if handle else "found" ))

            # Invoke method, producing result.  May result in
            # arbitrary exceptions.
            result		= method( *params )
        except Exception, e:
            error 		= {
                "name": 	e.__class__.__name__,
                "message":	e.message,
                }

        # At this point, either result (and reqid) OR error is set;
        # not both.  Attempt to encode and return JSON-RPC result; on
        # failure, fall thru and return error.
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
                    "message":	e.message + "; JSON-RPC result not serializable"
                    }

        # No result, or attempt to encode it must have failed; encode
        # and return JSON-RPC encoded error response.
        return json.dumps( {
                "id":		reqid,
                "result":	None,
                "error": 	error,
                })
            
        

            
