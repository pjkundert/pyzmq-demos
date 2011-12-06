
# 
# 0MQ interface for JSON-RPC
# 
# Based on the reference Python JSON-PRC implementation from http://json-rpc.org, and
# on the 
# 

import zmq
import zhelpers
import jsonrpc
import json

def RequestId( initial=1 ):
    """
    JSON-RPC request 'id' enumerator generator.  By default, returns a
    monotonically increasing integer value.  May be shared by many ServiceProxy
    objects, but is not thread-safe.
    """
    while True:
        yield initial
        initial		       += 1
        

class session_socket( object ):
    def __init__( self, socket, sessid, prefix=None ):
        self.__socket 		= socket
        assert sessid is None or type( sessid ) is list
        self.__sessid		= sessid or []
        assert prefix is None or type( prefix ) is list
        self.__prefix		= prefix or []

    def send_multipart( self, data ):
        """
        Sends data list with the supplied routing prefix (if any), and
        prepends the session id before the supplied data.
        """
        prefix			= self.__prefix
        assert type( data ) is list
        payload			= self.__sessid + data
        print "Sending (to %s): %s" % (
            ", ".join( [ zhelpers.format_part( lbl )
                         for lbl in prefix ] ),
            ", ".join( [ zhelpers.format_part( msg )
                         for msg in payload ] ))
        self.__socket.send_multipart( payload, prefix=prefix )

    def recv_multipart( self ):
        """
        Removes any leading session id from the result; fails if no session id is present.
        """
        result			= self.__socket.recv_multipart()
        if type( result ) is tuple:
            prefix, payload 	= result
        else:
            prefix		= []
            payload		= result
        print "Receive (fr %s): %s" % (
            ", ".join( [ zhelpers.format_part( lbl )
                         for lbl in prefix ] ),
            ", ".join( [ zhelpers.format_part( msg)
                         for msg in payload ] ))
        if self.__sessid:
            if result[:len(self.__sessid)] == self.__sessid:
                result		= result[len(self.__sessid):]
            else:
                raise Exception( "Expected session ID: %s" % (
                        ", ".join( self.__sessid )))
        return result

class ServiceProxy( jsonrpc.ServiceProxy ):
    """
    Creates a persistent connection using a 0MQ socket, and allows remote
    invocation of methods via JSON-RPC conventions.  The 0MQ socket provided is
    returned in many ServiceProxy objects, so ensure that they are not shared
    between threads, as 0MQ socket objects are not thread-safe.
    """
    def __init__(self, socket, name=None, reqid=None):
        self.__socket		= socket
        self.__name		= name
        self.__reqid		= reqid if reqid is not None else RequestId()

    def __getattr__( self, name ):
        if self.__name != None:
            name = "%s.%s" % (self.__name, name)
        return ServiceProxy( self.__socket, name=name, reqid=self.__reqid )

    def __call__( self, *args ):
        """
        Invoke the method via JSON-RPC, via the 0MQ socket, and return
        the result if successful.
        """
        postdict		={
	       'jsonrpc':	"2.0",
               'method': 	self.__name, 
               'params': 	args,
               'id':		str( self.__reqid.next() ),
               }
        print "RPC Request: ", repr( postdict )
        postdata = json.dumps( postdict )
        self.__socket.send_multipart(
            ['content-type: application/json', postdata] )
        respdata 		= self.__socket.recv_multipart()
        resp = json.loads( respdata[0] )
        if resp['error'] != None:
            raise JSONRPCException(resp['error'])

        return resp['result']
         

        
