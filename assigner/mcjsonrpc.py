
# 
# Implements JSON-RPC via mcbroker sessions
# 

import json

class NoOpRLock( object ):
    def acquire(self, *args, **kwargs):
        pass
    __enter__ = acquire
    def release(self):
        pass
    def __exit__(self, *args, **kwargs):
        self.release()

class client( object ):
    """
    Derive from mcjsonrpc.client to implement the client half of a
    JSON-RPC client/server pair.  Always creates/destroys its own
    session socket, to ensure no sharing between threads.
    """
    def __init__( self, context, broker, session=None, lock=NoOpRLock() ):
        """
        Connects to a list of Broker addresses, allocate a session via the
        broker, raising an exception if not possible.  The (unfortunately named)
        sesspool may be a 
        
        Threadsafe, if the mutual exclusion primitive (eg. threading.RLock()
        object is passed to every client sharing the same 'broker' object and
        'sesspool'; serializes access to the N:M 'broker' socket, and the shared
        pool of M:1 'session' sockets

        """
        self.lock		= lock
        if session is None:
            session		= {}
        self.session		= session

        with self.lock:
            broker.send_multipart( [''] )
            self.sesskey, self.sessaddr = broker.recv_multipart()
            if not self.sessaddr in self.session:
                self.session[self.sessaddr] = context.socket( zmq.REQ )
                self.session[self.sessaddr].connect( self.sessaddr )


    def invoke( self, method, *args, **kwargs ):
        """
        Arrange for your API entrypoints to class invoke with their method name
        and arguments.  Receives the and decodes the response, and returns
        results or raises an exception.

        JSON-RPC supports EITHER position OR keyword arguments -- not both.
        """
        request			= {
            "jsonrpc":	"2.0"
            "method":	method,
            "params": 	None
            }
        if args and kwargs:
            raise Exception( 
                "Both positional and keyword parameters not supported" )
        with self.lock:
            self.
        

if __name__ == "__main__":

    class helloclient( client ):
        def __init__( self, context, broker ):
            super( helloclient, self ).__init__( context, broker )

        def hello( self, *args, **kwargs ):
            return self.call( "hello", *args, **kwargs )

    class helloserver( server ):
        def __init__( self, context, work ):
            super( helloserver, self ).__init__( context, work )
            
        def hello( self, *args, **kwargs ):
            return "World"
