#
#   Request-reply service in Python
#   Connects REP socket to tcp://localhost:5560
#   Expects "Hello" from client, replies with "World"
#
import zmq
import zhelpers
import time
import threading

FRO_BND			= "tcp://*:5559"
FRO_URL			= "tcp://localhost:5559"

BCK_BND			= "tcp://*:5560"
SVR_URL 		= "tcp://localhost:5560"

CONTEXT			= zmq.Context()

def broker_routine( context, front, back ):

    frontend 		= context.socket( zmq.XREP )
    frontend.setsockopt( zmq.IDENTITY, front )
    frontend.bind( front )
    print "Broker Front HWM: %d, ID: %r" % (
        frontend.getsockopt(zmq.HWM), frontend.getsockopt(zmq.IDENTITY ))
    
    backend = context.socket( zmq.XREQ )
    backend.setsockopt( zmq.IDENTITY, back )
    backend.bind( back )
    print "Broker Back  HWM: %d, ID: %r" % (
        backend.getsockopt(zmq.HWM), backend.getsockopt(zmq.IDENTITY ))
    
    # Initialize poll set
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)
    
    # Switch messages between sockets
    while True:
        socks = dict(poller.poll())
    
        if socks.get(frontend) == zmq.POLLIN:
            message = frontend.recv()
            more = frontend.getsockopt(zmq.RCVMORE)
            if more:
                print "Rtr<<",
                zhelpers.dump_part(message)
                backend.send(message, zmq.SNDMORE)
            else:
                print "Rtr<.",
                zhelpers.dump_part(message)
                backend.send(message)

                if message == "HALT":
                    # TODO: Shut down cleanly; wait for completion of
                    # all ongoing prototcol sessions
                    break
    
        if socks.get(backend) == zmq.POLLIN:
            message = backend.recv()
            more = backend.getsockopt(zmq.RCVMORE)
            if more:
                print "Rtr>>",
                zhelpers.dump_part(message)
                frontend.send(message, zmq.SNDMORE)
            else:
                print "Rtr>.",
                zhelpers.dump_part(message)
                frontend.send(message)

    print "Rtr: Halting."
    frontend.close()
    backend.close()


def server_routine( context, url ):

    # Each server thread socket must have a unique identity within the
    # server (doesn't need to be globally unique)
    tid			= threading.current_thread().ident
    socket 		= context.socket( zmq.XREP )
    socket.setsockopt( zmq.IDENTITY, str( tid ))
    sockid 		= socket.getsockopt( zmq.IDENTITY )
    socket.connect( url )

    print "RR Server HWM: %d, ID: %s" % (
        socket.getsockopt(zmq.HWM), zhelpers.format_part( sockid ))
    
    while True:
        message 	= socket.recv()
    
        more = socket.getsockopt(zmq.RCVMORE)
        if more:
            print "Svr>>",
            zhelpers.dump_part(message)
            socket.send(message, zmq.SNDMORE)
        else:
            print "Svr>.",
            zhelpers.dump_part(message)
            # End of multi-part message
            print "Server %s received request: [%s]" % (
                zhelpers.format_part( sockid ), message )
    
            time.sleep( 1 )
            socket.send("World")
    
def main():
    threads 		= []
    t 			= threading.Thread( target=broker_routine,
                                           args=( CONTEXT, FRO_BND, BCK_BND, ))
    t.start()
    threads.append( t )
    for i in range(5):
        t 		= threading.Thread( target=server_routine,
                                            args=( CONTEXT, SVR_URL, ))
        t.start()
        threads.append( t )

    stop 		= CONTEXT.socket( zmq.XREQ )
    stop.connect( FRO_URL )
    stop.send( "START" )
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
