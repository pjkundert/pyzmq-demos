#
#   Request-reply service in Python
#   Connects REP socket to tcp://localhost:5560
#   Expects "Hello" from client, replies with "World"
#
import zmq
import zhelpers
import time
import threading

CLI_URL 		= "tcp://localhost:5560"
CONTEXT			= zmq.Context()

def server_routine( context, url ):
    socket = context.socket(zmq.XREP)
    socket.connect( url )
    sockid = socket.getsockopt( zmq.IDENTITY )
    print type( sockid ), repr( sockid )
    print "RR Server HWM: %d, ID: %s" % (
        socket.getsockopt(zmq.HWM), zhelpers.format_part( sockid ))
    
    while True:
        message = socket.recv()
    
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
    threads = []
    for i in range(5):
        t = threading.Thread(target=server_routine, args=(CONTEXT, CLI_URL, ))
        t.start()
        threads.append( t )

    for t in threads:
        t.join()

    # Never reached?  Not unless we define a termination condition for 
    CONTEXT.term()

if __name__ == "__main__":
    main()
