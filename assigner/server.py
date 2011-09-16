
#
# Proxy server in Python
# Binds REP socket to tcp://localhost:5555
# Expects "Hello" from client, replies with "World"
#
import zmq
import time

context = zmq.Context()

# Socket to respond to client
print "Binding to hello world server port..."
responder = context.socket(zmq.REP)
responder.bind("tcp://*:5555")
while True:
    request = responder.recv()
    print "Received request: [%s]" % request

    # Do some 'work'
    time.sleep( 1 )

    responder.send("World")

