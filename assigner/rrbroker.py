# Simple request-reply broker
#
# Author: Lev Givon <lev(at)columbia(dot)edu>

import zmq
import zhelpers

# Prepare our context and sockets
context = zmq.Context()
frontend = context.socket(zmq.XREP)
backend = context.socket(zmq.XREQ)
frontend.bind("tcp://*:5559")
print "Broker Front HWM: %d, ID: %r" % (
    frontend.getsockopt(zmq.HWM), frontend.getsockopt(zmq.IDENTITY ))

backend.bind("tcp://*:5560")
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
