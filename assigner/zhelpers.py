# encoding: utf-8
"""
Helper module for example applications. Mimics ZeroMQ Guide's zhelpers.h.
"""

from random import randint

import zmq


# Receives all message parts from socket, prints neatly
def dump(zsocket):
    print "----------------------------------------"
    for part in zsocket.recv_multipart():
        dump_part(part)

def dump_part(part):
    print "[%03d]" % len(part), format_part( part )

def format_part(part):
    if all(31 < ord(c) < 128 for c in part):
        return part
    else:
        return "".join("%x" % ord(c) for c in part)

# Set simple random printable identity on socket
def set_id(zsocket):
    identity = "%04x-%04x" % (randint(0, 0x10000), randint(0, 0x10000))
    zsocket.setsockopt(zmq.IDENTITY, identity)
