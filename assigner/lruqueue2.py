"""

   Least-recently used (LRU) queue device
   Clients and workers are shown here in-process
 
   Author: Guillaume Aubert (gaubert) <guillaume(dot)aubert(at)gmail(dot)com>
  
"""

import threading
import time
import zmq

def format_part(part):
    if all(31 < ord(c) < 128 for c in part):
        return part
    else:
        return "".join("%x" % ord(c) for c in part)

ZMQ_VER = int( zmq.zmq_version()[0] )
assert 2 <= ZMQ_VER <= 3
print "ZMQ Version: %d" % ( ZMQ_VER )

NBR_CLIENTS = 10
NBR_WORKERS = 3

def worker_thread(worker_url, context, i):
    """ Worker using REQ socket to do LRU routing """
    
    socket = context.socket(zmq.REQ)
    identity = "Worker-%d" % (i)
    if ZMQ_VER == 2:
        socket.setsockopt(zmq.IDENTITY, identity) #set worker identity
    socket.connect(worker_url)
    
    # Tell the borker we are ready for work
    socket.send("READY")
    
    try:
        while True:
            message = socket.recv_multipart()
            print "%s: %s" % ( identity, repr( message ))
            empty = message.index( "" )
            assert 0 < empty < len( message )
            address, request = message[:empty], message[empty+1:]

            print("%s: %s\n" %(identity, request))
            socket.send_multipart( address + ["", "OK"] )
            
    except zmq.ZMQError, zerr:
        # context terminated so quit silently
        if zerr.strerror == 'Context was terminated':
            return
        else:
            raise zerr
    
        
def client_thread(client_url, context, i):
    """ Basic request-reply client using REQ socket """
    
    socket = context.socket(zmq.REQ)

    identity = "Client-%d" % (i)
    if ZMQ_VER == 2:
        socket.setsockopt(zmq.IDENTITY, identity) #Set client identity. Makes tracing easier

    socket.connect(client_url)

    #  Send request, get reply
    socket.send("HELLO")
    
    reply = socket.recv()
    
    print("%s: %s\n" % (identity, reply))
    
    return
    

def main():
    """ main method """

    # Was "inproc://workers"
    ifc_worker = "tcp://*:55443"
    url_worker = "tcp://localhost:55443"
    # Was "inproc://clients"
    ifc_client = "tcp://*:55444"
    url_client = "tcp://localhost:55444"
    client_nbr = NBR_CLIENTS
    
    # Prepare our context and sockets
    context = zmq.Context(1)
    frontend = context.socket(zmq.XREP)
    frontend.bind(ifc_client)
    backend = context.socket(zmq.XREP)
    backend.bind(ifc_worker)
    
    
    
    # create workers and clients threads
    for i in range(NBR_WORKERS):
        thread = threading.Thread(target=worker_thread, args=(url_worker, context, i, ))
        thread.start()
    
    for i in range(NBR_CLIENTS):
        thread_c = threading.Thread(target=client_thread, args=(url_client, context, i, ))
        thread_c.start()
    
    # Logic of LRU loop
    # - Poll backend always, frontend only if 1+ worker ready
    # - If worker replies, queue worker as ready and forward reply
    # to client if necessary
    # - If client requests, pop next worker and send request to it
    
    # Queue of available workers
    available_workers = 0
    workers_list      = []
    
    # init poller
    poller = zmq.Poller()
    
    # Always poll for worker activity on backend
    poller.register(backend, zmq.POLLIN)

    # This does not mean what you think it means...
    # # Poll front-end only if we have available workers
    # poller.register(frontend, zmq.POLLIN)
    
    while True:
        
        socks = dict(poller.poll())
    
        # Handle worker activity on backend.  They send us:
        #     ["READY"]
        #     [<clientaddr>, '', 'OK']  (in 0MQ 3+, <clientaddr> may be multiple items)
        if (backend in socks and socks[backend] == zmq.POLLIN):
            # Queue worker address for LRU routing

            if ZMQ_VER == 2:
                message = backend.recv_multipart()
                print repr(message)
                # Second frame is empty in 0MQ 2.1.
                assert message[1] == ""
                worker_addr, response = message[0:1], message[2:]
            else:
                # 0MQ 3 -- ( ['label', ...], ['message', ...] )
                worker_addr, response = backend.recv_multipart()

            print "Worker:" + "-" * 32
            for p in worker_addr + [""] + response:
                print "[%03d] %s" % ( len( p ), format_part( p ))
        
            # add worker back to the list of workers
            assert len( workers_list ) < NBR_WORKERS
            workers_list.append(worker_addr)
            if len( workers_list ) == 1:
                poller.register(frontend, zmq.POLLIN)
            
            # READY or else a client reply address + "" + response
            # If client reply, send rest back to frontend
            if response[0] != "READY":
                
                # Following frame is empty
                empty = response.index( "" )
                assert 0 < empty < len( response )
                client_addr, reply = response[:empty], response[empty+1:]

                if ZMQ_VER == 2:
                    frontend.send_multipart(client_addr + [""] + reply)
                else:
                    frontend.send_multipart( reply, prefix=client_addr )
                
                client_nbr -= 1
                if client_nbr == 0:
                    break  # Exit after N messages
    
        if (frontend in socks and socks[frontend] == zmq.POLLIN):
            # Now get next client request, route to LRU worker
            # Client request is [address][empty][request]
            if ZMQ_VER == 2:
                message = frontend.recv_multipart()
                assert message[1] == ""
                client_addr, request = message[0:1], message[2:]
            else:
                client_addr, request = frontend.recv_multipart()

            print "Client:" + "-" * 32
            for p in client_addr + [""] + request:
                print "[%03d] %s" % ( len( p ), format_part( p ))
        
            # Dequeue and drop the next worker address; element is a list
            worker_addr = workers_list.pop()
            if ZMQ_VER == 2:
                backend.send_multipart( worker_addr + [""] + client_addr + [""] + request )
            else:
                backend.send_multipart( client_addr + [""] + request, prefix=worker_addr )
            if len( workers_list ) == 0:
                # poll on frontend only if workers are available
                poller.unregister( frontend )
                    

    #out of infinite loop: do some housekeeping
    time.sleep (1)
    
    frontend.close()
    backend.close()
    context.term()
    

if __name__ == "__main__":
    main()

