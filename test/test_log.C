#include <cut>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "zmq.h"
#include "zmq_utils.h"

#if defined( TESTSTANDALONE )
#    include "test_main.C"
#endif // TESTSTANDALONE

namespace cut {
    static force force_log;
    CUT( root, log, "log" ) {
        void *sock[10];
        int socknum = 0;

        void *ctx = zmq_init (1);
        assert.ISTRUE (ctx);

        //  Create a subscriber.
        void *sub = zmq_socket (ctx, ZMQ_SUB);
        assert.ISTRUE (sub);
        sock[socknum++] = sub;
        int rc = zmq_connect (sub, "sys://log");
        assert.ISEQUAL (rc, 0);
    
        //  Subscribe for all messages.
        rc = zmq_setsockopt (sub, ZMQ_SUBSCRIBE, "", 0);
        assert.ISEQUAL (rc, 0);
    
        int timeout = 250;
        rc = zmq_setsockopt (sub, ZMQ_RCVTIMEO, &timeout, sizeof timeout);
        assert.ISEQUAL (rc, 0);

        char buff [32];
        rc = zmq_recv (sub, buff, sizeof (buff), 0);
        assert.ISEQUAL (rc, -1);
        
        rc = zmq_log (ctx, "Hello, %s!", "World");
        assert.ISEQUAL (rc, 0);

        rc = zmq_recv (sub, buff, sizeof (buff), 0);
        assert.ISEQUAL (rc, 14);
        if (rc >0)
            printf("sub sys://log received: %s\n", buff);

        //  Clean up.
        for (int i = 0
                 ; i < socknum
                 ; ++i) {
            rc = zmq_close (sock[i]);
            assert.ISEQUAL (rc, 0);
        }
        rc = zmq_term (ctx);
        assert.ISEQUAL (rc, 0);
    }
}
