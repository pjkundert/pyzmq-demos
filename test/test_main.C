#include <cut>
#include <stdlib.h>

namespace cut {
    test			root( "0MQ Unit Tests" );
}

int
main( int, char ** )
{
    bool success;
    if ( getenv( "REQUEST_METHOD" )) {
        // 
        // Lets run our tests with CGI HTML output:
        // 
        //                            target  sparse  flat   cgi
        //                            ------  ------  ----   ---
        success = cut::htmlrunner( std::cout, false,  true,  true ).run();
    } else {
        // 
        // But, here's the (simpler) textual output:
        // 
        success = cut::runner( std::cout ).run();
    }

    return ! success;
}
