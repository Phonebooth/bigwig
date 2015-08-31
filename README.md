# bigwig
a log file indexer and correlator

Building:

    1. Install go (v1.5+)
    2. Setup a working directory and export its location as GOPATH    
    3. make

Running:

    For the following examples, refer to the example.log file in this directory.

    Bigwig needs to index a log file before it can do any correlation.
    The index can be saved to improve performance of subsequent uses.
 
    The following invocation indexes the file example.log and saves the index
    to a file named example.log.index.

        $ bigwig -s -f example.log

    To correlate one of the two "calls" in the file from the saved index, run the following:

        $ bigwig -l example.log.index -f example.log -c +18885554444

    The results should be:

        2015-08-25 15:26:38.965 [info] <0.28349.4629>@mod:func:1 connection_1
        2015-08-25 15:26:39.068 [info] <0.847.1748>@mod:func:1 session_1 conference_1
        2015-08-25 15:26:39.068 [info] <0.25538.4396>@mod:func:1 +18885554444 connection_1
        2015-08-25 15:26:39.069 [info] <0.16449.4409>@mod:func:1 connection_1 conference_1

    The same results can be achieved by passing these arguments instead:

        -c connection_1
        -c session_1
        -c conference_1

    Note that the original log file is always needed to show results.  Just using the index
    file is not enough.
