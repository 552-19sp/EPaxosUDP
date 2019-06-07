# EPaxosUDP

Implementation of [EPaxos](https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf), a Paxos-like state machine replication protocol, for [CSE 552 19sp](https://courses.cs.washington.edu/courses/cse552/19sp/). This version of EPaxos assumes and uses a UDP based network

Original Source: https://github.com/efficient/epaxos/tree/master/src

## Run
Built with Go version go1.1.2

To build:

    export GOPATH=[...]/git/epaxos/

    go install master
    go install server
    go install client

To run:

    bin/master &
    bin/server -port 7070 &
    bin/server -port 7071 &
    bin/server -port 7072 &
    bin/client
