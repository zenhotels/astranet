# astranet - Library for managing highly concurrent independent network streams

## What astranet is created for? ##
While dealing with everyday issues we have come to a situation
when we need a huge number of simultaneous independent data streams between two group of machines.
This number was much higher than standard TCP stack allows us to create.
We have checked for ready solutions to handle the issue and found out there are zero of them.

To sum it up, astranet is created. It has some extra features as well:
* Millions of independent data streams between two host machines using one tcp connection only
* Embedded service discovery system
* Ability to connect two machines without direct connection between them using 3rd one as a router
* Keep-alive frames to be sure the connections are alive


As much as possible, the astranet library strives to look and feel just like the standard library's net package.
You only thing you need is to setup AstraNet instance

    var astraNet = astranet.New()

    type AstraNet interface {
    	Dial(network string, hp string) (net.Conn, error)
    	DialTimeout(network string, hp string, t time.Duration) (net.Conn, error)
    	Bind(network string, hp string) (net.Listener, error)

    	Attach(conn io.ReadWriter)
    	ListenAndServe(network, address string) error
    	Join(network, address string) error
    	Services() []ServiceId
    	Routes() []ServiceId
    	ServiceMap() *RegistryStorage
    	RoutesMap() *RegistryStorage

    	WithEnv(env ...string) Multiplexer
    	WithLoopBack() Multiplexer
    	Client() Multiplexer
    	Server() Multiplexer
    	New() Multiplexer

    	HttpDial(net, host string) (net.Conn, error)
    }


### Client ###

Here's how you initiate a new client connection to a known astranet service:

    conn, err := astraNet.Dial("", "astranet_host:astranet_port")

or using embedded service discovery system

    conn, err := astraNet.Dial("", "service_name")

### Server ###

To bind a listener socket as a server:

    l, err := astraNet.Bind("", ":astranet_port"))

or using embedded service discovery system:

    l, err := astraNet.Bind("", "service_name"))

You can also use service discovery and port systems together:

    l, err := astraNet.Bind("", "service_name:astranet_port"))

The listener returned is pretty much like net.Listener one:

    for {
        service, err := l.Accept()
        go handleSession(service)
    }

You accept streams opened by the remote side as you would do with regular net package:

    stream, err := service.Accept()

Streams satisfy the net.Conn interface, so they're very familiar to work with:
    
    n, err = stream.Write(buf)
    n, err = stream.Read(buf)

## Basics ##

To understand astranet better you should think of it as P2P network with flat namespace and forget about physical tcp routing stuff.

Each astranet node receives a random uin64 id with acts as hostname. Each listener socket bind to astranet receives uint32 port it, pretty much like in TCP.
You should establish connection between two astraNet instances to make them visible to each other.

     var astraNet1 = astraNet.New()
     var astraNet2 = astraNet.New()

     astraNet1.ListenAndServe("tcp4", ":10000") // Wait for incoming astranet links on port 10000
     astraNet2.Join("tcp4", "127.0.0.1:10000")  // Join remote astranet instance
     // Gz! We are connected now

