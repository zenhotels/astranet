# AstraNet

`astranet` is a package for managing highly concurrent independent network streams.

## How did we get here

While dealing with everyday issues we have approached a situation when we need a huge amount of simultaneous independent data streams between two group of machines. The number was much higher than the standard TCP stack allows to have, even with sys limits tweaked in the right way.

We have checked every existing piece of software we could find to handle this issue, however they all seemed to be insufficient in some way; at the end it has been concluded that there is nothing to choose from, so we decided to roll our own.

So, `astranet` has been created. It does have some extra features as well:

* Millions of independent data streams between two host machines using one tcp connection only;
* Keep-alive frames to be sure the connections are alive;
* NAT traversal capabilities for connecting any two machines without direct route between them using a trusted relay;
* An embedded service discovery system;

As much as possible, the astranet package strives to look and feel just like the standard library's `net` package.
The only thing you'd need to do is setup an astranet instance like this:

```go
var astraNet = astranet.New()
```

An instance conforms this interface:

```go
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

    WithEnv(env ...string) AstraNet
    WithLoopBack() AstraNet
    Client() AstraNet
    Server() AstraNet
    New() AstraNet

    HttpDial(net, host string) (net.Conn, error)
}
```

### Client

Here's how you'd initiate a new client connection to some known astranet service:

```go
conn, err := astraNet.Dial("", "astranet_host:astranet_port")
```

Where `astranet_host` along with `astranet_port` are both "virtual" and are valid only inside an astranet network.

Or as a more convenient way, the same using the embedded service discovery system:

```go
conn, err := astraNet.Dial("", "service_name")
```

Where `service_name` is the name a service used while registering on the network's registry.

### Server

To bind a listener socket as a server:

```go
l, err := astraNet.Bind("", ":astranet_port"))
```

Where `astranet_port` is a virtual port, but the behaviour is like it was a TCP listener.

Or using the embedded service discovery system:

```go
l, err := astraNet.Bind("", "service_name"))
```

Where `service_name` is the name that will be used to register on a network's registry, so clients and other
servers could do service lookups.

You can also use both service discovery and port systems together:

```go
l, err := astraNet.Bind("", "service_name:astranet_port"))
```

#### Accepting streams

The listener returned is pretty much like the `net.Listener` one:

```go
for {
    conn, err := l.Accept()
    go handleSession(conn)
}
```

You usually accept streams opened by the remote side as you would do with the regular `net` package:

```go
stream, err := service.Accept()
```

Streams satisfy the `net.Conn` interface, so they're very familiar to work with:
    
```go
n, err = stream.Write(buf)
n, err = stream.Read(buf)

buf := new(bytes.Buffer)
io.Copy(buf, stream)
```

## Basics

To understand astranet better you should think of it as a P2P network with a flat namespace and forget about physical TCP routing stuff.

Each astranet node receives a random `uint64` id that acts as a hostname. Each listener socket that binds to the astranet receives an `uint32` port for itself, that's pretty much like TCP.

You should establish a connection between two `astraNet` instances to make them visible to each other:

```go
var astraNet1 = astraNet.New()
var astraNet2 = astraNet.New()

astraNet1.ListenAndServe("tcp4", ":10000") // Wait for incoming astranet links on port 10000
astraNet2.Join("tcp4", "127.0.0.1:10000")  // Join the remote astranet instance
// Gz! We are connected now
```

In fact, an astranet instance can attach itself to any managed connection that supports the same capabilities as TCP (ordering, retransmissions, etc), or it could be just a loopback. Internally (in `ListenAndServe`) astranet manages TCP connections itself and does `Attach`. There is an example of a loopback connection:

```go
var client, server IOLoop
server.Reader, client.Writer = io.Pipe()
client.Reader, server.Writer = io.Pipe()

// Use IOLoops as a pipe that conforms io.ReadWriter on the both ends
go astraNet1.Attach(server)
go astraNet2.Attach(client)
```
