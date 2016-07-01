package astranet

import (
	"hash/crc32"
	"hash/crc64"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenk/backoff"
	"github.com/zenhotels/astranet/addr"
	"github.com/zenhotels/astranet/glog"
	"github.com/zenhotels/astranet/listener"
	"github.com/zenhotels/astranet/protocol"
	"github.com/zenhotels/astranet/route"
	"github.com/zenhotels/astranet/service"
	"github.com/zenhotels/astranet/skykiss"
	"github.com/zenhotels/astranet/socket"
	"github.com/zenhotels/astranet/transport"
)

type fwdLoc struct {
	Host uint64
	Port uint32
}

type discoverLoc struct {
	Host     uint64
	upstream transport.Transport
}

type routeId struct {
	src uint64
	dst uint64
}

type multiplexer struct {
	initCtl     sync.Once
	initDone    bool
	Log         glog.Logger
	MaxDistance int

	cfg struct {
		Env      string
		LoopBack bool
		NoServer bool
		NoClient bool
	}

	joined map[string]bool
	bLock  sync.RWMutex

	discovered map[discoverLoc]bool
	dLock      sync.RWMutex
	dNew       sync.Cond

	lhosts    map[string]bool
	lports    map[string]bool
	lAddrLock sync.Mutex
	lNew      sync.Cond

	fwdCache map[routeId]transport.Transport
	fwdLock  sync.RWMutex

	httpc *http.Client

	routes     route.Registry
	services   service.Registry
	dispatcher transport.Router

	lPort uint32
	local uint64
}

func (mpx *multiplexer) init() {
	mpx.initCtl.Do(func() {
		idLock.Lock()
		mpx.local = uint64(idGen.Int63())
		idLock.Unlock()

		var dbg, _ = strconv.Atoi(os.Getenv("MPXDEBUG"))
		mpx.Log = glog.New(
			dbg,
			log.New(
				os.Stderr,
				addr.Uint2Host(mpx.local)+":",
				log.Lshortfile,
			),
		)
		mpx.Log.VLog(40, func(l *log.Logger) { l.Println("init") })
		mpx.initDone = true
		mpx.discovered = make(map[discoverLoc]bool)
		mpx.dNew.L = &mpx.dLock
		mpx.lPort = 1 << 24
		mpx.joined = make(map[string]bool)
		mpx.MaxDistance = 3
		mpx.lhosts = make(map[string]bool)
		mpx.lports = make(map[string]bool)
		mpx.lNew.L = &mpx.lAddrLock
		mpx.httpc = &http.Client{
			Transport: &http.Transport{Dial: mpx.Dial},
			Timeout:   10 * time.Second,
		}
		mpx.fwdCache = make(map[routeId]transport.Transport)

		go mpx.iohandler()
		go mpx.farAwayLoop()
		go mpx.routesWatcher()
		go mpx.serviceWatcher()
		go mpx.fwdGc()
		go mpx.httpDefaultHandler()

		if mpx.cfg.LoopBack {
			var ioLoop IOLoop
			ioLoop.Reader, ioLoop.Writer = io.Pipe()
			go mpx.attachDistance(ioLoop, 0)
		}

		if os.Getenv("MPXROUTER") != "" {
			go mpx.Join("tcp4", os.Getenv("MPXROUTER"))
		}

		if !mpx.cfg.NoServer {
			var skyBind = os.Getenv("SKYNET_BIND")
			if skyBind != "" {
				go mpx.ListenAndServe("tcp4", skyBind)
			}
		}
	})
}

func (mpx *multiplexer) copy() *multiplexer {
	var other = &multiplexer{}
	other.cfg = mpx.cfg
	return other
}

func (mpx *multiplexer) New() AstraNet {
	return mpx.copy()
}

func (mpx *multiplexer) WithEnv(env ...string) AstraNet {
	var other = mpx.copy()
	if len(env) > 0 {
		other.cfg.Env = "." + strings.Join(env, ".")
	}
	return other
}

func (mpx *multiplexer) WithLoopBack() AstraNet {
	var other = mpx.copy()
	other.cfg.LoopBack = true
	return other
}

func (mpx *multiplexer) Client() AstraNet {
	var other = mpx.copy()
	other.cfg.NoServer = true
	other.cfg.NoClient = false
	return other
}

func (mpx *multiplexer) Server() AstraNet {
	var other = mpx.copy()
	other.cfg.NoServer = false
	other.cfg.NoClient = true
	return other
}

func (mpx *multiplexer) httpDefaultHandler() {
	var httpL, httpLErr = mpx.bind("", 1, "ipc")
	if httpLErr != nil {
		mpx.Log.Panic(httpLErr)
	}
	if serveErr := http.Serve(httpL, nil); serveErr != nil {
		mpx.Log.Panic(serveErr)
	}
}

func (mpx *multiplexer) DialTimeout(network, hp string, t time.Duration) (net.Conn, error) {
	mpx.init()
	if t == 0 {
		t = time.Second * 10
	}
	mpx.Log.VLog(40, func(l *log.Logger) { l.Println("DialTimeout", network, hp, t) })
	var deadline = time.Now().Add(t)
	var hostStr, portStr, hpSplitErr = net.SplitHostPort(hp)
	if hpSplitErr != nil {
		hostStr = hp
	}
	var port, _ = strconv.ParseUint(portStr, 10, 64)

	var algo service.Selector = service.RandomSelector{}

	if netInfo, netErr := url.Parse(network); netErr == nil && netInfo.Scheme == "registry" {
		algo = service.HashRingSelector{VBucket: int(crc32.ChecksumIEEE([]byte(network)))}
	}

	var host, hpErr = addr.Host2Uint(hostStr)
	if hpErr != nil {
		var srv, srvFound = mpx.services.DiscoverTimeout(algo, hp+mpx.cfg.Env, t)
		if !srvFound {
			return nil, &net.AddrError{"Host not found", hp + mpx.cfg.Env}
		}
		host = srv.Host
		port = uint64(srv.Port)
	}

	var cRoute = mpx.findRouteTimeout(host, mpx.MaxDistance, deadline.Sub(time.Now()))
	if cRoute == nil {
		return nil, &net.AddrError{"No route to", addr.Uint2Host(host)}
	}

	var lPort = atomic.AddUint32(&mpx.lPort, 1)
	var conn = socket.NewClientSocket(network, mpx.local, lPort, cRoute)
	cRoute.SendTimeout(protocol.Op{Cmd: opNew,
		Local:  mpx.local,
		Remote: host,
		LPort:  lPort,
		RPort:  uint32(port),
	}, deadline.Sub(time.Now()))
	return conn, nil
}

func (mpx *multiplexer) Dial(network string, hp string) (net.Conn, error) {
	mpx.init()
	return mpx.DialTimeout(network, hp, 0)
}

func (mpx *multiplexer) HttpDial(lnet string, laddr string) (net.Conn, error) {
	mpx.init()
	mpx.Log.VLog(40, func(l *log.Logger) { l.Println("HttpDial") })
	var host, port, hpErr = net.SplitHostPort(laddr)
	if hpErr != nil {
		return nil, hpErr
	}
	if port != "80" {
		host += ":" + port
	}
	return mpx.DialTimeout(lnet, host, 0)
}

func (mpx *multiplexer) Bind(network string, hp string) (net.Listener, error) {
	mpx.init()
	mpx.Log.VLog(40, func(l *log.Logger) { l.Println("Bind") })
	var s, portStr, hpSplitErr = net.SplitHostPort(hp)
	if hpSplitErr != nil {
		s = hp
	}
	var port, _ = strconv.ParseUint(portStr, 10, 64)
	if port == 0 {
		port = uint64(atomic.AddUint32(&mpx.lPort, 1))
	}

	if s != "" {
		s = s + mpx.cfg.Env
	}

	return mpx.bind(network, uint32(port), s)
}

func (mpx *multiplexer) bind(network string, port uint32, s string) (net.Listener, error) {
	var lr = listener.New(network, mpx.local, port, s)
	var tFilter = mpx.dispatcher.Handle(lr.Recv, transport.Filter{
		Cmd:    opNew,
		Remote: mpx.local,
		RPort:  port,
	})
	lr.OnClose(func() {
		tFilter.Close()
	})
	if s != "" {
		mpx.services.Push(s, lr.ServiceInfo)
		lr.OnClose(func() {
			mpx.services.Pop(s, service.ServiceInfo{s, mpx.local, port})
		})
	}
	return lr, nil
}

func (mpx *multiplexer) discoverLoop(upstream transport.Transport, distance int) {
	var discoveryMsg = func(id uint64, distance int) protocol.Op {
		var op = protocol.Op{
			Cmd:   opDiscover,
			Local: id,
		}
		op.Data.Bytes = []byte{byte(distance)}
		return op
	}
	var forgetMsg = func(id uint64, distance int) protocol.Op {
		var op = protocol.Op{
			Cmd:   opForget,
			Local: id,
		}
		op.Data.Bytes = []byte{byte(distance)}
		return op
	}
	upstream.SendTimeout(
		discoveryMsg(mpx.local, distance),
		0,
	)
	if mpx.cfg.NoServer {
		return
	}

	var forEach route.Registry
	var iter = mpx.routes.Iter()
	for !upstream.IsClosed() {
		iter = iter.Next()
		forEach.Sync(&mpx.routes, func(_ uint64, s route.RouteInfo) {
			if s.Distance+distance <= mpx.MaxDistance {
				upstream.Queue(discoveryMsg(s.Host, s.Distance+distance))
			}
		}, func(_ uint64, s route.RouteInfo) {
			if s.Distance+distance <= mpx.MaxDistance {
				upstream.Queue(forgetMsg(s.Host, s.Distance+distance))
			}
		})
	}
}

func (mpx *multiplexer) discoverServiceLoop(upstream transport.Transport, distance int) {
	var serviceMsg = func(id uint64, port uint32, name string) protocol.Op {
		var op = protocol.Op{
			Cmd:   opService,
			Local: id,
			LPort: port,
		}
		op.Data.Bytes = []byte(name)
		return op
	}
	var noServiceMsg = func(id uint64, port uint32, name string) protocol.Op {
		var op = protocol.Op{
			Cmd:   opNoServcie,
			Local: id,
			LPort: port,
		}
		op.Data.Bytes = []byte(name)
		return op
	}

	if mpx.cfg.NoServer {
		return
	}

	var forEach service.Registry
	var iter = mpx.services.Iter()
	for !upstream.IsClosed() {
		iter = iter.Next()
		forEach.Sync(&mpx.services, func(_ string, s service.ServiceInfo) {
			if s.Host != mpx.local {
				return
			}
			upstream.Queue(serviceMsg(s.Host, s.Port, s.Service))
		}, func(_ string, s service.ServiceInfo) {
			if s.Host != mpx.local {
				return
			}
			upstream.Queue(noServiceMsg(s.Host, s.Port, s.Service))
		})
	}
}

func (mpx *multiplexer) routesWatcher() {
	var forEach route.Registry
	var iter = mpx.routes.Iter()

	for {
		iter = iter.Next()
		forEach.Sync(&mpx.routes, func(_ uint64, s route.RouteInfo) {
			mpx.Log.VLog(10, func(l *log.Logger) {
				l.Printf(
					"ADD ROUTE [%s -> %s] through %s {%d}",
					addr.Uint2Host(mpx.local), addr.Uint2Host(s.Host), s.Upstream, s.Distance,
				)
			})
		}, func(_ uint64, s route.RouteInfo) {
			mpx.Log.VLog(10, func(l *log.Logger) {
				l.Printf(
					"DEL ROUTE [%s -> %s] through %s {%d}",
					addr.Uint2Host(mpx.local), addr.Uint2Host(s.Host), s.Upstream, s.Distance,
				)
			})
		})
	}
}

func (mpx *multiplexer) serviceWatcher() {
	var forEach service.Registry
	var iter = mpx.services.Iter()

	for {
		iter = iter.Next()
		forEach.Sync(&mpx.services, func(_ string, s service.ServiceInfo) {
			mpx.Log.VLog(10, func(l *log.Logger) {
				l.Printf("ADD SERVICE %s [%s:%d]", s.Service, addr.Uint2Host(s.Host), s.Port)
			})
		}, func(_ string, s service.ServiceInfo) {
			mpx.Log.VLog(10, func(l *log.Logger) {
				l.Printf("DEL SERVICE %s [%s:%d]", s.Service, addr.Uint2Host(s.Host), s.Port)
			})
		})
	}
}

func (mpx *multiplexer) fwdGc() {
	var cleanup = time.NewTicker(time.Minute)
	defer cleanup.Stop()

	for range cleanup.C {
		var rId2Cleanup = make([]routeId, 0)
		mpx.fwdLock.Lock()
		for rId, upstream := range mpx.fwdCache {
			if upstream.IsClosed() {
				rId2Cleanup = append(rId2Cleanup, routeId{rId.src, rId.dst})
				rId2Cleanup = append(rId2Cleanup, routeId{rId.dst, rId.src})
			}
		}
		for _, rId := range rId2Cleanup {
			delete(mpx.fwdCache, rId)
		}
		mpx.fwdLock.Unlock()
	}
}

func (mpx *multiplexer) p2pNotifyLoop(upstream transport.Transport, distance int) {
	var welcomeMsg = func(id uint64, name string) protocol.Op {
		var op = protocol.Op{
			Cmd:   opJoinMe,
			Local: id,
		}
		op.Data.Bytes = []byte(name)
		return op
	}
	if distance == 1 && upstream.RAddr() != nil {
		var op = protocol.Op{
			Cmd:   opRHost,
			Local: mpx.local,
		}
		op.Data.Bytes = []byte(upstream.RAddr().String())
		upstream.Queue(op)
	}

	if mpx.cfg.NoServer {
		return
	}

	mpx.lAddrLock.Lock()
	var sent = map[string]bool{}
	for !upstream.IsClosed() {
		for lhost := range mpx.lhosts {
			for lport := range mpx.lports {
				var loc = lhost + lport
				if !sent[loc] {
					upstream.Queue(welcomeMsg(mpx.local, lhost+":"+lport))
					sent[loc] = true
				}
			}
		}
		skykiss.WaitTimeout(&mpx.lNew, time.Minute)
	}
	mpx.lAddrLock.Unlock()
}

func (mpx *multiplexer) Services() (services []service.ServiceInfo) {
	mpx.init()
	var forEach service.Registry
	forEach.Sync(&mpx.services, func(_ string, s service.ServiceInfo) {
		services = append(services, s)
	}, nil)
	return
}

func (mpx *multiplexer) ServiceMap() *service.Registry {
	mpx.init()
	return &mpx.services
}

func (mpx *multiplexer) Routes() (r []route.RouteInfo) {
	mpx.init()
	var forEach route.Registry
	forEach.Sync(&mpx.routes, func(_ uint64, s route.RouteInfo) {
		r = append(r, s)
	}, nil)
	return
}

func (mpx *multiplexer) RoutesMap() *route.Registry {
	mpx.init()
	return &mpx.routes
}

func (mpx *multiplexer) attachDistance(conn io.ReadWriter, distance int) {
	var _, wg = mpx.attachDistanceNonBlock(conn, distance)
	wg.Wait()
}

func (mpx *multiplexer) attachDistanceNonBlock(conn io.ReadWriter, distance int) (transport.Transport, *sync.WaitGroup) {
	mpx.init()
	var wg sync.WaitGroup
	wg.Add(1)

	var keepalive = time.Second * 10
	if distance > 1 {
		keepalive = time.Minute
	}

	var remote = transport.Upstream(conn, mpx.Log, mpx.EventHandler(&wg), keepalive)

	go mpx.discoverLoop(remote, distance)
	go mpx.discoverServiceLoop(remote, distance)
	go mpx.p2pNotifyLoop(remote, distance)

	go func() {
		remote.Join()
		wg.Done()
		remote.Close()
	}()

	return remote, &wg
}

func (mpx *multiplexer) Attach(conn io.ReadWriter) {
	mpx.init()
	mpx.Log.VLog(40, func(l *log.Logger) { l.Println("Attach") })
	mpx.attachDistance(conn, 1)
}

func (mpx *multiplexer) ListenAndServe(network, address string) error {
	mpx.init()
	mpx.Log.VLog(40, func(l *log.Logger) { l.Println("ListenAndServe") })
	mpx.Log.VLog(5, func(l *log.Logger) { l.Println("serving on", network, address) })
	var l, lErr = net.Listen(network, address)
	if lErr != nil {
		return lErr
	}

	var _, lPort, _ = net.SplitHostPort(l.Addr().String())
	mpx.lAddrLock.Lock()
	var lFound = mpx.lports[lPort] == false
	mpx.lports[lPort] = true
	mpx.lAddrLock.Unlock()
	if lFound {
		mpx.lNew.Broadcast()
	}

	go func() {
		for {
			var conn, connErr = l.Accept()
			if connErr != nil {
				lErr = connErr
				break
			}
			go mpx.Attach(conn)
		}
	}()
	return nil
}

func (mpx *multiplexer) localHostDiscover(l net.Addr) {
	var lHost, _, _ = net.SplitHostPort(l.String())
	if lHost != "127.0.0.1" {
		mpx.lAddrLock.Lock()
		var lFound = mpx.lhosts[lHost] == false
		mpx.lhosts[lHost] = true
		mpx.lAddrLock.Unlock()
		if lFound {
			mpx.lNew.Broadcast()
		}
	}
}

func (mpx *multiplexer) Join(network, address string) error {
	mpx.init()
	mpx.Log.VLog(40, func(l *log.Logger) { l.Println("Join") })
	mpx.bLock.Lock()
	var _, joined = mpx.joined[network+address]
	mpx.bLock.Unlock()
	if joined {
		return nil
	}
	mpx.Log.VLog(10, func(l *log.Logger) { l.Println("join network at", network, address) })
	var l, lErr = net.Dial(network, address)

	mpx.bLock.Lock()
	mpx.joined[network+address] = true
	mpx.bLock.Unlock()

	var retry = backoff.NewExponentialBackOff()
	retry.MaxElapsedTime = time.Hour * 4
	retry.MaxInterval = time.Minute
	var ticker = backoff.NewTicker(retry)

	go func() {
		for range ticker.C {
			if lErr == nil {
				mpx.localHostDiscover(l.LocalAddr())
				mpx.Attach(l)
				retry.Reset()
			} else {
				mpx.Log.VLog(30, func(l *log.Logger) { l.Println("Could not join", network, address) })
			}
			l, lErr = net.Dial(network, address)
		}
	}()

	return nil
}

func (mpx *multiplexer) findRouteTimeout(remote uint64, distance int, t time.Duration) (r transport.Transport) {
	if s, ok := mpx.routes.DiscoverTimeout(route.RndDistSelector{}, remote, t); ok && s.Distance <= distance {
		r = s.Upstream
	}
	return
}

func (mpx *multiplexer) iohandler() {
	var fwdHandler, fwdHandlerErr = mpx.bind("", 0, "")
	if fwdHandlerErr != nil {
		panic(fwdHandlerErr)
	}
	go func() {
		for {
			var fwdConn, fwdConnErr = fwdHandler.Accept()
			if fwdConnErr != nil {
				mpx.Log.VLog(20, func(l *log.Logger) { l.Println("Fwd handler accept err", fwdConnErr) })
				continue
			} else {
				mpx.Log.VLog(20, func(l *log.Logger) {
					l.Println("New connection from faraway host", fwdConn.RemoteAddr())
				})
				go func() {
					defer fwdConn.Close()
					mpx.attachDistance(fwdConn, 3)
					mpx.Log.VLog(20, func(l *log.Logger) {
						l.Println("faraway host lost", fwdConn.RemoteAddr())
					})
				}()
			}
		}
	}()
}

func (mpx *multiplexer) farAwayLoop() {
	var vHostDial = map[uint64]chan bool{}
	var vHostLoc = map[uint64]map[transport.Transport]bool{}
	var vHostDialLock sync.Mutex

	mpx.dLock.Lock()
	for {
		var cleanup = []discoverLoc{}
		var upstreamCleanup = map[transport.Transport]bool{}
		for loc := range mpx.discovered {
			if loc.upstream.IsClosed() {
				cleanup = append(cleanup, loc)
				upstreamCleanup[loc.upstream] = true
				continue
			}
			if vHostLoc[loc.Host] == nil {
				vHostLoc[loc.Host] = map[transport.Transport]bool{}
			}
			vHostLoc[loc.Host][loc.upstream] = true
		}
		for _, cI := range cleanup {
			delete(mpx.discovered, cI)
		}

		var vHostCleanup = []uint64{}
		vHostDialLock.Lock()
		for vHost, vLoc := range vHostLoc {
			for upstream := range upstreamCleanup {
				delete(vLoc, upstream)
			}
			if len(vLoc) == 0 {
				vHostCleanup = append(vHostCleanup, vHost)
				continue
			}
			if vHostDial[vHost] == nil {
				vHostDial[vHost] = make(chan bool, 1)
			}
			for upstream := range vLoc {
				select {
				case vHostDial[vHost] <- true:
					go func(vHost uint64, upstream transport.Transport) {
						var deadline = time.Now().Add(time.Second)
						var route = mpx.findRouteTimeout(vHost, 1, deadline.Sub(time.Now()))
						if route != nil {
							mpx.Log.VLog(20, func(l *log.Logger) {
								l.Println(
									"No more faraway connections allowed for",
									addr.Uint2Host(vHost),
								)
							})
							return
						}
						var lPort = atomic.AddUint32(&mpx.lPort, 1)
						var remoteConn = socket.NewClientSocket("", mpx.local, lPort, upstream)
						upstream.SendTimeout(protocol.Op{Cmd: opNew,
							Local:  mpx.local,
							Remote: vHost,
							LPort:  lPort,
							RPort:  0,
						}, deadline.Sub(time.Now()))

						var remote, wg = mpx.attachDistanceNonBlock(remoteConn, 3)
						go func() {
							for !remote.IsClosed() {
								var route = mpx.findRouteTimeout(vHost, 1, time.Second)
								if route != nil {
									remote.Drain()
								}
								time.Sleep(time.Minute)
							}
						}()
						wg.Wait()
						remote.Close()
						mpx.dNew.Broadcast()
						vHostDialLock.Lock()
						<-vHostDial[vHost]
						vHostDialLock.Unlock()
					}(vHost, upstream)
				default:
				}
				break
			}
		}

		for _, vH := range vHostCleanup {
			delete(vHostLoc, vH)
		}
		vHostDialLock.Unlock()

		mpx.dNew.Wait()
	}
	mpx.dLock.Unlock()
}

func (mpx *multiplexer) discover(upstream transport.Transport, host uint64, distance int) {
	if mpx.cfg.NoClient {
		return
	}
	var loc = discoverLoc{host, upstream}
	mpx.dLock.Lock()
	if !mpx.discovered[loc] {
		mpx.discovered[loc] = true
		mpx.dNew.Broadcast()
	}
	mpx.dLock.Unlock()
}

func (mpx *multiplexer) EventHandler(wg *sync.WaitGroup) transport.Callback {
	var routes route.Registry
	var services service.Registry
	go func() {
		wg.Wait()
		routes.Close()
		services.Close()
	}()

	var cb = func(job protocol.Op, upstream transport.Transport) {
		if job.Remote != mpx.local && job.Remote != 0 {
			// Forward chain here
			var rId = routeId{job.Local, job.Remote}
			var rrId = routeId{job.Remote, job.Local}
			// Fastpath
			mpx.fwdLock.RLock()
			var cachedRoute = mpx.fwdCache[rId]
			var rCachedRoute = mpx.fwdCache[rrId]
			mpx.fwdLock.RUnlock()
			if cachedRoute != nil && rCachedRoute != nil && !cachedRoute.IsClosed() && !rCachedRoute.IsClosed() {
				cachedRoute.Queue(job)
				mpx.Log.VLog(50, func(l *log.Logger) {
					l.Println("FWD", job, cachedRoute.String())
				})
				return
			}

			var dst, found = mpx.routes.DiscoverTimeout(route.RndDistSelector{}, job.Remote, 0)
			if !found || dst.Distance > 1 {
				mpx.Log.VLog(10, func(l *log.Logger) {
					l.Println("Can't forward to", addr.Uint2Host(job.Remote))
				})
				return
			}

			// Slowpath
			mpx.fwdLock.Lock()
			cachedRoute = mpx.fwdCache[rId]
			rCachedRoute = mpx.fwdCache[rrId]
			if cachedRoute == nil || rCachedRoute == nil || cachedRoute.IsClosed() || rCachedRoute.IsClosed() {
				mpx.fwdCache[rId] = dst.Upstream
				mpx.fwdCache[rrId] = upstream
				cachedRoute = dst.Upstream
			}
			mpx.fwdLock.Unlock()
			cachedRoute.Queue(job)
			mpx.Log.VLog(50, func(l *log.Logger) {
				l.Println("FWD", job, cachedRoute.String())
			})
			return
		}

		switch job.Cmd {
		case opJoinMe:
			if !mpx.cfg.NoClient {
				go mpx.Join("tcp4", string(job.Data.Bytes))
			}
		case opDiscover:
			var r = route.RouteInfo{
				Host:     job.Local,
				Distance: int(byte(job.Data.Bytes[0])),
				Upstream: upstream,
			}
			if r.Host == mpx.local && r.Distance > 0 {
				return
			}
			if r.Distance == 2 {
				go mpx.discover(upstream, r.Host, r.Distance)
			} else {
				mpx.routes.Push(r.Host, r)
				routes.Push(r.Host, r, func() {
					mpx.routes.Pop(r.Host, r)
				})
			}
		case opForget:
			var r = route.RouteInfo{
				Host:     job.Local,
				Distance: int(byte(job.Data.Bytes[0])),
				Upstream: upstream,
			}
			if r.Host == mpx.local && r.Distance > 0 {
				return
			}
			routes.Pop(r.Host, r)
		case opService:
			var s = service.ServiceInfo{
				Service: string(job.Data.Bytes),
				Host:    job.Local,
				Port:    job.LPort,
			}
			if s.Host == mpx.local {
				return
			}
			mpx.services.Push(s.Service, s)
			services.Push(s.Service, s, func() {
				mpx.services.Pop(s.Service, s)
			})
		case opNoServcie:
			var s = service.ServiceInfo{
				Service: string(job.Data.Bytes),
				Host:    job.Local,
				Port:    job.LPort,
			}
			if s.Host == mpx.local {
				return
			}
			services.Pop(s.Service, s)
		case opRHost:
			var sName = string(job.Data.Bytes)
			var host, _, hpErr = net.SplitHostPort(sName)
			if hpErr != nil {
				mpx.Log.VLog(20, func(l *log.Logger) { l.Println("Broken OP_RHOST command", sName) })
			}
			mpx.lAddrLock.Lock()
			if !mpx.lhosts[host] {
				mpx.lhosts[host] = true
				mpx.lNew.Broadcast()
			}
			mpx.lAddrLock.Unlock()
		case socket.OpFin2:
			// Silently skip some ghost OP_FIN2 frames
		case opNew:
			var cb = mpx.dispatcher.CheckFrame(job)
			if cb != nil {
				cb(job, upstream)
			} else {
				mpx.Log.VLog(10, func(l *log.Logger) { l.Println("Unknown frame", job) })
			}
		default:
			mpx.Log.VLog(10, func(l *log.Logger) { l.Println("Unknown frame", job) })
			//mpx.Log.Panic("Unknown frame", job)
		}
	}

	return cb
}

var machineNs = skykiss.NewV1()
var idGen = rand.NewSource(int64(crc64.Checksum(machineNs.Bytes(), crc64.MakeTable(crc64.ECMA))))
var idLock sync.Mutex

func New() AstraNet {
	return (&multiplexer{}).New()
}

var mpxId skykiss.AutoIncSequence
