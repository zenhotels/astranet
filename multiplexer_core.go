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

	"encoding/json"

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

type rstLoc struct {
	host     uint64
	upstream transport.Transport
}

type routeId struct {
	src uint64
	dst uint64
}

type clientCaps struct {
	ImprovedOpService bool
	Host              uint64
	Upstream          transport.Transport
	Meta              WelcomeInfo

	init chan struct{}
}

func (self *clientCaps) Close() {
	defer func() { recover() }()
	close(self.init)
}

type multiplexer struct {
	initCtl  sync.Once
	initDone bool
	Log      glog.Logger

	cfg struct {
		Env      string
		LoopBack bool
		Server   bool
		Client   bool
		Router   bool
	}

	joined map[string]bool
	bLock  sync.RWMutex

	lhosts    map[string]bool
	lports    map[string]bool
	lAddrLock sync.Mutex
	lNew      sync.Cond

	rstHndl map[rstLoc]map[uint64]net.Conn
	rstLock sync.Mutex

	fwdCache map[routeId]transport.Transport
	fwdLock  sync.RWMutex

	httpc *http.Client

	routes        route.Registry
	services      service.Registry
	neighbourhood service.Registry
	dispatcher    transport.Router

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
		mpx.lPort = 1 << 24
		mpx.joined = make(map[string]bool)
		mpx.lhosts = make(map[string]bool)
		mpx.lports = make(map[string]bool)
		mpx.lNew.L = &mpx.lAddrLock
		mpx.httpc = &http.Client{
			Transport: &http.Transport{Dial: mpx.Dial},
			Timeout:   10 * time.Second,
		}
		mpx.fwdCache = make(map[routeId]transport.Transport)
		mpx.rstHndl = make(map[rstLoc]map[uint64]net.Conn)

		go mpx.routesWatcher()
		go mpx.serviceWatcher()
		go mpx.fwdGc()
		go mpx.httpDefaultHandler()

		if mpx.cfg.LoopBack {
			var ioLoop IOLoop
			ioLoop.Reader, ioLoop.Writer = io.Pipe()
			go mpx.attach(ioLoop)
		}

		if os.Getenv("MPXROUTER") != "" {
			go mpx.Join("tcp4", os.Getenv("MPXROUTER"))
		}

		if mpx.cfg.Server {
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
	var other = mpx.copy()
	other.cfg.Server = true
	other.cfg.Client = true
	other.cfg.Router = false
	return other
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
	other.cfg.Server = false
	other.cfg.Client = true
	other.cfg.Router = false
	return other
}

func (mpx *multiplexer) Server() AstraNet {
	var other = mpx.copy()
	other.cfg.Server = true
	other.cfg.Client = false
	other.cfg.Router = false
	return other
}

func (mpx *multiplexer) Router() AstraNet {
	var other = mpx.copy()
	other.cfg.Server = true
	other.cfg.Client = true
	other.cfg.Router = true
	return other
}

func (mpx *multiplexer) httpDefaultHandler() {
	var httpL, httpLErr = mpx.bind("", 1, "ipc."+addr.Uint2Host(mpx.local)+mpx.cfg.Env)
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

	var selector service.Selector = service.RandomSelector{}

	if netInfo, netErr := url.Parse(network); netErr == nil && netInfo.Scheme == "registry" {
		selector = service.HashRingSelector{VBucket: int(crc32.ChecksumIEEE([]byte(network)))}
	}

	if network == "vport2registry" {
		selector = service.HashRingSelector{VBucket: int(crc32.ChecksumIEEE([]byte(portStr)))}
		hp = hostStr
	}

	var host, hpErr = addr.Host2Uint(hostStr)
	var cRoute transport.Transport
	if hpErr != nil {
		var srv, srvFound = mpx.neighbourhood.DiscoverTimeout(selector, hp+mpx.cfg.Env, t, service.UniqueHP)
		if !srvFound {
			return nil, &net.AddrError{"Host not found", hp + mpx.cfg.Env}
		}
		host = srv.Host
		port = uint64(srv.Port)
		cRoute = srv.Upstream
	}

	if cRoute == nil {
		cRoute = mpx.findRouteTimeout(host, 0, deadline.Sub(time.Now()))
	}
	if cRoute == nil {
		return nil, &net.AddrError{"No route to", addr.Uint2Host(host)}
	}

	var connId = connId.Next()
	var rstId = rstLoc{
		host:     host,
		upstream: cRoute,
	}
	var onClose = func() {
		mpx.rstLock.Lock()
		var rstGroup = mpx.rstHndl[rstId]
		if rstGroup != nil {
			delete(rstGroup, connId)
			if len(rstGroup) == 0 {
				delete(mpx.rstHndl, rstId)
			}
		}
		mpx.rstLock.Unlock()
	}

	var lPort = atomic.AddUint32(&mpx.lPort, 1)
	var conn = socket.NewClientSocket(network, mpx.local, lPort, cRoute, onClose)

	mpx.rstLock.Lock()
	if mpx.rstHndl[rstId] == nil {
		mpx.rstHndl[rstId] = make(map[uint64]net.Conn)
	}
	mpx.rstHndl[rstId][connId] = conn
	mpx.rstLock.Unlock()

	cRoute.SendTimeout(protocol.Op{Cmd: opDial,
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
		Cmd:    opDial,
		Remote: mpx.local,
		RPort:  port,
	})
	lr.OnClose(func() {
		tFilter.Close()
	})
	if s != "" {
		mpx.services.Push(s, lr.ServiceInfo)
		mpx.neighbourhood.Push(s, lr.ServiceInfo)
		lr.OnClose(func() {
			mpx.services.Pop(s, lr.ServiceInfo)
			mpx.neighbourhood.Pop(s, lr.ServiceInfo)
		})
	}
	return lr, nil
}

func (mpx *multiplexer) discoverServiceLoop(upstream transport.Transport, caps clientCaps) {
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
			Cmd:   opNoServiсe,
			Local: id,
			LPort: port,
		}
		op.Data.Bytes = []byte(name)
		return op
	}

	if !mpx.cfg.Server {
		return
	}

	var forEach service.Registry
	var iter = mpx.services.Iter()
	for !upstream.IsClosed() {
		iter = iter.Next()
		forEach.Sync(&mpx.services, func(_ string, s service.ServiceInfo) {
			if caps.ImprovedOpService || s.Host == mpx.local {
				upstream.Queue(serviceMsg(s.Host, s.Port, s.Service))
			}
		}, func(_ string, s service.ServiceInfo) {
			if caps.ImprovedOpService || s.Host == mpx.local {
				upstream.Queue(noServiceMsg(s.Host, s.Port, s.Service))
			}
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
	var cleanup = time.NewTicker(time.Second)
	defer cleanup.Stop()

	for range cleanup.C {
		var rId2Cleanup = make([]routeId, 0)
		mpx.fwdLock.Lock()
		for rId, upstream := range mpx.fwdCache {
			if upstream.IsClosed() {
				var rId = routeId{rId.src, rId.dst}
				var rrId = routeId{rId.dst, rId.src}
				rId2Cleanup = append(rId2Cleanup, rId)
				rId2Cleanup = append(rId2Cleanup, rrId)
				var pairRoute = mpx.fwdCache[rrId]
				if pairRoute != nil && !pairRoute.IsClosed() {
					pairRoute.Queue(protocol.Op{
						Cmd:    opRst,
						Local:  rId.dst,
						Remote: rId.src,
					})
				}
			}
		}
		for _, rId := range rId2Cleanup {
			delete(mpx.fwdCache, rId)
		}
		mpx.fwdLock.Unlock()
	}
}

func (mpx *multiplexer) p2pNotifyLoop(upstream transport.Transport, caps clientCaps) {
	var welcomeMsg = func(id uint64, name string) protocol.Op {
		var op = protocol.Op{
			Cmd:   opFollow,
			Local: id,
		}
		op.Data.Bytes = []byte(name)
		return op
	}

	if !mpx.cfg.Server {
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
	forEach.Sync(&mpx.neighbourhood, func(_ string, s service.ServiceInfo) {
		services = append(services, s)
	}, nil)
	return
}

func (mpx *multiplexer) ServiceMap() *service.Registry {
	mpx.init()
	return &mpx.neighbourhood
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

func (mpx *multiplexer) attach(conn io.ReadWriter) {
	var _, wg = mpx.attachNonBlock(conn)
	wg.Wait()
}

func (mpx *multiplexer) attachNonBlock(conn io.ReadWriter) (transport.Transport, *sync.WaitGroup) {
	mpx.init()
	var wg sync.WaitGroup
	wg.Add(1)

	var keepalive = time.Second * 10

	var caps = clientCaps{init: make(chan struct{})}
	var mpxHndl transport.Callback
	var opCb = func(job protocol.Op, upstream transport.Transport) {
		switch job.Cmd {
		case opHandshake:
			mpx.Log.VLog(10, func(l *log.Logger) {
				l.Println("Handshake with", addr.Uint2Host(job.Local))
			})
			var decErr = json.Unmarshal(job.Data.Bytes, &caps.Meta)
			switch {
			case decErr != nil:
				mpx.Log.VLog(10, func(l *log.Logger) {
					l.Println("Handshake err", addr.Uint2Host(job.Local), decErr)
				})
			default:
				caps.ImprovedOpService = true
				caps.Host = job.Local
				caps.Upstream = upstream
			}
			caps.Close()
		default:
			if caps.ImprovedOpService {
				mpxHndl(job, upstream)
			}
		}
	}

	caps.Upstream = transport.Upstream(conn, mpx.Log, opCb, keepalive)
	mpxHndl = mpx.EventHandler(&wg, &caps)

	// --- Send handshake frame ---
	var wInfo WelcomeInfo
	if caps.Upstream.RAddr() != nil {
		wInfo.RAddr = caps.Upstream.RAddr().String()
	}
	var wFrame = protocol.Op{Cmd: opHandshake, Local: mpx.local}
	wFrame.Data.Bytes, _ = json.Marshal(wInfo)
	caps.Upstream.Queue(wFrame)
	// ---

	var w4handshake = time.NewTimer(time.Second * 10)
	select {
	case <-caps.init:
	case <-w4handshake.C:
		mpx.Log.VLog(10, func(l *log.Logger) {
			l.Println("Handshake timeout", caps.Upstream)
		})
		caps.Upstream.Close()
		caps.Close()
	}
	w4handshake.Stop()
	if !caps.ImprovedOpService {
		wg.Done()
		caps.Upstream.Close()
		return caps.Upstream, &wg
	}

	go mpx.discoverServiceLoop(caps.Upstream, caps)
	go mpx.p2pNotifyLoop(caps.Upstream, caps)

	go func() {
		caps.Upstream.Join()
		wg.Done()
		caps.Upstream.Close()
	}()

	return caps.Upstream, &wg
}

func (mpx *multiplexer) Attach(conn io.ReadWriter) {
	mpx.init()
	mpx.attach(conn)
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

func (mpx *multiplexer) join(network, address string) error {
	mpx.init()
	mpx.bLock.Lock()
	var _, joined = mpx.joined[network+address]
	mpx.bLock.Unlock()
	if joined {
		return nil
	}
	mpx.Log.VLog(10, func(l *log.Logger) { l.Println("join network at", network, address) })
	var l, lErr = net.Dial(network, address)

	mpx.bLock.Lock()
	_, joined = mpx.joined[network+address]
	mpx.joined[network+address] = true
	mpx.bLock.Unlock()

	if joined {
		if l != nil {
			l.Close()
		}
		return nil
	}

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

func (mpx *multiplexer) Join(network, address string) error {
	var host, port, _ = net.SplitHostPort(address)
	var addrList, lookupErr = net.LookupHost(host)
	if lookupErr == nil && len(addrList) > 0 {
		for _, addr := range addrList {
			mpx.join(network, net.JoinHostPort(addr, port))

		}
	} else {
		return mpx.join(network, address)
	}
	return nil
}

func (mpx *multiplexer) findRouteTimeout(remote uint64, distance int, t time.Duration) (r transport.Transport) {
	if s, ok := mpx.routes.DiscoverTimeout(route.RndDistSelector{}, remote, t); ok && s.Distance <= distance {
		r = s.Upstream
	}
	return
}

func (mpx *multiplexer) fwd(job protocol.Op, upstream transport.Transport) {
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

	// Notify remote on closed fwd route
	if cachedRoute != nil && cachedRoute.IsClosed() && rCachedRoute != nil && !rCachedRoute.IsClosed() {
		rCachedRoute.Queue(protocol.Op{
			Cmd:    opRst,
			Local:  rId.dst,
			Remote: rId.src,
		})
	}
	if rCachedRoute != nil && rCachedRoute.IsClosed() && cachedRoute != nil && !cachedRoute.IsClosed() {
		rCachedRoute.Queue(protocol.Op{
			Cmd:    opRst,
			Local:  rId.src,
			Remote: rId.dst,
		})
	}
	// Notify remote done

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

func (mpx *multiplexer) EventHandler(wg *sync.WaitGroup, caps *clientCaps) transport.Callback {
	var services service.Registry

	go func() {
		<-caps.init
		var routeInfo = route.RouteInfo{
			Host:     caps.Host,
			Distance: 0,
			Upstream: caps.Upstream,
		}

		mpx.routes.Push(caps.Host, routeInfo)

		if len(caps.Meta.RAddr) > 0 {
			var host, _, hpErr = net.SplitHostPort(caps.Meta.RAddr)
			if hpErr != nil {
				mpx.Log.VLog(20, func(l *log.Logger) { l.Println("Broken RAddr", caps.Meta.RAddr) })
			}
			mpx.lAddrLock.Lock()
			if !mpx.lhosts[host] {
				mpx.lhosts[host] = true
				mpx.lNew.Broadcast()
			}
			mpx.lAddrLock.Unlock()
		}

		go func() {
			wg.Wait()
			services.Close()
			mpx.routes.Pop(caps.Host, routeInfo)
		}()
	}()

	var cb = func(job protocol.Op, upstream transport.Transport) {
		if job.Remote != mpx.local && job.Remote != 0 {
			mpx.fwd(job, upstream)
			return
		}

		switch job.Cmd {
		case opFollow:
			var hp = string(job.Data.Bytes)
			if mpx.cfg.Client && job.Local != mpx.local {
				go mpx.Join("tcp4", hp)
			}
		case opService:
			var s = service.ServiceInfo{
				Service:  string(job.Data.Bytes),
				Host:     job.Local,
				Port:     job.LPort,
				Upstream: upstream,
			}
			if s.Host != caps.Host {
				s.Priority = 1
				if strings.HasSuffix(s.Service, mpx.cfg.Env) {
					upstream.Queue(protocol.Op{
						Cmd:    opDiscover,
						Local:  mpx.local,
						Remote: s.Host,
					})
				}
			}

			if s.Priority == 0 && mpx.cfg.Router {
				mpx.neighbourhood.Push(s.Service, s)
				mpx.services.Push(s.Service, s)
				services.Push(s.Service, s, func() {
					mpx.neighbourhood.Pop(s.Service, s)
					mpx.services.Pop(s.Service, s)
				})
			} else {
				mpx.neighbourhood.Push(s.Service, s)
				services.Push(s.Service, s, func() {
					mpx.neighbourhood.Pop(s.Service, s)
				})
			}
		case opNoServiсe:
			var s = service.ServiceInfo{
				Service:  string(job.Data.Bytes),
				Host:     job.Local,
				Port:     job.LPort,
				Upstream: upstream,
			}
			if s.Host != caps.Host {
				s.Priority = 1
			}
			services.Pop(s.Service, s)
		case opDial:
			var cb = mpx.dispatcher.CheckFrame(job)
			if cb != nil {
				cb(job, upstream)
			} else {
				mpx.Log.VLog(10, func(l *log.Logger) { l.Println("Unknown frame", job) })
			}
		case opDiscover:
			mpx.lAddrLock.Lock()
			for lhost := range mpx.lhosts {
				for lport := range mpx.lports {
					var op = protocol.Op{
						Cmd:    opFollow,
						Local:  mpx.local,
						Remote: job.Local,
					}
					op.Data.Bytes = []byte(net.JoinHostPort(lhost, lport))
					upstream.Queue(op)
				}
			}
			mpx.lAddrLock.Unlock()
		case opRst:
			var rstId = rstLoc{
				host:     job.Local,
				upstream: upstream,
			}
			mpx.rstLock.Lock()
			var rstGroup = mpx.rstHndl[rstId]
			if rstGroup != nil {
				for _, conn := range rstGroup {
					go conn.Close()
				}
			}
			mpx.rstLock.Unlock()
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

var connId skykiss.AutoIncSequence
