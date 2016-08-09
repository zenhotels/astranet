package astranet

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
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
	"github.com/zenhotels/astranet/skykiss"
)

type Op struct {
	Cmd    ConnState    // 1 byte
	Local  uint64       // 9 byte
	Remote uint64       // 17 byte
	LPort  uint32       // 21 byte
	RPort  uint32       // 25 byte
	Data   BytesPackage // 29 byte with header
	Route  *mpxRemote
}

func (op Op) Encode() BytesPackage {
	var bEnc = binary.BigEndian
	var buf = BytesNew(29 + len(op.Data.Bytes))

	buf.Bytes[0] = byte(op.Cmd)
	bEnc.PutUint64(buf.Bytes[1:9], op.Remote)
	bEnc.PutUint64(buf.Bytes[9:17], op.Local)
	bEnc.PutUint32(buf.Bytes[17:21], op.RPort)
	bEnc.PutUint32(buf.Bytes[21:25], op.LPort)
	bEnc.PutUint32(buf.Bytes[25:29], uint32(len(op.Data.Bytes)))
	copy(buf.Bytes[29:], op.Data.Bytes)
	return buf
}

func (op *Op) Decode(buf []byte) {
	var bEnc = binary.BigEndian

	op.Cmd = ConnState(buf[0])
	op.Local = bEnc.Uint64(buf[1:9])
	op.Remote = bEnc.Uint64(buf[9:17])
	op.LPort = bEnc.Uint32(buf[17:21])
	op.RPort = bEnc.Uint32(buf[21:25])

	var bLen = int(bEnc.Uint32(buf[25:29]))
	op.Data.Bytes = buf[29 : 29+bLen]
}

const (
	ACCEPT_CONN_ESTABLISH_TIMEOUT = time.Second * 10
)

func (op *Op) Swap() {
	op.Local, op.Remote = op.Remote, op.Local
	op.LPort, op.RPort = op.RPort, op.LPort
}

func (op Op) String() string {
	return fmt.Sprintf(
		"OP{%d %s} {%s:%d<-%s:%d} (%d) bytes",
		op.Cmd,
		op.Cmd.String(),
		Uint2Host(op.Local), op.LPort,
		Uint2Host(op.Remote), op.RPort,
		len(op.Data.Bytes),
	)
}

type fwdLoc struct {
	Host uint64
	Port uint32
}

type discoverLoc struct {
	Host     uint64
	upstream *mpxRemote
}

type multiplexer struct {
	initCtl     sync.Once
	initDone    bool
	MaxDistance int

	cfg struct {
		Env      string
		LoopBack bool
		NoServer bool
		NoClient bool
	}

	streams map[uint32]*Stream
	sLock   sync.RWMutex

	binds  map[uint32]*Listener
	joined map[string]bool
	bLock  sync.RWMutex

	discovered map[discoverLoc]bool
	dLock      sync.RWMutex
	dNew       sync.Cond

	lhosts    map[string]bool
	lports    map[string]bool
	lAddrLock sync.Mutex
	lNew      sync.Cond

	gc    *time.Ticker
	httpc *http.Client

	routes   RegistryStorage
	services RegistryStorage
	rLock    sync.RWMutex

	lPort uint32
	local uint64
}

func (mpx *multiplexer) init() {
	mpx.initCtl.Do(func() {
		mpx.initDone = true
		mpx.gc = time.NewTicker(10 * time.Second)
		mpx.streams = make(map[uint32]*Stream)
		mpx.binds = make(map[uint32]*Listener)
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

		idLock.Lock()
		mpx.local = uint64(idGen.Int63())
		idLock.Unlock()

		mpx.iohandler()

		go mpx.farAwayLoop()
		go mpx.cleanupLoop()
		go mpx.routesWatcher()
		go mpx.serviceWatcher()
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
	var httpL, httpLErr = mpx.Bind("", "ipc:1")
	if httpLErr != nil {
		mpxLog.Panic(httpLErr)
	}
	if serveErr := http.Serve(httpL, nil); serveErr != nil {
		mpxLog.Panic(serveErr)
	}
}

func (mpx *multiplexer) dialTimeout(network, hp string, route *mpxRemote, t time.Duration) (*Stream, error) {
	mpx.init()
	var hostStr, portStr, hpSplitErr = net.SplitHostPort(hp)
	if hpSplitErr != nil {
		hostStr = hp
	}
	var port, _ = strconv.ParseUint(portStr, 10, 64)

	var vBucket = 0
	var algo RouteChooser = RandomChooser{}
	if netInfo, netErr := url.Parse(network); netErr == nil && netInfo.Scheme == "registry" {
		vBucket = int(crc32.ChecksumIEEE([]byte(network)))
		algo = SortedChooser{}
	}

	var host, hpErr = Host2Uint(hostStr)
	if hpErr != nil {
		var srv = mpx.services.DiscoverTimeout(algo, vBucket, hp+mpx.cfg.Env, t)
		if srv == nil {
			return nil, &net.AddrError{"Host not found", hp + mpx.cfg.Env}
		}
		host = srv.Host
		port = uint64(srv.Port)
	}

	var lPort = atomic.AddUint32(&mpx.lPort, 1)
	var stream = (&Stream{
		network: network,
		local:   mpx.local,
		remote:  host,
		lport:   uint32(lPort),
		rport:   uint32(port),
	}).init()
	mpx.sLock.Lock()
	mpx.streams[lPort] = stream
	mpx.sLock.Unlock()
	if route == nil {
		stream.mpx = mpx.findRouteTimeout(host, mpx.MaxDistance, t)
	} else {
		stream.mpx = route
	}
	if stream.mpx == nil {
		return nil, &net.AddrError{"Host connection failed", hp}
	}
	return stream, nil
}

func (mpx *multiplexer) dialSrv(network string, hp string, route *mpxRemote) (net.Conn, error) {
	var stream, err = mpx.dialTimeout(network, hp, route, ACCEPT_CONN_ESTABLISH_TIMEOUT)
	if err != nil {
		return nil, err
	}

	stream.SetDeadline(time.Now().Add(ACCEPT_CONN_ESTABLISH_TIMEOUT))
	stream.mpx.Send(stream.Op(OP_SYN, nil))
	stream.Join(OP_ACK)
	stream.SetDeadline(time.Time{})

	return stream, nil
}

func (mpx *multiplexer) DialTimeout(network string, hp string, t time.Duration) (net.Conn, error) {
	mpx.init()
	mpxLog.Println("dialing", network, hp, t)
	var stream, err = mpx.dialTimeout(network, hp, nil, t)
	if err != nil {
		return nil, err
	}

	var op = stream.Op(OP_NEW, nil)
	var timeouted bool
	var onTimeout = time.AfterFunc(t, func() {
		if t == 0 {
			return
		}
		stream.recv(stream.Op(OP_FORCE_CLOSED, nil))
		timeouted = true
	})

	mpxLog.Println("dialing", network, hp, t, op)
	var sendErr = stream.mpx.SendTimeout(op, t)
	if sendErr != nil {
		return nil, sendErr
	}
	stream.Join(OP_SYN_ACK)
	onTimeout.Stop()

	if timeouted {
		return nil, Dailimeout
	}

	return stream, nil
}

func (mpx *multiplexer) Dial(network string, hp string) (net.Conn, error) {
	mpx.init()
	return mpx.DialTimeout(network, hp, 0)
}

func (mpx *multiplexer) HttpDial(lnet string, laddr string) (net.Conn, error) {
	mpx.init()
	var host, port, hpErr = net.SplitHostPort(laddr)
	if hpErr != nil {
		return nil, hpErr
	}
	if port != "80" {
		host += ":" + port
	}
	return mpx.DialTimeout(lnet, host, 0)
}

func (mpx *multiplexer) sysBind(port uint32) (net.Listener, error) {
	var stream = (&Listener{
		network:  "",
		lport:    uint32(port),
		mpx:      mpx,
		hostname: "",
		hostport: "",
	}).init()

	mpx.bLock.Lock()
	mpx.binds[uint32(port)] = stream
	mpx.bLock.Unlock()

	return stream, nil
}

func (mpx *multiplexer) Bind(network string, hp string) (net.Listener, error) {
	mpx.init()
	var hostStr, portStr, hpSplitErr = net.SplitHostPort(hp)
	if hpSplitErr != nil {
		hostStr = hp
	}
	var port, _ = strconv.ParseUint(portStr, 10, 64)
	if port == 0 {
		port = uint64(atomic.AddUint32(&mpx.lPort, 1))
	}

	var stream = (&Listener{
		network:  network,
		lport:    uint32(port),
		mpx:      mpx,
		hostname: hostStr,
		hostport: hp,
		postfix:  mpx.cfg.Env,
	}).init()

	mpx.bLock.Lock()
	mpx.binds[uint32(port)] = stream
	mpx.bLock.Unlock()

	return stream, nil
}

func (mpx *multiplexer) discoverLoop(upstream *mpxRemote, distance int) {
	var discoveryMsg = func(id uint64, distance int) Op {
		var op = Op{
			Cmd:   OP_DISCOVER,
			Local: id,
		}
		op.Data.Bytes = []byte{byte(distance)}
		return op
	}
	var forgetMsg = func(id uint64, distance int) Op {
		var op = Op{
			Cmd:   OP_FORGET,
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

	var forEach RegistryStorage
	var iter = mpx.routes.Iter()
	for !upstream.IsClosed() {
		iter = iter.Next()
		forEach.Sync(&mpx.routes, func(s ServiceId) {
			if s.Priority+distance <= mpx.MaxDistance {
				go func(remote uint64) {
					upstream.SendTimeout(
						discoveryMsg(remote, s.Priority+distance),
						0,
					)
				}(s.Host)
			}
		}, func(s ServiceId) {
			if s.Priority+distance <= mpx.MaxDistance {
				go func(remote uint64) {
					upstream.SendTimeout(
						forgetMsg(remote, s.Priority+distance),
						0,
					)
				}(s.Host)
			}
		})
	}
}

func (mpx *multiplexer) discoverServiceLoop(upstream *mpxRemote, distance int) {
	var serviceMsg = func(id uint64, port uint32, name string) Op {
		var op = Op{
			Cmd:   OP_SERVICE,
			Local: id,
			LPort: port,
		}
		op.Data.Bytes = []byte(name)
		return op
	}
	var noServiceMsg = func(id uint64, port uint32, name string) Op {
		var op = Op{
			Cmd:   OP_NO_SERVICE,
			Local: id,
			LPort: port,
		}
		op.Data.Bytes = []byte(name)
		return op
	}

	if mpx.cfg.NoServer {
		return
	}

	var forEach RegistryStorage
	var iter = mpx.services.Iter()
	for !upstream.IsClosed() {
		iter = iter.Next()
		forEach.Sync(&mpx.services, func(s ServiceId) {
			if s.Host != mpx.local {
				return
			}
			go upstream.SendTimeout(
				serviceMsg(s.Host, s.Port, s.Service),
				0,
			)
		}, func(s ServiceId) {
			if s.Host != mpx.local {
				return
			}
			go upstream.SendTimeout(
				noServiceMsg(s.Host, s.Port, s.Service),
				0,
			)
		})
	}
}

func (mpx *multiplexer) routesWatcher() {
	var forEach RegistryStorage
	var iter = mpx.routes.Iter()

	for {
		iter = iter.Next()
		forEach.Sync(&mpx.routes, func(s ServiceId) {
			mpxStatLog.Println(fmt.Sprintf(
				"ADD ROUTE [%s -> %s] through %s {%d}",
				Uint2Host(mpx.local), Uint2Host(s.Host), s.Upstream, s.Priority,
			))
		}, func(s ServiceId) {
			mpxStatLog.Println(fmt.Sprintf(
				"DEL ROUTE [%s -> %s] through %s {%d}",
				Uint2Host(mpx.local), Uint2Host(s.Host), s.Upstream, s.Priority,
			))
		})
	}
}

func (mpx *multiplexer) serviceWatcher() {
	var forEach RegistryStorage
	var iter = mpx.services.Iter()

	for {
		iter = iter.Next()
		forEach.Sync(&mpx.services, func(s ServiceId) {
			mpxStatLog.Println(fmt.Sprintf(
				"ADD SERVICE %s [%s:%d]",
				s.Service, Uint2Host(s.Host), s.Port,
			))
		}, func(s ServiceId) {
			mpxStatLog.Println(fmt.Sprintf(
				"DEL SERVICE %s [%s:%d]",
				s.Service, Uint2Host(s.Host), s.Port,
			))
		})
	}
}

func (mpx *multiplexer) p2pNotifyLoop(upstream *mpxRemote, distance int) {
	var welcomeMsg = func(id uint64, name string) Op {
		var op = Op{
			Cmd:   OP_JOIN_ME,
			Local: id,
		}
		op.Data.Bytes = []byte(name)
		return op
	}
	if distance == 1 && upstream.rAddr != nil {
		var op = Op{
			Cmd:   OP_RHOST,
			Local: mpx.local,
		}
		op.Data.Bytes = []byte(upstream.rAddr.String())
		go upstream.Send(op)
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
					go upstream.Send(welcomeMsg(mpx.local, lhost+":"+lport))
					sent[loc] = true
				}
			}
		}
		skykiss.WaitTimeout(&mpx.lNew, time.Minute)
	}
	mpx.lAddrLock.Unlock()
}

func (mpx *multiplexer) Services() (services []ServiceId) {
	mpx.init()
	var forEach RegistryStorage
	forEach.Sync(&mpx.services, func(s ServiceId) {
		services = append(services, s)
	}, nil)
	return
}

func (mpx *multiplexer) ServiceMap() *RegistryStorage {
	mpx.init()
	return &mpx.services
}

func (mpx *multiplexer) Routes() (services []ServiceId) {
	mpx.init()
	var forEach RegistryStorage
	forEach.Sync(&mpx.routes, func(s ServiceId) {
		services = append(services, s)
	}, nil)
	return
}

func (mpx *multiplexer) RoutesMap() *RegistryStorage {
	mpx.init()
	return &mpx.routes
}

func (mpx *multiplexer) cleanupLoop() {
	for range mpx.gc.C {
		var nowConn = 0
		var streamCleanupList = []uint32{}
		var step1 = time.Now()
		mpx.sLock.RLock()
		for sId, stream := range mpx.streams {
			nowConn++
			stream.sLock.Lock()
			if stream.status&OP_CLOSED > 0 {
				nowConn--
				streamCleanupList = append(streamCleanupList, sId)
			}
			stream.sLock.Unlock()
		}
		mpx.sLock.RUnlock()
		var step2 = time.Now()
		for _, sId := range streamCleanupList {
			mpx.sLock.Lock()
			delete(mpx.streams, sId)
			mpx.sLock.Unlock()
		}
		var step3 = time.Now()

		mpxStatLog.Println(
			"gc", fmt.Sprintf("%s/%s/%s",
				step2.Sub(step1),
				step3.Sub(step2),
				step3.Sub(step1),
			),
			nowConn, "conns total",
			len(streamCleanupList), "conns closed",
		)
	}
}

func (mpx *multiplexer) attachDistance(conn io.ReadWriter, distance int) {
	var _, wg = mpx.attachDistanceNonBlock(conn, distance)
	wg.Wait()
}

func (mpx *multiplexer) attachDistanceNonBlock(conn io.ReadWriter, distance int) (*mpxRemote, *sync.WaitGroup) {
	mpx.init()
	var reader = bufio.NewReaderSize(conn, 64*1024)
	var writer = bufio.NewWriterSize(conn, 64*1024)
	var remote = &mpxRemote{reader: reader, writer: writer}
	if distance == 1 {
		remote.keepalive = time.Second * 10
	}
	remote.init()
	// var writer = bufio.NewWriterSize(printWrites(conn, 64*1024, false), 64*1024)

	if c, ok := conn.(net.Conn); ok {
		mpxLog.Println("Join", c.RemoteAddr().String())
		remote.lAddr = c.LocalAddr()
		remote.rAddr = c.RemoteAddr()
	}

	go mpx.discoverLoop(remote, distance)
	go mpx.discoverServiceLoop(remote, distance)
	go mpx.p2pNotifyLoop(remote, distance)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		var wErr = remote.IOLoopWriter()
		mpxLog.Println("Remote writer shutdown", wErr)
		wg.Done()
		if c, ok := conn.(io.Closer); ok {
			defer c.Close()
		}
		remote.Close()
	}()
	go func() {
		var rErr = mpx.IOLoopReader(remote)
		mpxLog.Println("Remote reader shutdown", rErr)
		wg.Done()
		if c, ok := conn.(io.Closer); ok {
			defer c.Close()
		}
		remote.Close()
	}()
	return remote, &wg
}

func (mpx *multiplexer) Attach(conn io.ReadWriter) {
	mpx.attachDistance(conn, 1)
}

func (mpx *multiplexer) publish(op Op) {
	var forEach RegistryStorage
	var wg sync.WaitGroup
	forEach.Sync(&mpx.routes, func(s ServiceId) {
		wg.Add(1)
		go func() {
			s.Upstream.SendTimeout(op, 0)
			wg.Done()
		}()
	}, nil)
	wg.Wait()
}

func (mpx *multiplexer) ListenAndServe(network, address string) error {
	mpx.init()
	mpxLog.Println("serving on", network, address)
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
	mpxLog.Println("join network at", network, address)
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
				mpxLog.Println("Could not join", network, address)
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

func (mpx *multiplexer) findRouteTimeout(remote uint64, maxDistance int, t time.Duration) (route *mpxRemote) {
	for distance := 0; distance <= maxDistance; distance++ {
		if service := mpx.routes.DiscoverTimeout(RandomChooser{}, 0, Uint2Host(remote), t); service != nil {
			return service.Upstream
		}
	}
	return nil
}

func (mpx *multiplexer) iohandler() {
	var fwdHandler, fwdHandlerErr = mpx.sysBind(0)
	if fwdHandlerErr != nil {
		panic(fwdHandlerErr)
	}
	go func() {
		for {
			var fwdConn, fwdConnErr = fwdHandler.Accept()
			if fwdConnErr != nil {
				mpxLog.Println("Fwd handler accept err", fwdConnErr)
				continue
			} else {
				mpxLog.Println("New connection from faraway host", fwdConn.RemoteAddr())
				go func() {
					defer fwdConn.Close()
					mpx.attachDistance(fwdConn, 3)
					mpxLog.Println("faraway host lost", fwdConn.RemoteAddr())
				}()
			}
		}
	}()
}

func (mpx *multiplexer) farAwayLoop() {
	var vHostDial = map[uint64]chan bool{}
	var vHostLoc = map[uint64]map[*mpxRemote]bool{}
	var vHostDialLock sync.Mutex

	mpx.dLock.Lock()
	for {
		var cleanup = []discoverLoc{}
		var upstreamCleanup = map[*mpxRemote]bool{}
		for loc := range mpx.discovered {
			if loc.upstream.IsClosed() {
				cleanup = append(cleanup, loc)
				upstreamCleanup[loc.upstream] = true
				continue
			}
			if vHostLoc[loc.Host] == nil {
				vHostLoc[loc.Host] = map[*mpxRemote]bool{}
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
					go func(vHost uint64, upstream *mpxRemote) {
						var route = mpx.findRouteTimeout(vHost, 1, time.Second)
						if route != nil {
							mpxStatLog.Println(
								"No more faraway connections allowed for",
								Uint2Host(vHost),
							)
							return
						}
						var remoteConn, remoteConnErr = mpx.dialTimeout("", Uint2Host(vHost)+":0", upstream, time.Second*10)
						upstream.SendTimeout(remoteConn.Op(OP_NEW, nil), 0)
						if remoteConnErr != nil {
							mpxStatLog.Println("Error while discovering", Uint2Host(vHost))
						} else {
							var remote, wg = mpx.attachDistanceNonBlock(remoteConn, 3)
							go func() {
								for !remote.IsClosed() {
									var route = mpx.findRouteTimeout(
										vHost, 1, time.Second,
									)
									if route != nil {
										remote.CloseIfIdle()
									}
									time.Sleep(time.Minute)
								}
							}()
							wg.Wait()
							remote.Close()
						}
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

func (mpx *multiplexer) discover(upstream *mpxRemote, host uint64, distance int) {
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

func (mpx *multiplexer) pushNew(op Op, upstream *mpxRemote) {
	var downstream = mpx.findRouteTimeout(op.Remote, mpx.MaxDistance, time.Second)
	if downstream == nil {
		mpxLog.Println("No route to", Uint2Host(op.Remote))
		return
	}

	var lBckwdPort = atomic.AddUint32(&mpx.lPort, 1)
	var bcwdStream = (&Stream{
		network: "",
		local:   mpx.local,
		remote:  op.Local,
		lport:   lBckwdPort,
		rport:   op.LPort,
		mpx:     upstream,
	}).init()

	mpx.sLock.Lock()
	mpx.streams[lBckwdPort] = bcwdStream
	mpx.sLock.Unlock()

	var lFwdPort = atomic.AddUint32(&mpx.lPort, 1)
	var fwdStream = (&Stream{
		network: "",
		local:   mpx.local,
		remote:  op.Remote,
		lport:   lFwdPort,
		rport:   op.RPort,
		mpx:     downstream,
	}).init()

	mpx.sLock.Lock()
	mpx.streams[lFwdPort] = fwdStream
	mpx.sLock.Unlock()

	go func() {
		bcwdStream.SetDeadline(time.Now().Add(ACCEPT_CONN_ESTABLISH_TIMEOUT))
		bcwdStream.mpx.Send(bcwdStream.Op(OP_SYN, nil))
		bcwdStream.Join(OP_ACK)
		bcwdStream.SetDeadline(time.Time{})

		io.Copy(fwdStream, bcwdStream)
		fwdStream.Close()
	}()

	go func() {
		var op = fwdStream.Op(OP_NEW, nil)
		var timeouted bool
		var onTimeout = time.AfterFunc(time.Second*10, func() {
			fwdStream.recv(fwdStream.Op(OP_FORCE_CLOSED, nil))
			timeouted = true
		})

		var sendErr = fwdStream.mpx.SendTimeout(op, 0)
		if sendErr != nil {
			mpxLog.Println("Upstream Send Err", sendErr)
			return
		}
		fwdStream.Join(OP_SYN_ACK)
		onTimeout.Stop()

		io.Copy(bcwdStream, fwdStream)
		bcwdStream.Close()
	}()

	return
}

func (mpx *multiplexer) IOLoopReader(upstream *mpxRemote) error {
	var r = upstream.reader
	var bEnc = binary.BigEndian
	var routes RegistryStorage
	var services RegistryStorage
	defer routes.Close()
	defer services.Close()

	for {
		var job Op
		var header, headerErr = r.Peek(29)
		if headerErr == nil {
			_, headerErr = r.Discard(29)
		}

		if headerErr != nil {
			return headerErr
		}

		job.Cmd = ConnState(header[0])
		job.Local = bEnc.Uint64(header[1:9])
		job.Remote = bEnc.Uint64(header[9:17])
		job.LPort = bEnc.Uint32(header[17:21])
		job.RPort = bEnc.Uint32(header[21:25])
		var bLen = int(bEnc.Uint32(header[25:29]))
		var dataErr error
		job.Data.Bytes, dataErr = r.Peek(bLen)
		if dataErr == nil {
			_, dataErr = r.Discard(bLen)
		}
		if dataErr != nil {
			return dataErr
		}

		upstream.dLock.Lock()
		upstream.lastOP = time.Now()
		upstream.dLock.Unlock()

		//mpxLog.Println(job)
		switch job.Cmd {
		case OP_NO_OP:
			continue
		case OP_JOIN_ME:
			if !mpx.cfg.NoClient {
				mpxStatLog.Println("OP_JOINME", "tcp4", string(job.Data.Bytes))
				go mpx.Join("tcp4", string(job.Data.Bytes))
			}
			continue
		case OP_DISCOVER, OP_FORGET:
			var distance = int(byte(job.Data.Bytes[0]))
			var service = ServiceId{
				Service:  Uint2Host(job.Remote),
				Host:     job.Remote,
				Priority: distance,
				Upstream: upstream,
			}
			if service.Host == mpx.local && distance > 0 {
				continue
			}
			if job.Cmd == OP_DISCOVER {
				if distance == 2 {
					go mpx.discover(upstream, service.Host, distance)
				} else {
					mpx.routes.Push(service)
					routes.Push(service, func() {
						mpx.routes.Pop(service)
					})
				}
			} else {
				routes.Pop(service)
			}
			continue
		case OP_SERVICE, OP_NO_SERVICE:
			var sName = string(job.Data.Bytes)
			var service = ServiceId{
				Service:  sName,
				Host:     job.Remote,
				Port:     job.RPort,
				Upstream: upstream,
			}
			if service.Host == mpx.local {
				continue
			}
			if job.Cmd == OP_SERVICE {
				mpx.services.Push(service)
				services.Push(service, func() {
					mpx.services.Pop(service)
				})
			} else {
				services.Pop(service)
			}
			continue
		case OP_RHOST:
			var sName = string(job.Data.Bytes)
			var host, _, hpErr = net.SplitHostPort(sName)
			if hpErr != nil {
				mpxStatLog.Println("Broken OP_RHOST command", sName)
				continue
			}
			mpx.lAddrLock.Lock()
			if !mpx.lhosts[host] {
				mpx.lhosts[host] = true
				mpx.lNew.Broadcast()
			}
			mpx.lAddrLock.Unlock()
			continue
		}

		if job.Local != mpx.local {
			job.Swap()
			switch job.Cmd {
			case OP_NEW:
				go mpx.pushNew(job, upstream)
			default:
				mpxLog.Println("dropping forward frame")
			}
			continue
		}

		switch job.Cmd {
		case OP_NEW:
			mpx.bLock.RLock()
			var listener = mpx.binds[job.LPort]
			mpx.bLock.RUnlock()
			job.Route = upstream
			if listener != nil {
				listener.recv(job)
			} else {
				mpxStatLog.Println("unknown bind", job.LPort)
				return errors.New("unknown bind")
			}
		case OP_SYN:
			mpx.sLock.RLock()
			var stream = mpx.streams[job.LPort]
			mpx.sLock.RUnlock()
			if stream != nil {
				stream.mpx = upstream
				stream.rport = job.RPort
				stream.remote = job.Remote
				stream.recv(job)
			} else {
				mpxStatLog.Println("unknown stream", job.LPort)
				return errors.New("unknown stream")
			}
		case OP_ACK, OP_SYN_ACK, OP_CLOSED:
			mpx.sLock.RLock()
			var stream = mpx.streams[job.LPort]
			mpx.sLock.RUnlock()
			if stream != nil {
				stream.recv(job)
			} else {
				mpxStatLog.Println("unknown stream", job.LPort)
				return errors.New("unknown stream")
			}
		case OP_FIN2, OP_DATA, OP_REWIND, OP_WND_SIZE:
			mpx.sLock.RLock()
			var stream = mpx.streams[job.LPort]
			mpx.sLock.RUnlock()
			if stream != nil {
				stream.data(job)
			}
		default:
			mpxStatLog.Println("unknown op", job.Cmd)
			continue
		}
	}
}

var machineNs = skykiss.NewV1()
var idGen = rand.NewSource(int64(crc64.Checksum(machineNs.Bytes(), crc64.MakeTable(crc64.ECMA))))
var idLock sync.Mutex

func New() AstraNet {
	return (&multiplexer{}).New()
}
