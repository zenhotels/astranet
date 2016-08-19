package main

import (
	"fmt"
	"io"
	"net"

	"os"
	"time"

	"github.com/zenhotels/astranet"
)

func echoHandler(conn net.Conn) (n int64, err error) {
	return io.Copy(conn, conn)
}

func echoLoop(listener net.Listener) error {
	for {
		var echoConn, lstErr = listener.Accept()
		if lstErr != nil {
			fmt.Println("echoLoop", lstErr)
			return lstErr
		}

		go func() {
			fmt.Println("New server-side echo connection")
			var n, ehErr = echoHandler(echoConn)
			echoConn.Close()
			fmt.Println("New server-side echo connection closed with ", n, ehErr)
		}()
	}
}

func main() {
	var astraNet = astranet.New().WithEnv("test").Server()
	go astraNet.ListenAndServe("tcp4", ":14000")
	os.Setenv("MPXROUTER", "localhost:10000")

	var astraNet2 = astranet.New().WithEnv("test", "skynet").Server()

	var l2, l2Err = astraNet2.Bind("", "skynet2")
	if l2Err != nil {
		panic(l2Err)
	}
	go echoLoop(l2)

	var astraNet3 = astranet.New().WithEnv("test", "skynet").Client()

	var l3, l3Err = astraNet3.Bind("", "skynet3")
	if l3Err != nil {
		panic(l3Err)
	}
	go echoLoop(l3)

	for {
		var echoReq, dialErr = astraNet3.Dial("", "skynet2")
		if dialErr != nil {
			panic(dialErr)
		}

		go func() {
			echoReq.Write([]byte("Hello!"))
			echoReq.Close()
		}()
		var n, rErr = io.Copy(os.Stderr, echoReq)
		fmt.Println("received", n, rErr, "closed with", echoReq.Close())
		time.Sleep(time.Second)
	}
}
