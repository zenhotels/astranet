package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/zenhotels/astranet"
)

func main() {
	var astraNet1 = astranet.New().Server()
	var astraNet2 = astranet.New().Client()

	var client, server IOLoop
	server.Reader, client.Writer = io.Pipe()
	client.Reader, server.Writer = io.Pipe()

	// Use IOLoops as a pipe that conforms io.ReadWriter on the both ends
	go astraNet1.Attach(client)
	go astraNet2.Attach(server)

	// Bind a server for the "hello" service
	l, err := astraNet1.Bind("", "hello")
	if err != nil {
		log.Fatalln(err)
	}
	go func() {
		conn, err := l.Accept()
		for err == nil {
			// For the each connected client we produce a message and close the connection
			fmt.Fprintf(conn, "Hello! Time: %s", time.Now().Format(time.Kitchen))
			conn.Close()
			conn, err = l.Accept()
		}
		if err != nil {
			log.Println("[accept ERR]", err)
		}
	}()

	// Now connect to the service and read the reply
	conn, err := astraNet2.Dial("", "hello")
	if err != nil {
		log.Fatalln("[dial ERR]", err)
	}
	body := new(bytes.Buffer)
	io.Copy(body, conn)

	log.Printf("Server says: %s", body.String())
}

// IOLoop is the same as the astranet.IOLoop, but copied here for clarity.
type IOLoop struct {
	io.Reader
	io.Writer
}
