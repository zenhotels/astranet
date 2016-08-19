package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/zenhotels/astranet"
	"fmt"
)

var name = flag.String("name", "anonym", "my name to send messages for")

func newMsg(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	fmt.Println(r.Form.Get("msg"))
}

func main() {
	flag.Parse()

	var astraNet = astranet.New().Server().WithEnv(*name, "chat")
	var skyL, lErr = astraNet.Bind("", "http-api")
	if lErr != nil {
		log.Panic(lErr)
	}
	astraNet.ListenAndServe("tcp4", "0.0.0.0:20000")

	http.HandleFunc("/send", newMsg)
	http.Serve(skyL, nil)
}
