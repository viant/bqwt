package main

import (
	"flag"
	"fmt"
	"github.com/prometheus/common/log"
	"github.com/viant/bqwt"
	"net/http"
)

var port = flag.String("port", "8080", "endpoint port")

func main() {
	flag.Parse()
	err := StartServer(*port, bqwt.Handle)
	if err != nil {
		log.Fatal(err)
	}
}

//StartServer start service
func StartServer(port string, handler http.HandlerFunc) error {
	httpServer := &http.Server{Addr: ":" + port, Handler: handler}
	fmt.Printf("Starting server on %v\n", port)
	return httpServer.ListenAndServe()
}
