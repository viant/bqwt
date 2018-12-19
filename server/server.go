package main

import (
	"fmt"
	"github.com/viant/bqwt"
	"net/http"
	"os"
)

func main() {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("HOME")+"/.secret/viant-e2e32.json")

	//ctx := context.Background()
	//
	//err := bqwt.DeleteGSObject(ctx , "gs://aw_test/meta.json-tmp")
	//fmt.Printf("%v\n", err)

	StartServer(8080, bqwt.Handle)
}

//StartServer starts http request, the server has ability to replay recorded  HTTP trips with https://github.com/viant/toolbox/blob/master/bridge/http_bridge_recording_util.go#L82
func StartServer(port int, handler http.HandlerFunc) error {
	httpServer := &http.Server{Addr: fmt.Sprintf(":%v", port), Handler: handler}
	fmt.Printf("Starting server on %v\n", port)
	return httpServer.ListenAndServe()
}
