package bqwt

import (
	"log"
	"os"
)

func init() {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("HOME")+"/.secret/viant-e2e.json"); err != nil {
			log.Fatal(err)
		}
	}
}
