package bqwt

import (
	"net/http"
)

// Handle windowed tables for supplied datasets, and meta file URL
func Handle(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			// error handling code, e.g., send the contents of `r` to
			// Stackdriver Error via the Cloud client library
			handleError(r, w)
		}
	}()
	HandleRequest(w, r)
}
