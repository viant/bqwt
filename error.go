package bqwt

import (
	"fmt"
	"strings"
)

//NotFoundError represents not found error
type NotFoundError struct {
	URL string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("not found: %v", e.URL)
}

//IsNotFoundError checks is supplied error is NotFoundError type
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*NotFoundError)
	return ok
}

func reclassifyNotFoundIfMatched(err error, URL string) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "doesn't exist") {
		return &NotFoundError{URL: URL}
	}
	return err
}
