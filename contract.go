package bqwt

import (
	"fmt"
	"strings"
)

//Request represents a request for windowed tables
type Request struct {
	Mode                string   `description:"operation mode: r - take snapshot, w - persist snapshot"`
	MetaURL             string   `description:"meta-file location, if relative path is used it adds gs:// protocol"`
	Location            string   `description:"dataset location"`
	DatasetID           string   `description:"source dataset"`
	MatchingTables      []string `description:"matching table contain expression"`
	PruneThresholdInSec int      `description:"max allowed duration in sec for unchanged windowed tables before removing"`
	LoopbackWindowInSec int      `description:"dataset max loopback window for checking changed tables in supplied dataset"`
	Expression          bool     `description:"if expression flag is set it returns only relative expression (without poejct id)"`
	AbsoluteExpression  bool     `description:"if expression flag is set it returns only abslute  expression (with poejct id)"`
	Method              string   `description:"data insert method: stream or load by default"`
}

func (r Request) IsRead() bool {
	return strings.HasPrefix(r.Mode, "r")
}

//Init initializes request
func (r *Request) Init() error {
	if r.PruneThresholdInSec == 0 {
		r.PruneThresholdInSec = 7 * 60 * 60 //7 days
	}

	if r.LoopbackWindowInSec == 0 {
		r.LoopbackWindowInSec = 3 * 60 * 60 //3 days
	}
	if r.Location == "" {
		r.Location = "US"
	}
	if r.Mode == "" {
		r.Mode = "rw"
	}
	if r.MetaURL != "" && !strings.Contains(r.MetaURL, "://") {
		r.MetaURL = "gs://" + r.MetaURL
	}
	if r.Method == "" {
		r.Method = "load"
	}
	return nil
}

//Validate check if request is valid
func (r *Request) Validate() error {
	if r.MetaURL == "" {
		return fmt.Errorf("SourceMetaURL was empty")
	}

	if r.DatasetID == "" {
		return fmt.Errorf("DatasetID was empty")
	}

	switch r.Mode {
	case "r", "w", "rw":
	default:
		return fmt.Errorf("unsupported mode: " + r.Mode)
	}
	return nil
}

//Response represents a windowed table response
type Response struct {
	Status string
	Error  string
	Meta   *Meta
}

func (r *Response) SetErrorIfNeeded(err error) bool {
	if err == nil {
		return false
	}
	r.Error = err.Error()
	r.Status = "error"
	return true
}
