package bqwt

import (
	"fmt"
	"github.com/viant/toolbox"
	"net/http"
	"strings"
)

type FormFields struct {
	Meta         string
	Dataset      string
	Mode         string
	Match        string
	Location     string
	Expr         string
	Loopback     string
	Prune        string
	AbsoluteExpr string
}

func (f *FormFields) Validate() error {
	if f.Meta == "" {
		return fmt.Errorf("meta wass empty")
	}
	if f.Dataset == "" {
		return fmt.Errorf("dataset wass empty")
	}
	return nil
}

func (f *FormFields) AsRequest() (*Request, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			JSON, _ := toolbox.AsJSONText(f)
			err = fmt.Errorf("failed to create request from %v %v", JSON, r)
		}
	}()
	request := &Request{}
	request.MetaURL = f.Meta
	request.DatasetID = f.Dataset
	request.Mode = f.Mode
	request.MatchingTables = strings.Split(f.Match, ",")
	request.Location = f.Location

	request.Expression = toolbox.AsBoolean(f.Expr)
	request.AbsoluteExpression = toolbox.AsBoolean(f.AbsoluteExpr)

	if f.Loopback != "" {
		if request.LoopbackWindowInSec, err = toolbox.ToInt(f.Loopback); err != nil {
			return nil, fmt.Errorf("invalid loopback %v, %v", f.Loopback, err)
		}
	}
	if f.Prune != "" {
		if request.PruneThresholdInSec, err = toolbox.ToInt(f.Prune); err != nil {
			return nil, fmt.Errorf("invalid loopback %v, %v", f.Prune, err)
		}
	}
	return request, err
}

func NewFormFields(request *http.Request) (*FormFields, error) {
	if err := request.ParseForm(); err != nil {
		return nil, err
	}
	if len(request.Form) == 0 {
		return nil, fmt.Errorf("form field were empty")
	}
	return &FormFields{
		Meta:         request.Form.Get("meta"),
		Dataset:      request.Form.Get("dataset"),
		Mode:         request.Form.Get("mode"),
		Match:        request.Form.Get("match"),
		Location:     request.Form.Get("location"),
		Expr:         request.Form.Get("expr"),
		Loopback:     request.Form.Get("loopback"),
		Prune:        request.Form.Get("prune"),
		AbsoluteExpr: request.Form.Get("absExpr"),
	}, nil
}
