package bqwt

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/toolbox/cred"
	"github.com/viant/toolbox/secret"
	"net/http"
	"os"
	"strings"
	"time"
)

func loadCredentials(location string) (*cred.Config, error) {
	if location == "" {
		location = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	}
	home := os.Getenv("HOME")
	secretService := secret.New(home, false)
	return secretService.CredentialsFromLocation(location)
}

func getProjectID(datasetID string) (string, error) {
	datasetFragments := strings.SplitN(datasetID, ":", 2)
	if len(datasetFragments) == 2 {
		return datasetFragments[0], nil
	}
	credConfig, err := loadCredentials("")
	if err != nil {
		return "", err
	}
	return credConfig.ProjectID, nil
}

func getSchemaName(datasetID string) string {
	datasetFragments := strings.SplitN(datasetID, ":", 2)
	if len(datasetFragments) == 2 {
		return datasetFragments[1]
	}
	return ""
}

// GetTablesInfo returns table info for supplied dataset
func GetTablesInfo(ctx context.Context, request *Request, lastModified bool) ([]*TableInfo, error) {
	projectID, err := getProjectID(request.DatasetID)
	if err != nil {
		return nil, err
	}

	//schema is same as dataset
	schema := getSchemaName(request.DatasetID)
	if schema == "" {
		return nil, fmt.Errorf("dataset schema field is empty")
	}

	SQL := ""
	useLegacy := true
	if !lastModified {
		loopBackTime := time.Now().Add(-time.Second * time.Duration(request.LoopbackWindowInSec))
		SQL = fmt.Sprintf(TableInfoLegacySQL, request.DatasetID, loopBackTime.Unix()*1000)
		if request.StorageRegion != "" {
			useLegacy = false
			SQL = fmt.Sprintf(TableInfoStandardSQL, request.StorageRegion, loopBackTime.Unix()*1000, schema)
		}
	} else {
		SQL = fmt.Sprintf(LastModifiedTableLegacySQL, request.DatasetID)
		if request.StorageRegion != "" {
			useLegacy = false
			SQL = fmt.Sprintf(LastModifiedTableStandardSQL, request.StorageRegion, schema)
		}
	}

	return GetTablesInfoFromSQL(ctx, projectID, request.Location, SQL, useLegacy)

}

func GetTablesInfoFromSQL(ctx context.Context, projectID, datasetLocation, SQL string, useLegacy bool) ([]*TableInfo, error) {
	var err error
	var result = make([]*TableInfo, 0)

	if err = RunBQQuery(ctx, projectID, datasetLocation, SQL, []interface{}{}, useLegacy, func(row []bigquery.Value) (b bool, e error) {
		tableName := AsString(row[1])
		info := &TableInfo{
			ProjectID: projectID,
			DatasetID: AsString(row[0]),
			TableID:   tableName,
		}
		if info.Created, err = AsTime(row[2]); err != nil {
			return false, err
		}
		if info.LastModified, err = AsTime(row[3]); err != nil {
			return false, err
		}
		result = append(result, info)
		return true, nil
	}); err != nil {
		return nil, err
	}

	return result, err
}

func getTempURL(URL string) string {
	return URL + "-tmp"
}

func buildRequest(httpRequest *http.Request) (*Request, error) {
	request := &Request{}
	if httpRequest.ContentLength > 0 {
		return request, json.NewDecoder(httpRequest.Body).Decode(request)
	}
	formFields, err := NewFormFields(httpRequest)
	if err == nil {
		err = formFields.Validate()
	}
	if err != nil {
		return nil, err
	}

	return formFields.AsRequest()
}

func HandleRequest(w http.ResponseWriter, r *http.Request) {
	request, err := buildRequest(r)
	if err != nil {
		handleError(err, w)
		return
	}
	srv := New()
	response := srv.Handle(request)
	if response.Error != "" {
		handleError(response.Error, w)
		return
	}
	if request.AbsoluteExpression {
		_, err = fmt.Fprint(w, response.Meta.AbsoluteExpression)
	} else if request.Expression {
		_, err = fmt.Fprint(w, response.Meta.Expression)
	} else {
		err = json.NewEncoder(w).Encode(response)
	}
	if err != nil {
		handleError(err, w)
	}
}

func handleError(err interface{}, w http.ResponseWriter) {
	http.Error(w, fmt.Sprintf("Error %v", err), http.StatusInternalServerError)
}
