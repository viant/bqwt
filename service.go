package bqwt

import (
	"bytes"
	"context"
	"github.com/gin-gonic/gin/json"
	"strings"
	"time"
)

//Service represents time windowed table service
type Service interface {
	//Handle retrieves windowed table from meta file and merges it with table info details
	Handle(*Request) *Response
}

type service struct{}

func (s *service) getTablesInfo(ctx context.Context, request *Request) ([]*TableInfo, error) {
	projectID, err := getProjectID(request.DatasetID)
	if err != nil {
		return nil, err
	}
	loopBackTime := time.Now().Add(-time.Second * time.Duration(request.LoopbackWindowInSec))
	return GetTablesInfo(ctx, projectID, request.DatasetID, request.Location, loopBackTime, request.MatchingTables)
}

func (s *service) loadMetaFile(ctx context.Context, URL, datasetID string) (*Meta, error) {
	var result = NewMeta(URL, datasetID)
	tempURL := getTempURL(URL)
	content, err := DownloadGSContent(ctx, tempURL)
	if err != nil {
		if IsNotFoundError(err) {
			if content, err = DownloadGSContent(ctx, URL); err == nil {
				err = json.NewDecoder(bytes.NewReader(content)).Decode(result)
			}
			return result, nil
		}
		return nil, err
	}
	err = json.NewDecoder(bytes.NewReader(content)).Decode(result)
	result.isTemp = true
	return result, err
}

func (s *service) Handle(request *Request) *Response {
	response := &Response{}
	err := request.Init()
	if err == nil {
		err = request.Validate()
	}
	if response.SetErrorIfNeeded(err) {
		return response
	}
	ctx := context.Background()
	var tablesInfo []*TableInfo
	response.Meta, err = s.loadMetaFile(ctx, request.MetaURL, request.DatasetID)
	if err == nil && !response.Meta.isTemp && request.IsRead() {
		tablesInfo, err = s.getTablesInfo(ctx, request)
	}
	if response.SetErrorIfNeeded(err) {
		return response
	}
	now := time.Now()
	defer func() {
		err = s.processMeta(ctx, response.Meta, request, now)
		response.SetErrorIfNeeded(err)
	}()
	if len(tablesInfo) == 0 {
		return response
	}

	for _, info := range tablesInfo {
		response.Meta.Update(info, now)
	}
	return response
}

func (s *service) processMeta(ctx context.Context, meta *Meta, request *Request, now time.Time) error {
	pruneThreshold := time.Duration(request.PruneThresholdInSec) * time.Second
	meta.Prune(pruneThreshold, now)
	meta.Expression = strings.Join(meta.expressions, ",")
	meta.AbsoluteExpression = strings.Join(meta.absoluteExpressions, ",")

	var err error
	switch request.Mode {
	case "r":
		URL := getTempURL(request.MetaURL)
		return s.uploadMeta(ctx, URL, meta)
	case "rw", "w":
		err = s.uploadMeta(ctx, request.MetaURL, meta)
		if err == nil {
			readURL := getTempURL(request.MetaURL)
			if ExistsGSObject(ctx, readURL) {
				err = DeleteGSObject(ctx, readURL)
			}
		}
	}
	return err
}

func (s *service) uploadMeta(ctx context.Context, URL string, meta *Meta) error {
	if URL == "" {
		return nil
	}
	data, err := json.Marshal(meta)
	if err == nil {
		err = UploadGSContent(ctx, URL, bytes.NewReader(data))
	}
	return err
}

//New creates a new windowed table service
func New() Service {
	return &service{}
}
