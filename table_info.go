package bqwt

import "time"

//TableInfo represents table info
type TableInfo struct {
	DatasetID    string
	ProjectID    string
	TableID      string
	Created      time.Time
	LastModified time.Time
}

func NewTableInfo(datasetID string, tableID string, created time.Time, lastModified time.Time) *TableInfo {
	return &TableInfo{
		DatasetID:    datasetID,
		TableID:      tableID,
		Created:      created,
		LastModified: lastModified,
	}
}
