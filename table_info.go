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

//NewTableInfo creates a new table info
func NewTableInfo(projectId, datasetID string, tableID string, created time.Time, lastModified time.Time) *TableInfo {
	return &TableInfo{
		ProjectID:    projectId,
		DatasetID:    datasetID,
		TableID:      tableID,
		Created:      created,
		LastModified: lastModified,
	}
}
