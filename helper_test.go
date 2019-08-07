package bqwt

import (
	"context"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bgc"
	"github.com/viant/dsunit"
	"testing"
	"time"
)

func TestGetTableInfo(t *testing.T) {
	ctx := context.Background()
	loopBackTime := time.Now().Add(-(time.Hour * 7 * 24))
	projectID, _ := getProjectID("")
	if dsunit.InitFromURL(t, "test/config/init.yaml") {
		if !dsunit.PrepareFor(t, "testdb", "test/data", "get_table_info") {
			return
		}
	}

	info, err := GetTablesInfo(ctx, projectID, "testdb", "", loopBackTime)
	assert.Nil(t, err)
	if !assert.True(t, len(info) > 0) {
		return
	}
	assert.Equal(t, "dummy", info[0].TableID)
	assert.Equal(t, "testdb", info[0].DatasetID)

}

func TestGetTableInfoLastModified(t *testing.T) {
	ctx := context.Background()
	projectID, _ := getProjectID("")
	if dsunit.InitFromURL(t, "test/config/init.yaml") {
		if !dsunit.PrepareFor(t, "testdb", "test/data", "get_table_info") {
			return
		}
	}
	time.Sleep(1 * time.Second)
	if dsunit.InitFromURL(t, "test/config/init2.yaml") {
		if !dsunit.PrepareFor(t, "testdb", "test/data", "get_table_info2") {
			return
		}
	}

	info, err := GetLastModifiedTableInfo(ctx, projectID, "testdb", "")
	assert.Nil(t, err)
	if !assert.True(t, len(info) == 1) {
		return
	}
	assert.Equal(t, "dummy2", info[0].TableID)
	assert.Equal(t, "testdb", info[0].DatasetID)

}
