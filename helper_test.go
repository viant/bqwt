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
	if dsunit.InitFromURL(t, "test/config/init.yaml") {
		if !dsunit.PrepareFor(t, "testdb", "test/data", "get_table_info") {
			return
		}
	}

	request := &Request{
		DatasetID:     "viant-e2e:testdb",
		StorageRegion: "viant-e2e.us-region",
	}
	info, err := GetTablesInfo(ctx, request)

	assert.Nil(t, err)
	if !assert.True(t, len(info) > 0) {
		return
	}
	assert.Equal(t, "dummy", info[0].TableID)
	assert.Equal(t, "testdb", info[0].DatasetID)

}

func TestGetTableInfoLastModified(t *testing.T) {
	ctx := context.Background()
	request := &Request{
		DatasetID:     "viant-e2e:testdb",
		StorageRegion: "viant-e2e.us-region",
	}

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

	info, err := GetTablesInfo(ctx, request)
	assert.Nil(t, err)
	if !assert.True(t, len(info) == 1) {
		return
	}
	assert.Equal(t, "dummy2", info[0].TableID)
	assert.Equal(t, "testdb", info[0].DatasetID)

}
