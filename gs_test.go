package bqwt

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetGSContent(t *testing.T) {

	{
		ctx := context.Background()
		_, err := DownloadGSContent(ctx, "gs://somebucket/blach.json")
		assert.NotNil(t, err)
	}

}
