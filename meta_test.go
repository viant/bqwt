package bqwt

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMeta_Update(t *testing.T) {

	inThePast := time.Now().Add(-time.Hour)
	now := time.Now()
	inTheFuture := time.Now().Add(time.Hour)
	from := inThePast.UnixNano() / int64(time.Millisecond)

	to := now.UnixNano() / int64(time.Millisecond)
	toFuture := inTheFuture.UnixNano() / int64(time.Millisecond)

	useCase2Meta := NewMeta("gs://bucket/meta.json", "dataset")
	useCase2Meta.Update(NewTableInfo("dataset", "tableX", inThePast, now), now)

	var useCases = []struct {
		description string
		tableInfo   *TableInfo
		meta        *Meta
		expected    *WindowedTable
	}{
		{
			description: "new table",
			meta:        NewMeta("gs://bucket/meta.json", "dataset"),
			tableInfo:   NewTableInfo("dataset", "tableX", inThePast, now),
			expected: &WindowedTable{
				ID:                 "dataset.tableX",
				Name:               "tableX",
				Dataset:            "dataset",
				Expression:         fmt.Sprintf("[dataset.tableX@%v-%v]", from, to),
				AbsoluteExpression: fmt.Sprintf("[dataset.tableX@%v-%v]", from, to),

				Window: &TimeWindow{
					From: inThePast,
					To:   now,
				},
				LastChanged: now,

				Changed: true,
			},
		},

		{
			description: "existing table no update",
			meta:        useCase2Meta,
			tableInfo:   NewTableInfo("dataset", "tableX", inThePast, now),
			expected: &WindowedTable{
				ID:                 "dataset.tableX",
				Name:               "tableX",
				Dataset:            "dataset",
				Expression:         fmt.Sprintf("[dataset.tableX@%v-%v]", from, to),
				AbsoluteExpression: fmt.Sprintf("[dataset.tableX@%v-%v]", from, to),
				Window: &TimeWindow{
					From: inThePast,
					To:   now,
				},
				LastChanged: now,

				Changed: false,
			},
		},

		{
			description: "existing table changing update",
			meta:        useCase2Meta,
			tableInfo:   NewTableInfo("dataset", "tableX", now, inTheFuture),
			expected: &WindowedTable{
				ID:                 "dataset.tableX",
				Name:               "tableX",
				Dataset:            "dataset",
				Expression:         fmt.Sprintf("[dataset.tableX@%v-%v]", to+1, toFuture),
				AbsoluteExpression: fmt.Sprintf("[dataset.tableX@%v-%v]", to+1, toFuture),

				Window: &TimeWindow{
					From: now.Add(time.Millisecond),
					To:   inTheFuture,
				},
				LastChanged: now,
				Changed:     true,
			},
		},
	}

	for _, useCase := range useCases {
		useCase.meta.expressions = make([]string, 0)
		updated := useCase.meta.Update(useCase.tableInfo, now)
		assert.Equal(t, useCase.expected, updated, useCase.description)
		if useCase.expected.Changed {
			assert.Equal(t, 1, len(useCase.meta.expressions), useCase.description)
		}
	}

}

func TestMeta_Prune(t *testing.T) {

	agesAgo := time.Now().Add(-time.Hour * 120)
	inThePast := time.Now().Add(-24 * time.Hour)
	now := time.Now()

	meta := NewMeta("gs://bucket/meta.json", "dataset")
	meta.Update(NewTableInfo("dataset", "tableX", inThePast, now), now)
	meta.Update(NewTableInfo("dataset", "tableY", agesAgo, inThePast), inThePast)
	{ //no prune
		meta.Prune(0, now)
		assert.Equal(t, 2, len(meta.Tables))
	}
	{ //no prune
		meta.Prune(time.Hour, now)
		assert.Equal(t, 1, len(meta.Tables))
		assert.Equal(t, "tableX", meta.Tables[0].Name)
	}

}
