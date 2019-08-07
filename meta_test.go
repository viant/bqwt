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
	useCase2Meta.Update(NewTableInfo("p", "dataset", "tableX", inThePast, now), now)

	var useCases = []struct {
		description string
		tableInfo   *TableInfo
		meta        *Meta
		expected    *WindowedTable
	}{
		{
			description: "new table",
			meta:        NewMeta("gs://bucket/meta.json", "dataset"),
			tableInfo:   NewTableInfo("p", "dataset", "tableX", inThePast, now),
			expected: &WindowedTable{
				ID:                 "dataset.tableX",
				ProjectID:          "p",
				Name:               "tableX",
				Dataset:            "dataset",
				Expression:         fmt.Sprintf("[dataset.tableX@%v-%v]", from, to),
				AbsoluteExpression: fmt.Sprintf("[p:dataset.tableX@%v-%v]", from, to),

				Window: &TimeWindow{
					From: inThePast,
					To:   now,
				},
				LastChanged: now,
				Changed:     true,
			},
		},

		{
			description: "existing table no update",
			meta:        useCase2Meta,
			tableInfo:   NewTableInfo("p", "dataset", "tableX", inThePast, now),
			expected: &WindowedTable{
				ID:                 "dataset.tableX",
				ProjectID:          "p",
				Name:               "tableX",
				Dataset:            "dataset",
				Expression:         fmt.Sprintf("[dataset.tableX@%v-%v]", from, to),
				AbsoluteExpression: fmt.Sprintf("[p:dataset.tableX@%v-%v]", from, to),
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
			tableInfo:   NewTableInfo("p", "dataset", "tableX", now, inTheFuture),
			expected: &WindowedTable{
				ID:                 "dataset.tableX",
				ProjectID:          "p",
				Name:               "tableX",
				Dataset:            "dataset",
				Expression:         fmt.Sprintf("[dataset.tableX@%v-%v]", to+1, toFuture),
				AbsoluteExpression: fmt.Sprintf("[p:dataset.tableX@%v-%v]", to+1, toFuture),

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

		updated := useCase.meta.Update(useCase.tableInfo, now)
		assert.Equal(t, useCase.expected, updated, useCase.description)
		assert.True(t, updated.Expression != "", useCase.description)

	}

}

func TestMeta_Prune(t *testing.T) {

	agesAgo := time.Now().Add(-time.Hour * 120)
	inThePast := time.Now().Add(-24 * time.Hour)
	now := time.Now()

	meta := NewMeta("gs://bucket/meta.json", "dataset")
	meta.Update(NewTableInfo("p", "dataset", "tableX", inThePast, now), now)
	meta.Update(NewTableInfo("p", "dataset", "tableY", agesAgo, inThePast), inThePast)
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

func TestMeta_SortLastModifiedDesc(t *testing.T) {
	agesAgo := time.Now().Add(-time.Hour * 120)
	t0 := time.Now().Add(-3 * time.Hour)
	t1 := time.Now().Add(-2 * time.Hour)
	t2 := time.Now().Add(-1 * time.Hour)
	t3 := time.Now()

	meta := NewMeta("gs://bucket/meta.json", "dataset")
	meta.Update(NewTableInfo("p", "dataset", "table2", agesAgo, t2), t2)
	meta.Update(NewTableInfo("p", "dataset", "table0", agesAgo, t0), t0)
	meta.Update(NewTableInfo("p", "dataset", "table3", agesAgo, t3), t3)
	meta.Update(NewTableInfo("p", "dataset", "table1", agesAgo, t1), t1)

	assert.Equal(t, meta.Tables[0].Name, "table2")

	meta.SortLastModifiedDesc()

	assert.Equal(t, meta.Tables[0].Name, "table3")
}
