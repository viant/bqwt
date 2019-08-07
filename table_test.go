package bqwt

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"testing"
	"time"
)

func TestNewWindowedTable(t *testing.T) {

	{ //from to withing a week case

		now := time.Now()
		nineDaysAgo, _ := toolbox.TimeDiff(now, "5DaysAgo")

		windowedTable := NewWindowedTable(&TableInfo{
			ProjectID:    "p",
			DatasetID:    "d",
			TableID:      "t",
			Created:      *nineDaysAgo,
			LastModified: now,
		}, now)
		assert.Equal(t, nineDaysAgo.Unix(), windowedTable.Window.From.Unix())
		assert.Equal(t, now, windowedTable.Window.To)

	}

	{ //from to exceeded a week case

		now := time.Now()
		nineDaysAgo, _ := toolbox.TimeDiff(now, "9DaysAgo")
		weekAgo, _ := toolbox.TimeDiff(now, "7DaysAgo")
		windowedTable := NewWindowedTable(&TableInfo{
			ProjectID:    "p",
			DatasetID:    "d",
			TableID:      "t",
			Created:      *nineDaysAgo,
			LastModified: now,
		}, now)
		assert.Equal(t, weekAgo.Unix(), windowedTable.Window.From.Unix())
		assert.Equal(t, now, windowedTable.Window.To)

	}

	{ //from to exceeded a week case
		now := time.Now()
		nineDaysAgo, _ := toolbox.TimeDiff(now, "8DaysAgo")
		eightDaysAgo, _ := toolbox.TimeDiff(now, "8DaysAgo")
		weekAgo, _ := toolbox.TimeDiff(now, "7DaysAgo")
		windowedTable := NewWindowedTable(&TableInfo{
			ProjectID:    "p",
			DatasetID:    "d",
			TableID:      "t",
			Created:      *nineDaysAgo,
			LastModified: *eightDaysAgo,
		}, now)
		assert.Equal(t, -1+time.Duration(weekAgo.UnixNano())/time.Millisecond, time.Duration(windowedTable.Window.From.UnixNano())/time.Millisecond)
		assert.Equal(t, weekAgo.UnixNano(), windowedTable.Window.To.UnixNano())
	}

}

func TestWindowTableNoChangeExpressions(t *testing.T) {
	windowedTable := NewWindowedTable(&TableInfo{
		ProjectID:    "p",
		DatasetID:    "d",
		TableID:      "t",
		Created:      time.Unix(1565121000, 0),
		LastModified: time.Unix(1565121999, 0),
	}, time.Unix(1565121999, 0))
	assert.Equal(t, "[p:d.t@1565121999001-1565121999001]", windowedTable.FormatUnchangedAbsoluteExpr())
	assert.Equal(t, "[d.t@1565121999001-1565121999001]", windowedTable.FormatUnchangedExpr())
}
