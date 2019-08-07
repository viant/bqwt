package bqwt

import (
	"fmt"
	"time"
)

//WindowedTable represents a windowed tables
type WindowedTable struct {
	ID                 string
	ProjectID          string
	Name               string
	Dataset            string
	Window             *TimeWindow
	LastChanged        time.Time
	Changed            bool
	Expression         string `description:"represents table ranged decorator table windowed expression"`
	AbsoluteExpression string `description:"represents absolute table path ranged decorator table windowed expression"`
}

//FormatExpr formats form SQL range decorator expression
func (t *WindowedTable) FormatExpr() string {
	from := t.Window.From.UnixNano() / int64(time.Millisecond)
	to := t.Window.To.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("[%v@%v-%v]", t.ID, from, to)
}

func (t *WindowedTable) FormatUnchangedExpr() string {
	to := t.Window.To.UnixNano()/int64(time.Millisecond) + 1
	return fmt.Sprintf("[%v@%v-%v]", t.ID, to, to)
}

//FormatExpr formats form SQL range decorator expression
func (t *WindowedTable) FormatAbsoluteExpr() string {
	from := t.Window.From.UnixNano() / int64(time.Millisecond)
	to := t.Window.To.UnixNano() / int64(time.Millisecond)
	project := t.ProjectID
	if project != "" {
		project += ":"
	}
	return fmt.Sprintf("[%v%v@%v-%v]", project, t.ID, from, to)
}

//FormatExpr formats form SQL range decorator expression
func (t *WindowedTable) FormatUnchangedAbsoluteExpr() string {
	to := t.Window.To.UnixNano()/int64(time.Millisecond) + 1
	project := t.ProjectID
	if project != "" {
		project += ":"
	}
	return fmt.Sprintf("[%v%v@%v-%v]", project, t.ID, to, to)
}

//NewWindowedTable creates a new windowed table for supplied table info
func NewWindowedTable(info *TableInfo, now time.Time) *WindowedTable {

	lowerBound := info.Created
	upperBound := info.LastModified
	weekAgo := now.Add(-(7*time.Hour*24 + time.Millisecond))
	if lowerBound.Before(weekAgo) {
		lowerBound = weekAgo
	}

	if upperBound.Before(lowerBound) {
		upperBound = lowerBound.Add(time.Millisecond)
	}

	var result = &WindowedTable{
		ID:          fmt.Sprintf("%v.%v", info.DatasetID, info.TableID),
		Dataset:     info.DatasetID,
		ProjectID:   info.ProjectID,
		Name:        info.TableID,
		LastChanged: now,
		Changed:     true,
		Window: &TimeWindow{
			From: lowerBound,
			To:   upperBound,
		},
	}
	result.Expression = result.FormatExpr()
	result.AbsoluteExpression = result.FormatAbsoluteExpr()
	return result
}
