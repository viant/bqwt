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
	LastChangedFlag    time.Time
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

//NewWindowedTable creates a new windowed table for supplied table info
func NewWindowedTable(info *TableInfo, now time.Time) *WindowedTable {
	var result = &WindowedTable{
		ID:              fmt.Sprintf("%v.%v", info.DatasetID, info.TableID),
		Dataset:         info.DatasetID,
		ProjectID:       info.ProjectID,
		Name:            info.TableID,
		LastChangedFlag: now,
		Changed:         true,
		Window: &TimeWindow{
			From: info.Created,
			To:   info.LastModified,
		},
	}
	result.Expression = result.FormatExpr()
	result.AbsoluteExpression = result.FormatAbsoluteExpr()
	return result
}
