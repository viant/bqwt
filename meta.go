package bqwt

import (
	"time"
)

type Meta struct {
	URL                string
	DatasetID          string
	Tables             []*WindowedTable
	Expression         string `description:"represents recently changed tables ranged decorator relative expression (without project id)"`
	AbsoluteExpression string `description:"represents recently changed tables ranged decorator absolute expression (with project id)"`

	//internals
	expressions         []string
	absoluteExpressions []string
	indexTables         map[string]*WindowedTable
	isTemp              bool
}

//Update updates table info
func (m *Meta) Update(table *TableInfo, currentTime time.Time) *WindowedTable {
	if len(m.indexTables) == 0 {
		m.expressions = []string{}
		m.absoluteExpressions = []string{}
		m.indexTables = make(map[string]*WindowedTable)
		for _, table := range m.Tables {
			m.indexTables[table.Name] = table
		}
	}
	windowed, has := m.indexTables[table.TableID]
	if !has {
		windowed = NewWindowedTable(table, currentTime)
		m.indexTables[windowed.Name] = windowed
		m.Tables = append(m.Tables, windowed)
		m.expressions = append(m.expressions, windowed.Expression)
		m.absoluteExpressions = append(m.absoluteExpressions, windowed.AbsoluteExpression)
		return windowed
	}

	if windowed.Window.To.Equal(table.LastModified) {
		windowed.Changed = false
		return windowed
	}
	nextBoundary := windowed.Window.To.Add(time.Millisecond)
	windowed.Window.From = nextBoundary
	windowed.Window.To = table.LastModified
	windowed.LastChanged = currentTime
	windowed.Expression = windowed.FormatExpr()
	windowed.AbsoluteExpression = windowed.FormatAbsoluteExpr()
	windowed.Changed = true
	m.expressions = append(m.expressions, windowed.Expression)
	m.absoluteExpressions = append(m.absoluteExpressions, windowed.AbsoluteExpression)
	return windowed
}

//Prune removes windowed table info that have not been update since: now - threshold
func (m *Meta) Prune(threshold time.Duration, now time.Time) {
	if threshold == 0 {
		return
	}
	var tables = make([]*WindowedTable, 0)
	for _, candidate := range m.Tables {
		if now.Sub(candidate.LastChanged) > threshold {
			continue
		}
		tables = append(tables, candidate)
	}
	m.Tables = tables
}

//NewMeta creates a new window table meta instance
func NewMeta(URL, datasetID string) *Meta {
	return &Meta{
		URL:                 URL,
		DatasetID:           datasetID,
		Tables:              make([]*WindowedTable, 0),
		expressions:         make([]string, 0),
		absoluteExpressions: make([]string, 0),
	}
}
