package bqwt

import (
	"time"
)

type Meta struct {
	URL                 string
	DatasetID           string
	Tables              []*WindowedTable
	indexTables         map[string]*WindowedTable
	Expressions         []string
	AbsoluteExpressions []string
	Expression          string
	AbsoluteExpression  string
	isTemp              bool
}

//Update updates table info
func (m *Meta) Update(table *TableInfo, currentTime time.Time) *WindowedTable {
	if len(m.indexTables) == 0 {
		m.Expressions = []string{}
		m.AbsoluteExpressions = []string{}
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
		m.Expressions = append(m.Expressions, windowed.Expression)
		m.AbsoluteExpressions = append(m.AbsoluteExpressions, windowed.AbsoluteExpression)
		return windowed
	}

	if windowed.Window.To.Equal(table.LastModified) {
		windowed.Changed = false
		return windowed
	}
	nextBoundary := windowed.Window.To.Add(time.Millisecond)
	windowed.Window.From = nextBoundary
	windowed.Window.To = table.LastModified
	windowed.LastChangedFlag = currentTime
	windowed.Expression = windowed.FormatExpr()
	windowed.AbsoluteExpression = windowed.FormatAbsoluteExpr()
	windowed.Changed = true
	m.Expressions = append(m.Expressions, windowed.Expression)
	m.AbsoluteExpressions = append(m.AbsoluteExpressions, windowed.AbsoluteExpression)
	return windowed
}

//Prune removes windowed table info that have not been update since: now - threshold
func (m *Meta) Prune(threshold time.Duration, now time.Time) {
	if threshold == 0 {
		return
	}
	var tables = make([]*WindowedTable, 0)
	for _, candidate := range m.Tables {
		if now.Sub(candidate.LastChangedFlag) > threshold {
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
		Expressions:         make([]string, 0),
		AbsoluteExpressions: make([]string, 0),
	}
}
