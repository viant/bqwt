package bqwt

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRunBQQuery(t *testing.T) {

	ctx := context.Background()
	SQL := "SELECT 1 as f1, CURRENT_DATE() AS f2,  CURRENT_TIMESTAMP() AS f3"
	record := struct {
		f1 string
		f2 time.Time
		f3 time.Time
	}{}
	var err error
	err = RunBQQuery(ctx, "viant-e2e", "US", SQL, nil, true, func(row []bigquery.Value) (b bool, e error) {
		record.f1 = AsString(row[0])
		if record.f2, err = AsTime(row[1]); err != nil {
			return false, err
		}
		if record.f3, err = AsTime(row[2]); err != nil {
			return false, err
		}
		return true, nil
	})
	assert.Nil(t, err)
	assert.EqualValues(t, "1", record.f1)
	assert.NotNil(t, record.f2)
	assert.NotNil(t, record.f3)

}

func TestAsTime(t *testing.T) {
	{
		ts, err := AsTime(bigquery.Value(int64(1544714944730)))
		assert.Nil(t, err)
		assert.EqualValues(t, 2018, ts.Year())
	}
	{
		ts, err := AsTime(bigquery.Value("2018-12-13"))
		assert.Nil(t, err)
		assert.EqualValues(t, 2018, ts.Year())
	}
	{
		ts, err := AsTime(bigquery.Value("2018-12-13 23:28:46.639807939 +0000 UTC"))
		assert.Nil(t, err)
		assert.EqualValues(t, 2018, ts.Year())
	}

}
