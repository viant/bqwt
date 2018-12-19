package bqwt

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"time"
)

//RunBQQuery runs BQ SQL
func RunBQQuery(ctx context.Context, project, datasetLocation string, SQL string, params []interface{}, useLegacy bool, handler func(row []bigquery.Value) (bool, error)) error {
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return err
	}
	query := client.Query(SQL)
	query.UseLegacySQL = useLegacy
	if len(params) > 0 {
		query.Parameters = make([]bigquery.QueryParameter, 0)
		for _, param := range params {
			query.Parameters = append(query.Parameters, bigquery.QueryParameter{Value: param})
		}
	}
	query.Location = datasetLocation
	job, err := query.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	it, err := job.Read(ctx)
	if err != nil {
		return err
	}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		cont, e := handler(row)
		if e != nil || !cont {
			return e
		}
	}
	return nil
}

func AsString(value bigquery.Value) string {
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprintf("%v", value)
}

func AsTime(value bigquery.Value) (time.Time, error) {
	if timeInMs, ok := value.(int64); ok {
		ts := time.Unix(0, timeInMs*int64(time.Millisecond))
		return ts, nil
	}

	layout := "2006-01-02 15:04:05.999999999 Z0700 MST"
	timeLiteral := AsString(value)
	if len(timeLiteral) < len(layout) {
		layout = string(layout[:len(timeLiteral)])
	}
	return time.Parse(layout, timeLiteral)
}
