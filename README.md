# BigQuery Windowed Tables (bqwt)

This library is compatible with Go 1.11+

#Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#Motivation)


## Motivation

Ability to process incrementally incoming data in a way that is both duplication free and cost-effective is of paramount importance, 
especially when data is loaded or streamed to BigQuery in real time.
When dealing with many tables at once managing processing state can be adds yet additional aspect that needs to be taken care.
This library was developed to simplify multi tables time windowing processing.
It can be deployed as stand alone service or as cloud function.

## Introduction

Big Query provides a mechanism allowing windowing data added within the last 7 days with [range decorators](https://cloud.google.com/bigquery/table-decorators).

Syntax:

```sql
SELECT * PROJECT_ID:DATASET.TABLE@<timeFrom>-<timeTo>
```


References table data added between <timeFrom> and <timeTo>, in milliseconds since the epoch.
- <timeFrom> and <timeTo> must be within the last 7 days.


One important factor driving Big Query table layout design that needs to be taken into account is that the range decorators are only supported with Legacy SQL, 
meaning that standardSQL supported partition and clustered tables can not be windowed with this method currently.

In the absence of partition and clustering the following table design layout should provide good flexibility:

- DATASET.TABLE_[DATE_SUFFIX]
- DATASET.TABLE_[PARTITION_SHARD]_[DATE_SUFFIX]

In both of the scenarios it is possible to use [table template](https://cloud.google.com/bigquery/streaming-data-into-bigquery) in case when data is streamed to Big Query.

**Multi Read One Write scenario**

The following shows example dataset windowing timeline:

1) t0: data is streamed to Big Query
2) t1: Process X reads dataset snapshot between t0 and t1 
    -  call http endpoint with WindowedTable?mode=r&meta=bucket/x/meta.json&dataset=project:dataset&expr=true'
3) t2: more data is streamed
4) t3: Process X completed t0 to t1 processing, flags t0-t1 completed 
    -   WindowedTable?mode=w&meta=bucket/x/meta.json&dataset=project:dataset&expr=true'
5) t4: more data is streamed
6) t5: Process X reads dataset snapshot between t2 and t4 
    -   WindowedTable?mode=r&meta=bucket/x/meta.json&dataset=project:dataset&expr=true'
7) t6: more data is streamed
8) t7: Process X tries to process data but something goes wrong, thus no update
9) t8: more data is streamed
10) t9: Process X again reads dataset snapshot between t2 and t4 
    -   WindowedTable?mode=r&meta=bucket/x/meta.json&dataset=project:dataset&expr=true'
11) t10: more data is streamed
12) t11: Process X completed t2 to t4 processing, flags t2-t4 completed
    -   WindowedTable?mode=w&meta=bucket/x/meta.json&dataset=project:dataset&expr=true'


### Endpoint invocation:

 TDD - add documentation here

## Usage

### Stand alone app

### Apache beam 

 - JDK
 
 - GO sdk


    
## Deployment

Stand alone service
TDD - add documentation here
Docker service
TDD - add documentation here

### Google cloud deployment

Disclaimer: Go Cloud function is only available at alpha at the moment, use the following [form](https://docs.google.com/forms/d/e/1FAIpQLSfJ08R2z7FumQyYGGuTyK4x5M-6ch7WmJ_3uWYI5SdZUb5SBw/viewform) to apply for early access.

- gcloud auth login
- gcloud components install alpha
- gcloud config set project PROJECT_ID
- export GOOGLE_APPLICATION_CREDENTIALS=credentialFile
- gcloud alpha functions deploy WindowedTable --entry-point Handle --runtime go111 --trigger-http



