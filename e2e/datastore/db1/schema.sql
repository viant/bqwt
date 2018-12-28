CREATE OR REPLACE TABLE table1 (
      id       INT64 NOT NULL,
      name     STRING,
      modified TIMESTAMP
);

CREATE OR REPLACE TABLE table2 (
      id       INT64 NOT NULL,
      name     STRING,
      payload  STRING,
      modified TIMESTAMP
);
