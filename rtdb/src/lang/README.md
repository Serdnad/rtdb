## Priorities

Expressivity: Be able to do anything (reasonable)
Ergonomic

## Language

Read Series
```sql
SELECT series_name
```

Read certain fields from Series
```sql
SELECT series_name['field1', 'field2', 'time']
```

Read with a filter
```sql
SELECT series_name['field1'] WHERE field1 = 'value'
```


OPTIONS FOR RANGES

SQL + InfluxQL
```sql
... WHERE time > {} AND time < {}
```

InfluxDB Flux inspired by range ()
```sql
... BETWEEN (start, end)
```

```sql
... AFTER {} BEFORE {}
```


oh what about a mixture of the 2 before? so
```sql
... BEFORE {}
... AFTER {}
... BETWEEN ({}, {})
```


### JOINS

```sql
SELECT [series1['field1'], series2] MATCH series1.field1 = series.field2
```


OLD

DOCS:

supported select queries

To select all recorded metrics in a series, execute the following:

SELECT series

To only select certain fields from a series,

```sql
SELECT series[field1, field2]
```

To select fields within a certain time range, there's the following

SELECT series AFTER now()-5m
SELECT series BEFORE now()-5m
SELECT series BETWEEN (now()-30m, now()-20m)

Supported time units are as follow:

- Seconds: "s"
- Minutes: "m"
- Hours: "h"
- Days: "d"
- Weeks: "w"

Note: Months and years are intentionally unsupported, due to increased likelihood of errors or
unexpected behavior stemming from ambiguity in what constitutes a month or a year. For example, a
month can be either 28, 30, or 31 days, and a year can be either 365 days, ~365.25, or 366. Rather
than risk unexpected results, we encourage users to use either absolute timestamps, or one of the
well defined units of time listed above.
