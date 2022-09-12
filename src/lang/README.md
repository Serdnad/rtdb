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