a minimal library for bulk inserts

### Usage

```go
txn, err := db.Begin()

inserter := bulkinserter.New(txn)

inserter.Prepare(
  "table_name",
  "column_a",
  "column_b",
  "column_c",
)

for i := range rows {
  err := inserter.Insert(rows[i].A, rows[i].B, rows[i].C)
}

err = inserter.Flush()

txn.Commit()
```

### Motivation

- go's sql interface doesn't expose a method for bulk inserts other than copy
- copy from has different rules for escaping single quotes and escape sequences
- copy from doesn't work great with json
