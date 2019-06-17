package bulkinsert

import (
	"database/sql"
	_ "github.com/lib/pq"
	"testing"
)

func TestPrepareRaw(t *testing.T) {

	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		t.Fatal(err)
	}

	db.Exec("delete from test_upsert")

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	loader := New(txn)

	stmt := `
	INSERT INTO test_upsert as t (id, val)
	VALUES %s
	ON CONFLICT (id)
	DO UPDATE SET
		val = EXCLUDED.val
`

	loader.Prepare("insert into test_upsert (id, val) values %s")

	for i := 0; i < 2000; i++ {
		if err := loader.Insert(i, "aaa"); err != nil {
			t.Fatal(err)
		}
	}

	if err := loader.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	txn, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	loader = New(txn)
	loader.Prepare(stmt)
	for i := 0; i < 1000; i++ {
		loader.Insert(i, "bbb")
	}

	if err := loader.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
}
