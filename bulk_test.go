package bulkinsert

import (
	"testing"
)

type nullExecer struct{}

func (s nullExecer) Exec(q string, args ...interface{}) (Result, error) {
	return nil, nil
}

func TestBatchSize(t *testing.T) {
	ne := &nullExecer{}

	inserter := New(ne)

	inserter.Prepare("test", "one", "two", "three", "four")

	for i := 0; i < 1000000; i++ {
		if err := inserter.Insert(i, i, i, i); err != nil {
			t.Fatal(err)
		}
	}

	if err := inserter.Flush(); err != nil {
		t.Fatal(err)
	}
}
