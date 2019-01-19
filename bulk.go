package bulkinsert

import (
	"database/sql"
	"fmt"
	"strings"
)

func New(txn *sql.Tx) *Inserter {
	return &Inserter{txn: txn, batchSize: 500}
}

func (s *Inserter) WithBatchSize(sz int) *Inserter {
	s.batchSize = sz
	return s
}

type Inserter struct {
	txn       *sql.Tx
	stmt      string
	values    []interface{}
	n         int
	template  []string
	batchSize int
}

func (s *Inserter) Prepare(table string, cols ...string) {
	s.stmt = fmt.Sprintf("insert into %s (%s) values ", table, strings.Join(cols, ","))

	var xs []string
	for i := 0; i < len(cols); i++ {
		xs = append(xs, "$%d")
	}
	part := "(" + strings.Join(xs, ",") + ")"
	for i := 0; i < s.batchSize; i++ {
		start := len(cols) * i
		var ns []interface{}
		for j := 0; j < len(cols); j++ {
			ns = append(ns, 1+start+j)
		}

		s.template = append(s.template, fmt.Sprintf(part, ns...))
	}

	fmt.Println(s.stmt + strings.Join(s.template, ","))

}

func (s *Inserter) Insert(xs ...interface{}) error {
	s.n++
	s.values = append(s.values, xs...)

	if s.n >= s.batchSize {
		return s.flush()
	}

	return nil
}

func (s *Inserter) Flush() error {
	return s.flush()
}

func (s *Inserter) Commit() error {
	return s.txn.Commit()
}

func (s *Inserter) flush() error {
	query := s.stmt + strings.Join(s.template[:s.n], ",")
	_, err := s.txn.Exec(query, s.values...)

	s.values = nil
	s.n = 0
	return err

}
