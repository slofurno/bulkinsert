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
	txn         *sql.Tx
	stmt        string
	values      []interface{}
	n           int
	template    []string
	batchSize   int
	ncols       int
	didTemplate bool
}

func (s *Inserter) Prepare(stmt string) {
	s.stmt = stmt
}

func (s *Inserter) buildTemplate(ncols int) []string {
	var ret []string
	var xs []string
	for i := 0; i < ncols; i++ {
		xs = append(xs, "$%d")
	}
	part := "(" + strings.Join(xs, ",") + ")"
	for i := 0; i < s.batchSize; i++ {
		start := ncols * i
		var ns []interface{}
		for j := 0; j < ncols; j++ {
			ns = append(ns, 1+start+j)
		}

		ret = append(ret, fmt.Sprintf(part, ns...))
	}
	return ret
}

func (s *Inserter) Insert(xs ...interface{}) error {
	if !s.didTemplate {
		s.didTemplate = true
		s.ncols = len(xs)
		s.template = s.buildTemplate(len(xs))
	}
	if len(xs) != s.ncols {
		return fmt.Errorf("expected %d cols, got %d", s.ncols, len(xs))
	}
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
	if s.n == 0 {
		return nil
	}
	query := fmt.Sprintf(s.stmt, strings.Join(s.template[:s.n], ","))
	_, err := s.txn.Exec(query, s.values...)

	s.values = nil
	s.n = 0
	return err

}
