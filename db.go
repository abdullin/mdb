package mdb

import (
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/pkg/errors"
)

type TxOp func(tx *Tx) error

func (db *DB) CreateRead() (tx *Tx, err error) {
	return db.CreateTransaction(ReadOnly)
}

func (db *DB) Read(fn TxOp) error {
	return db.Env.View(func(t *lmdb.Txn) error {

		tx := &Tx{db.DBI, db.Env, t}
		if err := fn(tx); err != nil {
			return errors.Wrap(err, "db.Env.View")
		}
		return nil
	})
}

func (db *DB) Update(fn TxOp) error {
	return db.Env.Update(func(t *lmdb.Txn) error {

		tx := &Tx{db.DBI, db.Env, t}
		if err := fn(tx); err != nil {
			return errors.Wrap(err, "db.Env.View")
		}
		return nil
	})
}

func (db *DB) UpdateLocked(threadLocked bool, fn TxOp) error {
	if !threadLocked {
		return db.Update(fn)
	}

	return db.Env.UpdateLocked(func(t *lmdb.Txn) error {

		tx := &Tx{db.DBI, db.Env, t}
		if err := fn(tx); err != nil {
			return errors.Wrap(err, "db.Env.View")
		}
		return nil
	})
}

func (db *DB) CreateWrite() (tx *Tx, err error) {
	return db.CreateTransaction(0)
}

const (
	ReadOnly = lmdb.Readonly
)

func (db *DB) CreateTransaction(flags uint) (tx *Tx, err error) {

	var txn *lmdb.Txn
	if txn, err = db.Env.BeginTxn(nil, flags); err != nil {
		return nil, errors.Wrap(err, "BeginTxn(flags)")
	}

	return &Tx{db.DBI, db.Env, txn}, nil
}
