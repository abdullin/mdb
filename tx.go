package mdb

import (
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/pkg/errors"
)

type Tx struct {
	DB  lmdb.DBI
	Env *lmdb.Env
	Tx  *lmdb.Txn
}

func (tx *Tx) Get(key []byte) (data []byte, err error) {
	if data, err = tx.Tx.Get(tx.DB, key); err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}

		return nil, errors.Wrap(err, "Tx.Get")
	}
	return data, nil
}

func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}

func (tx *Tx) Put(key []byte, val []byte) error {
	if err := tx.Tx.Put(tx.DB, key, val, 0); err != nil {
		return errors.Wrap(err, "tx.Put")
	}
	return nil
}

func (tx *Tx) Del(key []byte) error {
	if err := tx.Tx.Del(tx.DB, key, nil); err != nil {
		return err

	}
	return nil
}

func (tx *Tx) PutReserve(key []byte, size int) ([]byte, error) {
	return tx.Tx.PutReserve(tx.DB, key, size, 0)
}

func (tx *Tx) Close() (err error) {
	return nil
}
