package mdb

import (
	"log"
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/pkg/errors"
)

type DB struct {
	Env *lmdb.Env
	DBI lmdb.DBI
}

// New creates a new DB wrapper around LMDB
func New(folder string, cfg *Config) (*DB, error) {
	env, err := lmdb.NewEnv()

	if err != nil {
		return nil, errors.Wrap(err, "env create failed")
	}

	err = env.SetMaxDBs(cfg.MaxDBs)
	if err != nil {
		return nil, errors.Wrap(err, "env config failed")
	}
	err = env.SetMapSize(cfg.SizeMbs * 1024 * 1024)
	if err != nil {
		return nil, errors.Wrap(err, "map size failed")
	}

	if err = env.SetFlags(cfg.EnvFlags); err != nil {
		return nil, errors.Wrap(err, "set flag")
	}

	os.MkdirAll(folder, os.ModePerm)
	err = env.Open(folder, 0, cfg.Mode)
	if err != nil {
		return nil, errors.Wrap(err, "open env")
	}

	var staleReaders int
	if staleReaders, err = env.ReaderCheck(); err != nil {
		return nil, errors.Wrap(err, "reader check")
	}
	if staleReaders > 0 {
		log.Printf("cleared %d reader slots from dead processes", staleReaders)
	}

	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("agg")
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "create DB")
	}

	return &DB{env, dbi}, nil

}

// Close the environment
func (db *DB) Close() error {

	if db.Env == nil {
		return nil
	}

	//db.Env.CloseDBI(db.DBI)
	err := db.Env.Close()
	db.Env = nil
	return errors.Wrap(err, "Env.Close")
}
