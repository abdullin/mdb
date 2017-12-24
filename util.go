package mdb

import (
	"bytes"

	"github.com/abdullin/lex-go/tuple"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
	"github.com/pkg/errors"

	proto "github.com/golang/protobuf/proto"
)

func CreateKey(args ...tuple.Element) []byte {
	tpl := tuple.Tuple(args)
	return tpl.Pack()
}

func (tx *Tx) PutProto(key []byte, val proto.Message) error {
	var err error
	var data []byte

	if data, err = proto.Marshal(val); err != nil {
		return errors.Wrap(err, "Marshal")
	}
	return tx.Put(key, data)
}

func (tx *Tx) ReadProto(key []byte, pb proto.Message) error {
	var data []byte
	var err error

	if data, err = tx.Get(key); key != nil {
		return errors.Wrap(err, "tx.Get")
	}

	if data == nil {
		return nil
	}

	if err = proto.Unmarshal(data, pb); err != nil {
		return errors.Wrap(err, "Unmarshal")
	}
	return nil
}

func (tx *Tx) GetNext(key []byte) (k, v []byte, err error) {
	scanner := lmdbscan.New(tx.Tx, tx.DB)
	defer scanner.Close()
	if !scanner.Set(key, nil, lmdb.SetRange) {
		err = lmdb.NotFound
		return
	}

	if !scanner.Scan() {
		err = lmdb.NotFound
		return
	}

	k = scanner.Key()
	v = scanner.Val()
	err = scanner.Err()
	return
}

func (tx *Tx) GetPrev(key []byte) (k, v []byte, err error) {

	scanner := lmdbscan.New(tx.Tx, tx.DB)
	defer scanner.Close()
	if !scanner.Set(key, nil, lmdb.SetRange) {
		err = lmdb.NotFound
		return
	}
	if !scanner.Set(nil, nil, lmdb.Prev) {
		err = lmdb.NotFound
		return
	}

	if !scanner.Scan() {
		err = lmdb.NotFound
		return
	}

	k = scanner.Key()
	v = scanner.Val()
	err = scanner.Err()
	return
}

func (tx *Tx) ScanRange(key []byte, row func(k, v []byte) error) error {
	scanner := lmdbscan.New(tx.Tx, tx.DB)
	defer scanner.Close()
	if !scanner.Set(key, nil, lmdb.SetRange) {
		return nil
	}

	for scanner.Scan() {
		if !bytes.HasPrefix(scanner.Key(), key) {
			break
		}
		err := row(scanner.Key(), scanner.Val())
		if err != nil {
			return err
		}
	}
	return scanner.Err()
}
func (t *Tx) DelRange(key []byte) error {

	scanner := lmdbscan.New(t.Tx, t.DB)
	defer scanner.Close()

	if !scanner.Set(key, nil, lmdb.SetRange) {
		return nil
	}

	for scanner.Scan() {
		if !bytes.HasPrefix(scanner.Key(), key) {
			break
		}

		err := t.Tx.Del(t.DB, scanner.Key(), nil)
		if err != nil {
			return err
		}
	}

	return scanner.Err()
}
