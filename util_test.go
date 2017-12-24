package mdb

import (
	"testing"

	"github.com/abdullin/lex-go/tuple"
)

func NewDbWithRange(t *testing.T, max int) (*DB, *Tx) {

	f := getFolder()

	cfg := NewConfig()
	db, err := New(f, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var tx *Tx
	if tx, err = db.CreateWrite(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < max; i += 2 {
		key := CreateKey(i)
		tx.Put(key, key)
	}

	return db, tx
}

func decodeFirstAsInt(b []byte) int64 {
	tpl, err := tuple.Unpack(b)
	if err != nil {
		panic(err)
	}

	return tpl[0].(int64)
}

func TestGetNext(t *testing.T) {

	db, tx := NewDbWithRange(t, 10)
	defer db.Close()

	k, v, err := tx.GetNext(CreateKey(5))

	if err != nil {
		t.Fatal("Failed to find", err)
	}

	dk, dv := decodeFirstAsInt(k), decodeFirstAsInt(v)

	if dk != 6 || dv != 6 {
		t.Fatal("Expected key/value 6/6", "got", dk, dv)
	}

}

func TestGetPrev(t *testing.T) {

	db, tx := NewDbWithRange(t, 10)
	defer db.Close()

	k, v, err := tx.GetPrev(CreateKey(5))

	if err != nil {
		t.Fatal("Failed to find", err)
	}

	dk, dv := decodeFirstAsInt(k), decodeFirstAsInt(v)

	if dk != 4 || dv != 4 {
		t.Fatal("Expected key/value 4/4", "got", dk, dv)
	}

}
