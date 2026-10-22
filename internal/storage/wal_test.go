package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	records, err := ReadWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || string(records[0]) != "one" || string(records[1]) != "two" {
		t.Fatalf("unexpected records: %q", records)
	}
}

func TestWALRecoveryTruncatesTornRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("complete")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{10, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	records, err := ReadWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || string(records[0]) != "complete" {
		t.Fatalf("unexpected records after recovery: %q", records)
	}
}
