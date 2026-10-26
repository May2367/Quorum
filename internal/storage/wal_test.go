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

func TestWALRecoveryTruncatesCorruptTrailingRecordAndKeepsPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []string{"one", "two", "three"} {
		if err := w.Append([]byte(record)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	records, err := ReadWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || string(records[0]) != "one" || string(records[1]) != "two" {
		t.Fatalf("records after corrupt tail recovery = %q, want [one two]", records)
	}

	recovered, err := ReadWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 2 || string(recovered[0]) != "one" || string(recovered[1]) != "two" {
		t.Fatalf("records after second recovery = %q, want [one two]", recovered)
	}
}
