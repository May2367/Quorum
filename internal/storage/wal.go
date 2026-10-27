package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

const headerSize = 8

var ErrRecordTooLarge = errors.New("wal record too large")

type WAL struct {
	f *os.File
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f}, nil
}

func (w *WAL) Append(record []byte) error {
	return w.AppendBatch([][]byte{record})
}

func (w *WAL) AppendBatch(records [][]byte) error {
	for _, record := range records {
		if err := w.writeRecord(record); err != nil {
			return err
		}
	}
	return w.f.Sync()
}

func (w *WAL) writeRecord(record []byte) error {
	if len(record) > int(^uint32(0)) {
		return ErrRecordTooLarge
	}
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(record)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(record))
	if _, err := w.f.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.f.Write(record)
	return err
}

func (w *WAL) Close() error {
	return w.f.Close()
}

func ReadWAL(path string) ([][]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records [][]byte
	var offset int64
	for {
		var hdr [headerSize]byte
		n, err := io.ReadFull(f, hdr[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if truncErr := os.Truncate(path, offset); truncErr != nil {
				return nil, truncErr
			}
			break
		}
		if err != nil {
			return nil, err
		}
		offset += int64(n)
		size := binary.LittleEndian.Uint32(hdr[0:4])
		wantCRC := binary.LittleEndian.Uint32(hdr[4:8])
		record := make([]byte, size)
		n, err = io.ReadFull(f, record)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			if truncErr := os.Truncate(path, offset-headerSize); truncErr != nil {
				return nil, truncErr
			}
			break
		}
		if err != nil {
			return nil, err
		}
		if got := crc32.ChecksumIEEE(record); got != wantCRC {
			if truncErr := os.Truncate(path, offset-headerSize); truncErr != nil {
				return nil, truncErr
			}
			break
		}
		offset += int64(n)
		records = append(records, record)
	}
	return records, nil
}
