// Portions derived from Go1.27.3 src/time/zoneinfo_read.go.
// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by the BSD-style LICENSE in this directory.
package timezonesource

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"syscall"
)

// ReadSource mirrors the directory/stored-zip source boundary, including Go's
// source-specific directory-file size limit. Custom bytes bypass this reader.
func ReadSource(source, name string) ([]byte, error) {
	if len(source) > 4 && strings.HasSuffix(source, ".zip") {
		return readZip(source, name)
	}
	if source != "" {
		name = source + "/" + name
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, fileError(err)
	}
	defer func() { _ = file.Close() }()
	var data []byte
	var block [4096]byte
	for {
		n, err := file.Read(block[:])
		data = append(data, block[:n]...)
		if n == 0 || err != nil {
			if err == io.EOF {
				err = nil
			}
			return data, fileError(err)
		}
		if len(data) > 10<<20 {
			return nil, errors.New("time: file " + name + " is too large")
		}
	}
}
func fileError(err error) error {
	if path, ok := errors.AsType[*os.PathError](err); ok {
		return path.Err
	}
	return err
}
func readZip(path, name string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fileError(err)
	}
	defer func() { _ = file.Close() }()
	stat, err := file.Stat()
	if err != nil {
		return nil, fileError(err)
	}
	return readStoredZip(file, stat.Size(), path, name)
}
func zipRead(reader io.ReaderAt, fileSize, offset, size int64) ([]byte, error) {
	if offset < 0 || size < 0 || size > int64(math.MaxInt) || offset > fileSize || size > fileSize-offset {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, int(size))
	_, err := reader.ReadAt(data, offset)
	return data, err
}
func zipUint16(b []byte) int64 { return int64(binary.LittleEndian.Uint16(b)) }
func zipUint32(b []byte) int64 { return int64(binary.LittleEndian.Uint32(b)) }
func readStoredZip(reader io.ReaderAt, fileSize int64, path, name string) ([]byte, error) {
	corrupt := errors.New("corrupt zip file " + path)
	tail, err := zipRead(reader, fileSize, fileSize-22, 22)
	if err != nil || zipUint32(tail) != 0x06054b50 {
		return nil, corrupt
	}
	count := zipUint16(tail[10:])
	size, offset := zipUint32(tail[12:]), zipUint32(tail[16:])
	directory, err := zipRead(reader, fileSize, offset, size)
	if err != nil {
		return nil, corrupt
	}
	for range count {
		if len(directory) < 46 || zipUint32(directory) != 0x02014b50 {
			break
		}
		method, size := zipUint16(directory[10:]), zipUint32(directory[24:])
		nameLen, extraLen, commentLen := zipUint16(directory[28:]), zipUint16(directory[30:]), zipUint16(directory[32:])
		offset := zipUint32(directory[42:])
		next := 46 + nameLen + extraLen + commentLen
		if next > int64(len(directory)) {
			return nil, corrupt
		}
		entryName := string(directory[46 : 46+nameLen])
		directory = directory[next:]
		if entryName != name {
			continue
		}
		if method != 0 {
			return nil, errors.New("unsupported compression for " + name + " in " + path)
		}
		header, err := zipRead(reader, fileSize, offset, 30+nameLen)
		if err != nil || zipUint32(header) != 0x04034b50 || zipUint16(header[8:]) != method || zipUint16(header[26:]) != nameLen || string(header[30:]) != name {
			return nil, corrupt
		}
		data, err := zipRead(reader, fileSize, offset+30+nameLen+zipUint16(header[28:]), size)
		if err != nil {
			return nil, corrupt
		}
		return data, nil
	}
	return nil, syscall.ENOENT
}
