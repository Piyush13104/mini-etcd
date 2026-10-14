// ┌─────────────────┬─────────────────┬───────────────────────────────┐
//
//	│ Payload Length  │ Payload CRC32   │ Serialized Entry (Binary)     │
//	│   (4 Bytes)     │   (4 Bytes)     │          (N Bytes)            │
//	└─────────────────┴─────────────────┴───────────────────────────────┘
package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type Entry struct {
	Index int64  `json:"index"` // increaing number
	Term  int64  `json:"term"`  // election term of Raft algo
	Type  int32  `json:"type"`  // normal configChange or any
	Data  []byte `json:"data"`  // commands // for buffer you need this is in []byte
	Crc   []int  `json:"crc"`   // checksum for curruption detecttion
}

func encodeEntry(e Entry) ([]byte, error) {

	//fmt.Println(e)

	buf := new(bytes.Buffer)

	err1 := binary.Write(buf, binary.BigEndian, e.Index)
	if err1 != nil {
		return nil, err1
	}

	err2 := binary.Write(buf, binary.BigEndian, e.Term)
	if err2 != nil {
		return nil, err2
	}

	err3 := binary.Write(buf, binary.BigEndian, e.Type)
	if err3 != nil {
		return nil, err3
	}

	dataLen := uint32(len(e.Data))
	err4 := binary.Write(buf, binary.BigEndian, dataLen)
	if err4 != nil {
		return nil, err4
	}

	var _, err = buf.Write(e.Data)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil

}

func WriteWAL(file *os.File, e Entry) error {

	// We cant use os.WriteFile as it overwrite all content
	encodedPayload, err := encodeEntry(e)

	if err != nil {
		return fmt.Errorf("Failed to encode ...", err)
	}

	// checksum
	checksum := crc32.ChecksumIEEE(encodedPayload)
	encodedPayloadLen := uint32(len(encodedPayload))

	frame := new(bytes.Buffer)

	payloadError := binary.Write(frame, binary.BigEndian, encodedPayloadLen)
	if payloadError != nil {
		return payloadError
	}

	checkSumError := binary.Write(frame, binary.BigEndian, checksum)
	if checkSumError != nil {
		return checkSumError
	}

	var _, appendPayloadError = frame.Write(encodedPayload)
	if appendPayloadError != nil {
		return appendPayloadError
	}

	var _, appendBytesError = file.Write(frame.Bytes())
	if appendBytesError != nil {
		return appendBytesError
	}

	return file.Sync()

}

func decodeEntry(payload []byte) (*Entry, error) {
	buf := bytes.NewReader(payload)
	var e Entry

	// Read Params
	if err := binary.Read(buf, binary.BigEndian, &e.Index); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &e.Term); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &e.Type); err != nil {
		return nil, err
	}

	// datalen read
	var dataLen int32
	if err := binary.Read(buf, binary.BigEndian, &dataLen); err != nil {
		return nil, err
	}

	e.Data = make([]byte, dataLen)
	if _, err := io.ReadFull(buf, e.Data); err != nil {
		return nil, err
	}

	return &e, nil
}

func ReadNextEntry(r io.Reader) (*Entry, error) {

	var payLoadLen uint32
	var expectedCheckSum uint32

	// Read Payload Length load in payLoadLen varible
	if err := binary.Read(r, binary.BigEndian, &payLoadLen); err != nil {
		return nil, err
	}

	// Read CheckSum load in ExpectedChecksum  variable
	if err := binary.Read(r, binary.BigEndian, &expectedCheckSum); err != nil {
		return nil, err
	}

	// Read Payload
	payload := make([]byte, payLoadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("Truncated Payload write %w", err)
	}

	// verifying via checksum

	actualCheckSum := crc32.ChecksumIEEE(payload)
	if actualCheckSum != expectedCheckSum {
		return nil, fmt.Errorf("CheckSum mismatched Corrupted Enrty detected %d  !=  %d ", expectedCheckSum, actualCheckSum)
	}

	return decodeEntry(payload)

}

func ReadAllWal(filePath string) ([]Entry, error) {

	file, errorFileOpening := os.Open(filePath)

	if errorFileOpening != nil {
		// if file not present
		if os.IsNotExist(errorFileOpening) {
			return nil, nil
		}
		return nil, errorFileOpening
	}
	defer file.Close()

	var entries []Entry

	for {
		entry, err := ReadNextEntry(file)

		// end of file
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		entries = append(entries, *entry)
	}

	return entries, nil
}
