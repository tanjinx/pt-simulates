package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
)

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func byteReader(b []byte) io.Reader { return bytes.NewReader(b) }
