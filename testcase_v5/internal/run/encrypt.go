package runphase

import (
	"crypto/sha256"
	"fmt"
)

// encryptBlob is the v4 contract: SHA-256(source || label-bytes),
// then repeat that 32-byte digest to match the source length (or 32 if the
// source was empty). The "encryption" name matches the workload's vocabulary
// — the config fixes encryption.mode == "sha256".
func encryptBlob(source []byte, label string) []byte {
	target := len(source)
	if target == 0 {
		target = 32
	}
	h := sha256.New()
	h.Write(source)
	h.Write([]byte(label))
	digest := h.Sum(nil) // 32 bytes
	out := make([]byte, target)
	for i := 0; i < target; i += len(digest) {
		copy(out[i:], digest)
	}
	return out
}

// encryptText is encryptBlob over a UTF-8-encoded string source.
func encryptText(source string, label string) []byte {
	return encryptBlob([]byte(source), label)
}

func contentsENCLabel(rowID int64) string {
	return fmt.Sprintf("%d-contents_enc", rowID)
}

func highlightENCLabel(rowID int64) string {
	return fmt.Sprintf("%d-highlight_enc", rowID)
}

func metadataENCLabel(rowID int64) string {
	return fmt.Sprintf("%d-metadata_enc", rowID)
}
