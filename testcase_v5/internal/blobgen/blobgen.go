package blobgen

import (
	"crypto/md5" //nolint:gosec // md5 column is content fingerprint, not a security boundary
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
)

// Row carries the 43 values needed by one INSERT row (matching v4's
// INSERT_COLUMNS). The order is the same as Columns() so callers can
// flatten the row into the parameter slice without re-mapping.
type Row struct {
	ID                     int64
	TenantID               int64
	UserID                 int64
	DateCreate             int64
	Token                  string
	PubToken               string
	Size                   int64
	IsStored               int
	OriginalName           string
	StoredName             string
	Title                  string
	Mimetype               string
	ParentID               int64
	Contents               []byte
	ContentsENC            []byte // nil at init time
	ContentsHighlight      []byte
	ContentsHighlightENC   []byte // nil at init time
	HighlightType          string
	Metadata               string
	MetadataENC            []byte // nil at init time
	ExternalURL            string
	IsDeleted              int
	DateDelete             int64
	IsPublic               int
	PubShared              int
	LastIndexed            int64
	Source                 string
	ExternalID             string
	ServiceTypeID          int
	ServiceID              int64
	IsMultitenant          int
	OriginalTenantID       int64
	TenantsSharedWith      string
	ServiceTenantID        int64
	IsTombstoned           int
	ThumbnailVersion       int64
	DateThumbnailRetrieved int64
	ExternalPtr            []byte
	MD5                    string
	MetadataVersion        int
	UnencryptedMetadata    string
	RestrictionType        int
	Version                int64
}

// Columns returns the column names in the same order Row's fields use.
// The list matches v4's INSERT_COLUMNS so the wire-level
// INSERT shape matches.
func Columns() []string {
	return []string{
		"id", "tenant_id", "user_id", "date_create",
		"token", "pub_token",
		"size", "is_stored",
		"original_name", "stored_name", "title", "mimetype",
		"parent_id",
		"contents", "contents_enc",
		"contents_highlight", "contents_highlight_enc",
		"highlight_type",
		"metadata", "metadata_enc",
		"external_url",
		"is_deleted", "date_delete",
		"is_public", "pub_shared",
		"last_indexed",
		"source", "external_id",
		"service_type_id", "service_id",
		"is_multitenant", "original_tenant_id", "tenants_shared_with",
		"service_tenant_id",
		"is_tombstoned", "thumbnail_version", "date_thumbnail_retrieved",
		"external_ptr",
		"md5",
		"metadata_version", "unencrypted_metadata",
		"restriction_type", "version",
	}
}

// Values flattens Row into the []any slice the database/sql package wants
// for a prepared INSERT. Order matches Columns().
func (r Row) Values() []any {
	return []any{
		r.ID, r.TenantID, r.UserID, r.DateCreate,
		r.Token, r.PubToken,
		r.Size, r.IsStored,
		r.OriginalName, r.StoredName, r.Title, r.Mimetype,
		r.ParentID,
		r.Contents, r.ContentsENC,
		r.ContentsHighlight, r.ContentsHighlightENC,
		r.HighlightType,
		r.Metadata, r.MetadataENC,
		r.ExternalURL,
		r.IsDeleted, r.DateDelete,
		r.IsPublic, r.PubShared,
		r.LastIndexed,
		r.Source, r.ExternalID,
		r.ServiceTypeID, r.ServiceID,
		r.IsMultitenant, r.OriginalTenantID, r.TenantsSharedWith,
		r.ServiceTenantID,
		r.IsTombstoned, r.ThumbnailVersion, r.DateThumbnailRetrieved,
		r.ExternalPtr,
		r.MD5,
		r.MetadataVersion, r.UnencryptedMetadata,
		r.RestrictionType, r.Version,
	}
}

// Inputs bundles the per-row generator inputs. dateStep is added per row
// (rowN * dateStep), matching v4.
type Inputs struct {
	TenantID int64
	StartID  int64
	DateBase int64
	DateStep int64
	Seed     uint64
	Class    SizeClass
}

// Generate produces the row at offset rowN within the tenant. Output is
// deterministic in (TenantID, StartID, rowN, Seed). The
// PRNG is math/rand/v2 ChaCha8 seeded by a 32-byte derivation of those
// inputs.
func Generate(rowN uint64, in Inputs) Row {
	rng := rand.New(rand.NewChaCha8(seedFor(in.TenantID, in.Seed, rowN)))
	ranges := classRanges[in.Class]

	contentsSize := pickInRange(rng, ranges.contents)
	highlightSize := pickInRange(rng, ranges.highlight)
	externalPtrSize := pickInRange(rng, ranges.externalPtr)

	rowID := in.StartID + int64(rowN)
	dateCreate := in.DateBase + int64(rowN)*in.DateStep

	contents := randBytes(rng, contentsSize)
	highlight := randBytes(rng, highlightSize)
	externalPtr := randBytes(rng, externalPtrSize)
	metadata := randString(rng, ranges.metadataText)

	return Row{
		ID:                     rowID,
		TenantID:               in.TenantID,
		UserID:                 800_000_000_000 + (rowID % 10_000_000),
		DateCreate:             dateCreate,
		Token:                  fmt.Sprintf("sec-%d", rowID),
		PubToken:               fmt.Sprintf("pub-%d", rowID),
		Size:                   int64(contentsSize),
		IsStored:               0,
		OriginalName:           fmt.Sprintf("orig_%d.dat", rowID),
		StoredName:             fmt.Sprintf("stored_%d.dat", rowID),
		Title:                  fmt.Sprintf("tenant_%d_file_%d", in.TenantID, rowN),
		Mimetype:               "application/octet-stream",
		ParentID:               in.TenantID,
		Contents:               contents,
		ContentsENC:            nil,
		ContentsHighlight:      highlight,
		ContentsHighlightENC:   nil,
		HighlightType:          "backfill",
		Metadata:               metadata,
		MetadataENC:            nil,
		ExternalURL:            fmt.Sprintf("https://files.example/%d", rowID),
		IsDeleted:              0,
		DateDelete:             0,
		IsPublic:               0,
		PubShared:              0,
		LastIndexed:            dateCreate,
		Source:                 "HARNESS_TC",
		ExternalID:             fmt.Sprintf("ext-%d", rowID),
		ServiceTypeID:          0,
		ServiceID:              in.TenantID,
		IsMultitenant:          0,
		OriginalTenantID:       in.TenantID,
		TenantsSharedWith:      randString(rng, 256),
		ServiceTenantID:        in.TenantID,
		IsTombstoned:           0,
		ThumbnailVersion:       0,
		DateThumbnailRetrieved: 0,
		ExternalPtr:            externalPtr,
		MD5:                    md5Hex(contents),
		MetadataVersion:        0,
		UnencryptedMetadata:    randString(rng, 1024),
		RestrictionType:        0,
		Version:                0,
	}
}

func md5Hex(b []byte) string {
	sum := md5.Sum(b) //nolint:gosec // content fingerprint, not a security primitive
	return hex.EncodeToString(sum[:])
}

func pickInRange(rng *rand.Rand, span [2]int) int {
	low, high := span[0], span[1]
	if high <= low {
		return low
	}
	return low + rng.IntN(high-low+1)
}

func randBytes(rng *rand.Rand, n int) []byte {
	if n <= 0 {
		return []byte{}
	}
	out := make([]byte, n)
	// rand.Rand has no direct Read; fill 8 bytes at a time from Uint64.
	for i := 0; i < n; i += 8 {
		v := rng.Uint64()
		for j := 0; j < 8 && i+j < n; j++ {
			out[i+j] = byte(v >> (8 * j))
		}
	}
	return out
}

// alphabet for randString — 64 printable characters indexed by 6-bit
// windows of the PRNG output. v4 used SHA-256 expansion; v5 just samples a
// deterministic stream, which still produces text-shaped data for the
// `metadata` (TEXT) and `unencrypted_metadata` (TEXT) columns.
const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

func randString(rng *rand.Rand, n int) string {
	if n <= 0 {
		return ""
	}
	out := make([]byte, n)
	for i := 0; i < n; i += 10 {
		v := rng.Uint64()
		for j := 0; j < 10 && i+j < n; j++ {
			out[i+j] = alphabet[v&0x3F]
			v >>= 6
		}
	}
	return string(out)
}

func seedFor(tenantID int64, seed uint64, rowN uint64) [32]byte {
	h := fnv.New128a()
	var buf [8]byte
	putUint64(buf[:], uint64(tenantID))
	_, _ = h.Write(buf[:])
	putUint64(buf[:], seed)
	_, _ = h.Write(buf[:])
	putUint64(buf[:], rowN)
	_, _ = h.Write(buf[:])
	sum := h.Sum(nil) // 16 bytes
	var out [32]byte
	copy(out[:16], sum)
	copy(out[16:], sum) // double the digest to fill 32 bytes
	return out
}

func putUint64(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
}
