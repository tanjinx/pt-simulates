// Package blobgen produces deterministic per-row column payloads for the
// init phase. Generate is keyed on (tenantID, rowN, seed) so two
// runs with the same JSON config produce identical wire bytes.
// Statistical size parity with v4's blob_profile is required; byte
// parity with v4's PRNG is explicitly NOT required.
package blobgen
