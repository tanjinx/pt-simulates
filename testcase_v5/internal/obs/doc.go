// Package obs sets up the dual-handler slog logger: JSON to
// artifacts/<run-id>/run.jsonl and
// text to stdout, with mandatory ts/level/component/phase fields on every
// record.
package obs
