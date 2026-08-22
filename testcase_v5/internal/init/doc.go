// Package initphase implements the "Init" seeding phase. Per-tenant workers
// consume a buffered channel sized to init.max_parallel_tenants, each issuing
// multi-row INSERTs capped by both insert_batch_rows and insert_batch_bytes,
// then verifying row counts. When init.max_replication_lag_seconds > 0,
// repllag.Watch samples the read endpoint during seeding and optional bulk
// index rebuild; a lag breach cancels the errgroup. First worker error also
// cancels.
//
// Directory is named "init"; the Go package name is "initphase" to avoid
// colliding with the language's implicit init() function.
package initphase
