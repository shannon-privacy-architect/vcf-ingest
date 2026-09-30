# vcc-ingest
A Rust-based, offline VCF ingestion and validation pipeline built for privacy. Streams large files in bounded chunks, performs deterministic structural checks, and zeroizes raw buffers after use. No raw genomic data is logged, stored, or exposed to external runtimes.
