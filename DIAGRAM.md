┌──────────────────────────────────────────────────────────────┐
│                    Ingestion Layer (Rust)                    │
│                                                              │
│  1. File Access (local device)                               │
│     - User selects VCF file                                  │
│     - Rust opens file via path                               │
│                                                              │
│  2. Structural Precheck                                      │
│     - Validate VCF header structure                          │
│     - Check column counts                                    │
│     - Ensure file is streamable                              │
│                                                              │
│  3. Streaming Engine                                         │
│     - Read file in chunks                                    │
│     - Reuse buffer (no full-file load)                       │
│     - Zeroize buffer after each use                          │
│                                                              │
│  4. Rust Validation Layer                                    │
│     - Structural record validation                           │
│     - Field-level checks                                     │
│     - Deterministic rule application                         │
│                                                              │
│  5. Error Surface                                            │
│     - Structured Rust errors                                 │
│     - Line-number reporting                                  │
│     - Precheck vs streaming vs validation error distinction  │
│                                                              │
│  6. Session Metadata                                         │
│     - UUID session ID                                        │
│     - Chunk index                                            │
│     - Timing + instrumentation (optional)                    │
└──────────────────────────────────────────────────────────────┘



┌──────────────────────────────────────────────────────────────┐
│                     vcf-ingest (Rust Crate)                  │
│                                                              │
│  src/                                                        │
│  ├── main.rs                                                 │
│  │     - Entry point                                         │
│  │     - Pipeline orchestration                              │
│  │     - Calls precheck → stream → validate                  │
│                                                              │
│  ├── precheck.rs                                             │
│  │     - Header structure validation                         │
│  │     - Column count checks                                 │
│  │     - Streamability checks                                │
│                                                              │
│  ├── stream.rs                                               │
│  │     - Chunked reading                                     │
│  │     - Buffer reuse                                        │
│  │     - Zeroization after use                               │
│                                                              │
│  ├── validate.rs                                             │
│  │     - Structural record validation                        │
│  │     - Field-level checks                                  │
│  │     - Deterministic rule application                      │
│                                                              │
│  ├── schema.rs                                               │
│  │     - Definitions of VCF fields                           │
│  │     - Type structures for records                         │
│  │     - Shared schema utilities                             │
│                                                              │
│  ├── erase.rs                                                │
│  │     - Memory zeroization utilities                        │
│  │     - Buffer wiping                                       │
│                                                              │
│  ├── error.rs                                                │
│  │     - Structured error types                              │
│  │     - Precheck / streaming / validation error surfaces    │
│                                                              │
│  fixtures/                                                   │
│     - Sample VCF files for testing                           │
│                                                              │
│  target/                                                     │
│     - Build artifacts (ignored by Git)                       │
└──────────────────────────────────────────────────────────────┘



┌──────────────────────────────────────────────────────────────┐
│                   Zeroization Lifecycle (Rust)               │
│                                                              │
│  1. Chunk Allocation                                         │
│     - Streaming engine allocates buffer                      │
│                                                              │
│  2. Chunk Processing                                         │
│     - Precheck or validation reads fields                    │
│     - Deterministic rule application                         │
│                                                              │
│  3. Post-Processing                                          │
│     - Drop temporary structures                              │
│     - Release references                                     │
│                                                              │
│  4. Zeroization                                              │
│     - Overwrite buffer contents                              │
│     - Ensure no residual data remains                        │
│                                                              │
│  5. Buffer Reuse                                             │
│     - Allocate fresh buffer or reuse wiped buffer            │
│     - Begin next chunk                                       │
└──────────────────────────────────────────────────────────────┘
