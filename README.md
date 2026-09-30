# vcf-ingest

A Rust-based, offline, privacy-preserving VCF ingestion and structural validation
pipeline. The ingestion layer streams the VCF file in bounded chunks, performs
deterministic structural checks, and zeroizes raw buffers immediately after use.
No raw VCF data is ever logged, stored, or retained.

---

## Threat Model

This ingestion layer is designed for environments where raw genomic data must
never be written to disk, never be retained in memory longer than necessary, and
never be exposed to external runtimes or cloud services.

**Primary threats addressed:**

- **Residual data exposure**  
  Raw VCF bytes must not remain in memory after processing.

- **Full-file loading risks**  
  Large VCFs must not be read into RAM in their entirety.

- **Cross-runtime leakage**  
  All validation occurs in Rust; no IPC, Python, or multi-runtime handoff.

- **Implicit logging or debug leakage**  
  No raw lines, buffers, or fields are ever logged.

- **Non-deterministic behavior**  
  Validation must be deterministic, predictable, and auditable.

**Out of scope (V1):**

- Semantic canonicalization  
- Compression/decompression  
- Cryptographic witness generation  
- ZK circuit preparation  
- Cloud ingestion or remote execution

---

## Why Privacy?

VCF files contain highly sensitive genomic information. Even structural ingestion
can leak identifiable data if:

- buffers are not zeroized  
- logs contain raw lines  
- crashes leave memory artifacts  
- external runtimes receive raw bytes  
- full-file loads expose unnecessary surface area  

This project enforces a **local-first, deterministic, zero-knowledge boundary**
around raw variant data. The ingestion layer is intentionally minimal, predictable,
and privacy-preserving.

---

## Design Goals

- **Rust-only pipeline**  
  No Python, no IPC, no multi-runtime orchestration.

- **Bounded streaming**  
  Files are processed in chunks, no full-file load.

- **Deterministic structural validation**  
  Header checks, column counts, and record-level validation.

- **Zeroization after use**  
  Raw buffers are wiped immediately after each chunk.

- **Typed error surfaces**  
  Clear distinctions between precheck, streaming, and validation errors.

- **No raw data retention**  
  Nothing is stored, echoed, or written to disk.

---

## Architecture

```
input.vcf
│
▼
┌──────────────────────────---┐ 
│ Rust Ingestion Layer        │
│ · header + schema precheck  │
│ · bounded streaming         │
│ · reusable zeroized buffer  │
│ · deterministic flow        │
└──────────────────────────---┘
│
▼
┌─────────────────────────--─-┐
│ Rust Validation Layer       │
│ · structural record checks  │
│ · field-level validation    │
│ · typed error surfaces      │
└──────────────────────────---┘
│
▼
validated records → (user-defined downstream processing)
```

---

## Build & Run

```bash
cargo build --release
./target/release/vcf-ingest fixtures/tiny-valid.vcf

```

---

## Tests

```bash
cargo test        # pre-check unit tests against fixtures

```

---

## Notes & limitations (V1)

- Raw VCF lines are streamed through a single bounded buffer; individual lines
are capped at 8 MiB.
- .gz input is rejected in precheck (decompression is out of scope for V1).
- Output is currently limited to structural validation; semantic or canonical
transformations are intentionally out of scope.

---

## Future Work

- Optional semantic validation layer (Rust-only)
- Optional canonical record output
- Optional decompression support
- Optional streaming adapters for downstream systems
- Optional integration with privacy-preserving pipelines

---

## Contributing

This project is intentionally minimal and privacy-focused. Contributions should
preserve:
- deterministic behavior
- zeroization guarantees
- Rust-only execution
- no raw data retention
- no multi-runtime dependencies

Pull requests are welcome for structural improvements, documentation, and
additional validation rules.