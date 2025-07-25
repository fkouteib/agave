#!/usr/bin/env bash
set -eo pipefail

here="$(dirname "$0")"

#shellcheck source=ci/bench/common.sh
source "$here"/common.sh

# Run runtime benches
_ cargo +"$rust_nightly" bench --manifest-path runtime/Cargo.toml ${V:+--verbose} \
  -- -Z unstable-options --format=json | tee -a "$BENCH_FILE"

(
  # solana-keygen required when building C programs
  _ cargo build --manifest-path=keygen/Cargo.toml
  export PATH="$PWD/target/debug":$PATH

  _ make -C programs/sbf all

  # Run sbf benches
  _ cargo +"$rust_nightly" bench --manifest-path programs/sbf/Cargo.toml ${V:+--verbose} --features=sbf_c \
    -- -Z unstable-options --format=json --nocapture | tee -a "$BENCH_FILE"
)

# Run banking/accounts bench. Doesn't require nightly, but use since it is already built.
_ cargo +"$rust_nightly" run --release --manifest-path banking-bench/Cargo.toml ${V:+--verbose} | tee -a "$BENCH_FILE"

# Run zk-elgamal-proof benches.
_ cargo +"$rust_nightly" bench --manifest-path programs/zk-elgamal-proof/Cargo.toml ${V:+--verbose} | tee -a "$BENCH_FILE"

# Run precompile benches.
_ cargo +"$rust_nightly" bench --manifest-path precompiles/Cargo.toml ${V:+--verbose} | tee -a "$BENCH_FILE"
