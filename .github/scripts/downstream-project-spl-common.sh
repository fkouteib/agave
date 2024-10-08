#!/usr/bin/env bash
set -e

here="$(dirname "${BASH_SOURCE[0]}")"

#shellcheck source=ci/downstream-projects/common.sh
source "$here"/../../ci/downstream-projects/common.sh

set -x
rm -rf spl
git clone https://github.com/solana-labs/solana-program-library.git spl

# copy toolchain file to use solana's rust version
cp "$SOLANA_DIR"/rust-toolchain.toml spl/
cd spl || exit 1
echo "HEAD: $(git rev-parse HEAD)"

project_used_solana_version=$(sed -nE 's/solana-sdk = \"[>=<~]*(.*)\"/\1/p' <"token/program/Cargo.toml")
echo "used solana version: $project_used_solana_version"
if semverGT "$project_used_solana_version" "$SOLANA_VER"; then
  echo "skip"
  export SKIP_SPL_DOWNSTREAM_PROJECT_TEST=1
  return
fi

./patch.crates-io.sh "$SOLANA_DIR"

# anza migration stopgap. can be removed when agave is fully recommended for public usage.
sed -i 's/solana-geyser-plugin-interface/agave-geyser-plugin-interface/g' ./Cargo.toml

# should be removed when spl bump their curve25519-dalek
sed -i "s/^curve25519-dalek =.*/curve25519-dalek = \"4.1.3\"/" token/confidential-transfer/proof-generation/Cargo.toml

# fix curve25519-dalek

sed -i '/\[patch.crates-io\]/a curve25519-dalek = { git = "https://github.com/anza-xyz/curve25519-dalek.git", rev = "b500cdc2a920cd5bff9e2dd974d7b97349d61464" }' ./Cargo.toml
