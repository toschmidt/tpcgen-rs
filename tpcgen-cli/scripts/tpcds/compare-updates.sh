#!/usr/bin/env bash
#
# compare-updates.sh — Compare the Rust refresh data sets to C dsdgen's.
#
# Please see print_usage() below for details.

set -euo pipefail

print_usage() {
    cat << 'EOF'
compare-updates.sh — Compare the Rust refresh data sets to C dsdgen's.

Generates update sets 1 to 4 with `tpcgen-cli tpcds updates` and checks the
MD5 hash of each of their files against
tests/fixtures/tpcds/updates-scale-N-c/MD5SUMS, the hashes of the files
`dsdgen -scale N -update <n>` writes. Exits non-zero if any file differs.

The hashes were made with dsdgen 2.11 given TPC-DS v4.0.0's return reasons
(the reason table and the reason keys of the returns are the only base data
the two versions generate differently).

USAGE:
    ./scripts/tpcds/compare-updates.sh [OPTIONS]

OPTIONS:
    --scale N       Scale factor: 1 (default), 10, 100 or 1000
    --keep          Keep the generated files (prints their directory)
    -h, --help      Show this help

ENVIRONMENT:
    TPCGEN_CLI_BIN  tpcgen-cli binary to use. Defaults to the workspace's
                    target/release/tpcgen-cli, built if missing.
EOF
}

SCALE=1
KEEP=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --scale) SCALE=$2; shift 2 ;;
        --keep) KEEP=true; shift ;;
        -h|--help) print_usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; print_usage >&2; exit 1 ;;
    esac
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
MD5SUMS="$PROJECT_ROOT/tests/fixtures/tpcds/updates-scale-$SCALE-c/MD5SUMS"
if [[ ! -f "$MD5SUMS" ]]; then
    echo "No reference hashes for scale factor $SCALE: $MD5SUMS" >&2
    exit 1
fi

BINARY=${TPCGEN_CLI_BIN:-}
if [[ -z "$BINARY" ]]; then
    WORKSPACE_ROOT=$(cd "$PROJECT_ROOT" && dirname "$(cargo locate-project --workspace --message-format=plain)")
    BINARY="$WORKSPACE_ROOT/target/release/tpcgen-cli"
    [[ -x "$BINARY" ]] || (cd "$WORKSPACE_ROOT" && cargo build --release -p tpcgen-cli)
fi

OUTPUT=$(mktemp -d)
if [[ $KEEP == false ]]; then
    trap 'rm -r "$OUTPUT"' EXIT
fi
"$BINARY" tpcds updates --scale-factor "$SCALE" --sets 4 --output-dir "$OUTPUT" --quiet

FAILED=0
while read -r expected file; do
    actual=$(md5sum "$OUTPUT/$file" | cut -d' ' -f1)
    if [[ $actual != "$expected" ]]; then
        echo "DIFF $file: expected $expected, got $actual"
        FAILED=$((FAILED + 1))
    fi
done < "$MD5SUMS"

if [[ $KEEP == true ]]; then
    echo "Generated files: $OUTPUT"
fi
if [[ $FAILED -ne 0 ]]; then
    echo "Scale factor $SCALE: $FAILED of $(wc -l < "$MD5SUMS") files differ from dsdgen"
    exit 1
fi
echo "Scale factor $SCALE: all $(wc -l < "$MD5SUMS") files match dsdgen"
