#!/usr/bin/env bash
# Regenerate Hive Metastore Thrift bindings.
# Run this when upgrading the target Hive Metastore version.
#
# Requirements:
#   - thrift >= 0.22.0 compiler: brew install thrift (macOS) or apt install thrift-compiler
#   - The Hive .thrift source files from the target Hive version
#
# Usage: bash scripts/generate_hms.sh <path-to-hive-thrift-dir>

set -e
THRIFT_DIR=${1:-"./hive-thrift"}
OUTPUT_DIR="./app-code/etl/vendor/hms"

echo "Generating Python bindings from: $THRIFT_DIR"

if [ ! -d "$THRIFT_DIR" ]; then
    echo "Error: Thrift directory $THRIFT_DIR not found."
    exit 1
fi

thrift --gen py -r -out $OUTPUT_DIR $THRIFT_DIR/hive_metastore.thrift

echo "Done. Files written to $OUTPUT_DIR"
echo "Remember to update etl/vendor/hms/__init__.py with the Hive version."
