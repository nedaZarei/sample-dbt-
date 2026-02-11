#!/bin/bash

# Copy and verify all 18 COPY files from postgres/ to snowflake4/

set -e

SOURCE_DIR="./postgres"
TARGET_DIR="./snowflake4"

# Create target directory structure
echo "Creating directory structure in ${TARGET_DIR}..."
mkdir -p "${TARGET_DIR}/seeds"
mkdir -p "${TARGET_DIR}/models/staging"
mkdir -p "${TARGET_DIR}/models/intermediate"
mkdir -p "${TARGET_DIR}/models/marts"
mkdir -p "${TARGET_DIR}/tests"
mkdir -p "${TARGET_DIR}/schemas"

# Define all 18 COPY files
declare -a FILES=(
    # Seeds (10 CSV files)
    "seeds/raw_benchmarks.csv"
    "seeds/raw_cashflows.csv"
    "seeds/raw_counterparties.csv"
    "seeds/raw_dates.csv"
    "seeds/raw_fund_structures.csv"
    "seeds/raw_instruments.csv"
    "seeds/raw_portfolios.csv"
    "seeds/raw_positions.csv"
    "seeds/raw_trades.csv"
    "seeds/raw_valuations.csv"
    # Documentation (3 YAML files)
    "models/staging/_staging.yml"
    "models/intermediate/_intermediate.yml"
    "models/marts/_marts.yml"
    # Test definitions (1 file)
    "tests/assert_pnl_balanced.sql"
    # Schema documentation (2 files)
    "schemas/all_schemas_postgres.md"
    "schemas/generate_schemas.py"
    # Configuration (2 files)
    ".user.yml"
    "packages.yml"
)

echo ""
echo "Copying files from ${SOURCE_DIR} to ${TARGET_DIR}..."
COPY_COUNT=0
for file in "${FILES[@]}"; do
    SOURCE_FILE="${SOURCE_DIR}/${file}"
    TARGET_FILE="${TARGET_DIR}/${file}"
    
    if [ ! -f "$SOURCE_FILE" ]; then
        echo "❌ MISSING SOURCE: $file"
        continue
    fi
    
    cp -v "$SOURCE_FILE" "$TARGET_FILE"
    COPY_COUNT=$((COPY_COUNT + 1))
done

echo ""
echo "✓ Copied ${COPY_COUNT}/${#FILES[@]} files"
echo ""

# Verify with SHA256 checksums
echo "Verifying checksums..."
MATCHES=0
MISMATCHES=0

for file in "${FILES[@]}"; do
    SOURCE_FILE="${SOURCE_DIR}/${file}"
    TARGET_FILE="${TARGET_DIR}/${file}"
    
    if [ ! -f "$SOURCE_FILE" ] || [ ! -f "$TARGET_FILE" ]; then
        echo "❌ MISSING: $file"
        MISMATCHES=$((MISMATCHES + 1))
        continue
    fi
    
    SOURCE_HASH=$(sha256sum "$SOURCE_FILE" | awk '{print $1}')
    TARGET_HASH=$(sha256sum "$TARGET_FILE" | awk '{print $1}')
    
    if [ "$SOURCE_HASH" = "$TARGET_HASH" ]; then
        echo "✓ $file"
        MATCHES=$((MATCHES + 1))
    else
        echo "❌ $file - SHA256 MISMATCH"
        echo "   Source: $SOURCE_HASH"
        echo "   Target: $TARGET_HASH"
        MISMATCHES=$((MISMATCHES + 1))
    fi
done

echo ""
echo "Results: $MATCHES matches, $MISMATCHES mismatches"
echo ""

if [ $MISMATCHES -eq 0 ]; then
    echo "✓✓✓ VERIFICATION PASSED ✓✓✓"
    echo "All 18 COPY files are byte-identical between source and target."
    exit 0
else
    echo "❌❌❌ VERIFICATION FAILED ❌❌❌"
    echo "Found $MISMATCHES file(s) with discrepancies."
    exit 1
fi
