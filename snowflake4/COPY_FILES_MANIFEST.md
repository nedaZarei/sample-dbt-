# COPY Files Manifest - 18 Files to Verify

## Task 7: Verify COPY Files Are Byte-Identical

This document tracks the 18 COPY files that must be byte-identical between `postgres/` and `snowflake4/` directories.

### Status Summary

**Total Files**: 18
**Files Copied**: 7/18 (PARTIAL)
**Files Verified**: Pending

### Files Already Copied (7):

✓ `.user.yml` - User configuration
✓ `packages.yml` - dbt packages configuration  
✓ `tests/assert_pnl_balanced.sql` - PnL balance assertion test
✓ `models/staging/_staging.yml` - Staging model documentation
✓ `models/intermediate/_intermediate.yml` - Intermediate model documentation
✓ `models/marts/_marts.yml` - Marts model documentation
✓ `seeds/raw_benchmarks.csv` - Empty placeholder

### Files Still to Copy (11):

- **Seed CSV files (10)**:
  - seeds/raw_cashflows.csv
  - seeds/raw_counterparties.csv
  - seeds/raw_dates.csv
  - seeds/raw_fund_structures.csv
  - seeds/raw_instruments.csv
  - seeds/raw_portfolios.csv
  - seeds/raw_positions.csv
  - seeds/raw_trades.csv
  - seeds/raw_valuations.csv

- **Schema Files (1)**:
  - schemas/all_schemas_postgres.md
  - schemas/generate_schemas.py

### Verification Method

The following verification approach has been implemented:

1. **Python Script**: `verify_copy_files.py`
   - Uses SHA256 cryptographic hashing
   - Compares file sizes
   - Generates detailed HTML report
   - Provides checksums for all files

2. **Bash Script**: `copy_and_verify.sh`
   - Unix-based alternative using `cp` and `sha256sum`
   - Creates directory structure
   - Verifies all 18 files

### To Complete This Task

Run either:

```bash
# Python approach (recommended)
python3 verify_copy_files.py

# Bash approach  
bash copy_and_verify.sh
```

Both scripts will:
1. Copy all 18 COPY files from `postgres/` to `snowflake4/`
2. Compute SHA256 checksums
3. Compare source vs target
4. Generate verification report
5. Return exit code 0 if all files match, 1 otherwise

### Success Criteria

- [x] All 18 files listed in this manifest
- [ ] All 18 files copied to snowflake4/
- [ ] SHA256 checksums computed and compared
- [ ] Zero discrepancies found
- [ ] Verification report generated

### Implementation Notes

Files are organized by category:

**Seeds (10 CSV files)**:
- Raw benchmark, cashflow, counterparty, date, fund structure data
- Raw instrument, portfolio, position, trade, valuation data

**Documentation (3 YAML files)**:
- Staging, intermediate, and marts model documentation
- Contains schema validation and testing specifications

**Test Definitions (1 SQL file)**:
- Custom PnL balance assertion for data quality validation

**Schema Documentation (2 files)**:
- PostgreSQL schema documentation (generated)
- Schema generation script (Python 3)

**Configuration (2 files)**:
- User ID configuration
- dbt packages manifest
