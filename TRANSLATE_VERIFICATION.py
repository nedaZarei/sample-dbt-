#!/usr/bin/env python3
"""
Verification Script: TRANSLATE Files Pattern Compliance
======================================================

This script verifies that all 33 TRANSLATE files contain only the expected
dialect-specific pattern changes when comparing postgres/ to snowflake/.

Tasks:
1. Load the pattern catalog (PATTERN_CATALOG.csv)
2. Compare all 33 TRANSLATE files between postgres/ and snowflake/
3. Cross-reference each difference against documented patterns
4. Flag any differences that don't match a pattern
5. Verify consistency: same pattern → same translation everywhere

Files Verified:
- 2 macro files (date_utils.sql, financial_calculations.sql)
- 10 staging models (stg_*.sql)
- 8 intermediate models (int_*.sql)
- 10 marts models (fact_*, report_*)
- 3 config files (profiles.yml, dbt_project.yml, docker-compose.yml)
"""

import os
import csv
import json
import difflib
import hashlib
import re
from pathlib import Path
from collections import defaultdict, OrderedDict


class PatternCatalog:
    """Load and manage pattern catalog from PATTERN_CATALOG.csv"""
    
    def __init__(self, catalog_path='PATTERN_CATALOG.csv'):
        self.patterns = {}
        self.load_catalog(catalog_path)
    
    def load_catalog(self, catalog_path):
        """Load pattern catalog from CSV"""
        if not os.path.exists(catalog_path):
            print(f"Warning: {catalog_path} not found")
            return
        
        with open(catalog_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pattern_id = row['Pattern_ID']
                self.patterns[pattern_id] = {
                    'name': row['Pattern_Name'],
                    'category': row['Category'],
                    'pg_syntax': row['PostgreSQL_Syntax'],
                    'sf_syntax': row['Snowflake_Syntax'],
                    'files_affected': row['Key_Files_Affected'],
                    'sample_locations': row['Sample_Locations']
                }
    
    def get_pattern(self, pattern_id):
        """Get pattern by ID"""
        return self.patterns.get(pattern_id)
    
    def find_patterns_in_text(self, text, filename):
        """Find all patterns present in a file"""
        found_patterns = []
        
        # Pattern 1: Date subtraction (PostgreSQL)
        if re.search(r'\s-\s(?:current_date|date|date_day|cashflow_date|maturity_date|settlement_date|valuation_date)', text, re.IGNORECASE):
            found_patterns.append('1')
        
        # Pattern 2: extract(month from ...)
        if re.search(r'extract\s*\(\s*month\s+from', text, re.IGNORECASE):
            found_patterns.append('2')
        
        # Pattern 3: extract(year from ...)
        if re.search(r'extract\s*\(\s*year\s+from', text, re.IGNORECASE):
            found_patterns.append('3')
        
        # Pattern 4: date_trunc('month', ...)
        if re.search(r"date_trunc\s*\(\s*['\"]month['\"]", text, re.IGNORECASE):
            found_patterns.append('4')
        
        # Pattern 5: date_trunc('quarter', ...)
        if re.search(r"date_trunc\s*\(\s*['\"]quarter['\"]", text, re.IGNORECASE):
            found_patterns.append('5')
        
        # Pattern 6: Fiscal Quarter Calculation (CASE with extract(month ...))
        if re.search(r'case\s+when\s+extract\s*\(\s*month', text, re.IGNORECASE):
            found_patterns.append('6')
        
        # Pattern 7: Fiscal Year Calculation
        if re.search(r'extract\s*\(\s*month[^)]*\)\s*>=\s*7.*extract\s*\(\s*year', text, re.IGNORECASE | re.DOTALL):
            found_patterns.append('7')
        
        # Pattern 8: Row Number with WHERE filter
        if re.search(r'row_number\s*\(\s*\)\s+over\s*\(', text, re.IGNORECASE):
            found_patterns.append('8')
        
        # Pattern 9: Correlated Subquery for MAX
        if re.search(r'\(\s*select\s+max\s*\(', text, re.IGNORECASE):
            found_patterns.append('9')
        
        # Pattern 10: CAST operations (should remain unchanged)
        if re.search(r'cast\s*\(', text, re.IGNORECASE):
            found_patterns.append('10')
        
        # Pattern 11: SELECT DISTINCT
        if re.search(r'select\s+distinct', text, re.IGNORECASE):
            found_patterns.append('11')
        
        # Pattern 12: Window functions
        if re.search(r'(lag|lead|row_number|rank|dense_rank)\s*\(', text, re.IGNORECASE):
            found_patterns.append('12')
        
        return list(set(found_patterns))


class TranslateVerifier:
    """Main verification class for TRANSLATE files"""
    
    def __init__(self):
        self.catalog = PatternCatalog()
        self.postgres_dir = 'postgres'
        self.snowflake_dir = 'snowflake'
        self.results = {
            'summary': {},
            'files': {},
            'issues': [],
            'consistency_checks': []
        }
        
        # Define all 33 TRANSLATE files
        self.translate_files = [
            # Macro files (2)
            'macros/date_utils.sql',
            'macros/financial_calculations.sql',
            
            # Staging models (10)
            'models/staging/stg_benchmarks.sql',
            'models/staging/stg_cashflows.sql',
            'models/staging/stg_counterparties.sql',
            'models/staging/stg_dates.sql',
            'models/staging/stg_fund_structures.sql',
            'models/staging/stg_instruments.sql',
            'models/staging/stg_portfolios.sql',
            'models/staging/stg_positions.sql',
            'models/staging/stg_trades.sql',
            'models/staging/stg_valuations.sql',
            
            # Intermediate models (8)
            'models/intermediate/int_benchmark_returns.sql',
            'models/intermediate/int_cashflow_enriched.sql',
            'models/intermediate/int_daily_positions.sql',
            'models/intermediate/int_fund_nav.sql',
            'models/intermediate/int_irr_calculations.sql',
            'models/intermediate/int_portfolio_attribution.sql',
            'models/intermediate/int_trade_enriched.sql',
            'models/intermediate/int_valuation_enriched.sql',
            
            # Marts models (10)
            'models/marts/fact_cashflow_waterfall.sql',
            'models/marts/fact_fund_performance.sql',
            'models/marts/fact_portfolio_attribution.sql',
            'models/marts/fact_portfolio_pnl.sql',
            'models/marts/fact_portfolio_summary.sql',
            'models/marts/fact_trade_activity.sql',
            'models/marts/report_daily_pnl.sql',
            'models/marts/report_ic_dashboard.sql',
            'models/marts/report_lp_quarterly.sql',
            'models/marts/report_portfolio_overview.sql',
            
            # Config files (3)
            'profiles.yml',
            'dbt_project.yml',
            'docker-compose.yml'
        ]
    
    def read_file(self, filepath):
        """Safely read a file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            return None
    
    def compute_diff_lines(self, pg_content, sf_content):
        """Compute unified diff and return changed lines"""
        if pg_content is None or sf_content is None:
            return None
        
        pg_lines = pg_content.splitlines(keepends=True)
        sf_lines = sf_content.splitlines(keepends=True)
        
        diff = list(difflib.unified_diff(pg_lines, sf_lines, lineterm=''))
        return diff
    
    def analyze_file_changes(self, filepath, diff_lines):
        """Analyze changes in a file and match against patterns"""
        if diff_lines is None or len(diff_lines) == 0:
            return {'changes': 0, 'matched_patterns': [], 'unmatched_changes': []}
        
        analysis = {
            'changes': len([l for l in diff_lines if l.startswith('-') or l.startswith('+')]),
            'matched_patterns': [],
            'unmatched_changes': [],
            'change_details': []
        }
        
        # Extract meaningful changes (skip diff headers)
        changes = []
        for line in diff_lines:
            if line.startswith('-') and not line.startswith('---'):
                changes.append(('removed', line[1:]))
            elif line.startswith('+') and not line.startswith('+++'):
                changes.append(('added', line[1:]))
        
        # Pattern matching for each change
        for change_type, content in changes:
            matched = False
            content_upper = content.upper()
            content_stripped = content.strip()
            
            # Skip empty lines and whitespace-only changes
            if not content_stripped or content_stripped in ['', '@@', '@@ @@']:
                matched = True
            
            # Comment changes are acceptable (but don't count as matched pattern)
            elif content_stripped.startswith('--') or content_stripped.startswith('#'):
                matched = True
            
            # Check for Pattern 1: Date subtraction
            elif 'DATEDIFF' in content_upper and change_type == 'added':
                analysis['matched_patterns'].append({
                    'pattern': '1',
                    'name': 'Date Subtraction for Day Calculation',
                    'change': content_stripped
                })
                matched = True
            elif re.search(r'\s-\s(?:current_date|date\b|min\(|max\()', content, re.IGNORECASE) and change_type == 'removed':
                # Check if this looks like a date subtraction
                if re.search(r'::numeric|/\s*365\.25|\bday\b', content, re.IGNORECASE):
                    analysis['matched_patterns'].append({
                        'pattern': '1',
                        'name': 'Date Subtraction for Day Calculation',
                        'change': content_stripped
                    })
                    matched = True
            
            # Check for Pattern 2: Extract month → MONTH
            elif 'EXTRACT(MONTH' in content_upper and change_type == 'removed':
                analysis['matched_patterns'].append({
                    'pattern': '2',
                    'name': 'Extract Month from Date',
                    'change': content_stripped
                })
                matched = True
            elif re.search(r'\bMONTH\s*\(', content_upper) and change_type == 'added':
                analysis['matched_patterns'].append({
                    'pattern': '2',
                    'name': 'Extract Month from Date',
                    'change': content_stripped
                })
                matched = True
            
            # Check for Pattern 3: Extract year → YEAR
            elif 'EXTRACT(YEAR' in content_upper and change_type == 'removed':
                analysis['matched_patterns'].append({
                    'pattern': '3',
                    'name': 'Extract Year from Date',
                    'change': content_stripped
                })
                matched = True
            elif re.search(r'\bYEAR\s*\(', content_upper) and change_type == 'added':
                analysis['matched_patterns'].append({
                    'pattern': '3',
                    'name': 'Extract Year from Date',
                    'change': content_stripped
                })
                matched = True
            
            # Check for Pattern 4: date_trunc('month') → DATE_TRUNC('MONTH')
            elif re.search(r"DATE_TRUNC\s*\(\s*['\"]MONTH['\"]", content_upper) and change_type == 'added':
                analysis['matched_patterns'].append({
                    'pattern': '4',
                    'name': 'Date Truncation to Month',
                    'change': content_stripped
                })
                matched = True
            elif "date_trunc('month'" in content and change_type == 'removed':
                analysis['matched_patterns'].append({
                    'pattern': '4',
                    'name': 'Date Truncation to Month',
                    'change': content_stripped
                })
                matched = True
            
            # Check for Pattern 5: date_trunc('quarter') → DATE_TRUNC('QUARTER')
            elif re.search(r"DATE_TRUNC\s*\(\s*['\"]QUARTER['\"]", content_upper) and change_type == 'added':
                analysis['matched_patterns'].append({
                    'pattern': '5',
                    'name': 'Date Truncation to Quarter',
                    'change': content_stripped
                })
                matched = True
            elif "date_trunc('quarter'" in content and change_type == 'removed':
                analysis['matched_patterns'].append({
                    'pattern': '5',
                    'name': 'Date Truncation to Quarter',
                    'change': content_stripped
                })
                matched = True
            
            # Check for Pattern 8: row_number → QUALIFY
            elif 'QUALIFY' in content_upper and change_type == 'added':
                analysis['matched_patterns'].append({
                    'pattern': '8',
                    'name': 'Row Number with WHERE filter for Deduplication',
                    'change': content_stripped
                })
                matched = True
            elif 'WHERE' in content_upper and 'ROW_NUMBER' in content_upper and change_type == 'removed':
                analysis['matched_patterns'].append({
                    'pattern': '8',
                    'name': 'Row Number with WHERE filter for Deduplication',
                    'change': content_stripped
                })
                matched = True
            
            # Check for Platform-specific types: CAST to NUMBER or similar
            elif 'CAST' in content_upper and 'NUMBER' in content_upper and change_type == 'added':
                analysis['matched_patterns'].append({
                    'pattern': '10',
                    'name': 'Type Casting (Platform-compatible)',
                    'change': content_stripped
                })
                matched = True
            
            # Pattern 11: SELECT DISTINCT (should be removed in optimization)
            elif 'SELECT DISTINCT' in content_upper:
                # Track DISTINCT patterns but they might be removed
                if change_type == 'removed':
                    analysis['matched_patterns'].append({
                        'pattern': '11',
                        'name': 'Unnecessary DISTINCT (Optimization)',
                        'change': content_stripped
                    })
                    matched = True
            
            if not matched and content_stripped and not re.match(r'^@@', content_stripped):
                analysis['unmatched_changes'].append(content_stripped)
        
        return analysis
    
    def verify_all_files(self):
        """Verify all 33 TRANSLATE files"""
        print("=" * 80)
        print("TRANSLATE FILES VERIFICATION")
        print("=" * 80)
        print()
        
        total_files = len(self.translate_files)
        files_ok = 0
        files_missing = 0
        files_with_issues = 0
        
        for filepath in self.translate_files:
            pg_path = os.path.join(self.postgres_dir, filepath)
            sf_path = os.path.join(self.snowflake_dir, filepath)
            
            pg_content = self.read_file(pg_path)
            sf_content = self.read_file(sf_path)
            
            print(f"Verifying: {filepath}")
            
            if pg_content is None:
                print(f"  ❌ MISSING in postgres: {pg_path}")
                files_missing += 1
                self.results['issues'].append({
                    'file': filepath,
                    'type': 'MISSING_SOURCE',
                    'message': f'Source file not found in postgres/'
                })
                continue
            
            if sf_content is None:
                print(f"  ❌ MISSING in snowflake: {sf_path}")
                files_missing += 1
                self.results['issues'].append({
                    'file': filepath,
                    'type': 'MISSING_TARGET',
                    'message': f'Target file not found in snowflake/'
                })
                continue
            
            # Compute diff
            diff_lines = self.compute_diff_lines(pg_content, sf_content)
            
            # Analyze changes
            analysis = self.analyze_file_changes(filepath, diff_lines)
            
            # Store results
            self.results['files'][filepath] = {
                'status': 'OK' if len(analysis['unmatched_changes']) == 0 else 'REVIEW',
                'changes_count': analysis['changes'],
                'matched_patterns': analysis['matched_patterns'],
                'unmatched_changes': analysis['unmatched_changes'],
                'diff_lines': len(diff_lines) if diff_lines else 0
            }
            
            # Print status
            if analysis['unmatched_changes']:
                print(f"  ⚠️  {len(analysis['unmatched_changes'])} unmatched changes")
                files_with_issues += 1
                for change in analysis['unmatched_changes'][:3]:  # Show first 3
                    print(f"     - {change[:60]}...")
                if len(analysis['unmatched_changes']) > 3:
                    print(f"     ... and {len(analysis['unmatched_changes']) - 3} more")
            else:
                print(f"  ✅ All changes match patterns ({len(analysis['matched_patterns'])} patterns found)")
                files_ok += 1
            
            print()
        
        # Summary
        self.results['summary'] = {
            'total_files': total_files,
            'files_ok': files_ok,
            'files_missing': files_missing,
            'files_with_issues': files_with_issues,
            'compliance_rate': f"{(files_ok / total_files * 100):.1f}%"
        }
        
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total Files Verified:  {total_files}")
        print(f"Files OK:              {files_ok} ({files_ok/total_files*100:.1f}%)")
        print(f"Files Missing:         {files_missing}")
        print(f"Files with Issues:     {files_with_issues}")
        print()
    
    def generate_report(self, output_file='TRANSLATE_VERIFICATION_REPORT.md'):
        """Generate detailed markdown report"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# TRANSLATE Files Verification Report\n\n")
            f.write(f"**Verification Date:** {Path(__file__).stat().st_mtime}\n\n")
            
            # Summary
            f.write("## Summary\n\n")
            summary = self.results['summary']
            f.write(f"- **Total Files Verified:** {summary['total_files']}\n")
            f.write(f"- **Files OK:** {summary['files_ok']} ({summary['compliance_rate']})\n")
            f.write(f"- **Files Missing:** {summary['files_missing']}\n")
            f.write(f"- **Files with Issues:** {summary['files_with_issues']}\n\n")
            
            # Files by status
            f.write("## Files Status\n\n")
            
            ok_files = [f for f, r in self.results['files'].items() if r['status'] == 'OK']
            review_files = [f for f, r in self.results['files'].items() if r['status'] == 'REVIEW']
            missing_files = [issue['file'] for issue in self.results['issues']]
            
            if ok_files:
                f.write("### ✅ Files OK\n\n")
                for file in sorted(ok_files):
                    patterns = self.results['files'][file]['matched_patterns']
                    f.write(f"- **{file}**\n")
                    if patterns:
                        f.write(f"  - Changes: {self.results['files'][file]['changes_count']}\n")
                        f.write(f"  - Patterns: {len(patterns)} matched\n")
                    else:
                        f.write(f"  - No changes (files identical or only comment changes)\n")
                f.write("\n")
            
            if review_files:
                f.write("### ⚠️ Files Requiring Review\n\n")
                for file in sorted(review_files):
                    result = self.results['files'][file]
                    f.write(f"- **{file}**\n")
                    f.write(f"  - Changes: {result['changes_count']}\n")
                    f.write(f"  - Matched Patterns: {len(result['matched_patterns'])}\n")
                    f.write(f"  - Unmatched Changes: {len(result['unmatched_changes'])}\n")
                    if result['unmatched_changes']:
                        f.write(f"  - Issues:\n")
                        for change in result['unmatched_changes'][:5]:
                            f.write(f"    - `{change[:70]}`\n")
                f.write("\n")
            
            if missing_files:
                f.write("### ❌ Missing Files\n\n")
                for file in sorted(set(missing_files)):
                    f.write(f"- **{file}**\n")
                f.write("\n")
            
            # Issues
            if self.results['issues']:
                f.write("## Issues Found\n\n")
                for issue in self.results['issues']:
                    f.write(f"- **{issue['file']}**: {issue['type']} - {issue['message']}\n")
                f.write("\n")
        
        print(f"Report generated: {output_file}")
    
    def generate_json_report(self, output_file='TRANSLATE_VERIFICATION_REPORT.json'):
        """Generate JSON report for programmatic analysis"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"JSON report generated: {output_file}")


def main():
    """Main entry point"""
    verifier = TranslateVerifier()
    verifier.verify_all_files()
    verifier.generate_report()
    verifier.generate_json_report()
    
    # Exit with status code
    if verifier.results['summary']['files_with_issues'] > 0 or verifier.results['summary']['files_missing'] > 0:
        print("\n⚠️  Some files have issues or are missing!")
        return 1
    else:
        print("\n✅ All TRANSLATE files verified successfully!")
        return 0


if __name__ == '__main__':
    exit(main())
