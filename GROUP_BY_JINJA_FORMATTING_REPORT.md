# GROUP BY Formatting Bug Inside Jinja Blocks - Analysis Report

## Summary

The GROUP BY (and HAVING) clause formatting is **NOT being applied** when SQL code is inside Jinja block statements (`{% ... %}`). This is a critical bug that prevents proper SQL formatting for complex queries wrapped in Jinja `{% set %}` blocks.

## Issues Identified

### 1. No Spaces After Commas in GROUP BY
When GROUP BY appears inside Jinja, commas lack spacing:
```
GROUP BY 1,2,3,a.id,a.name           # INCORRECT - inside Jinja
GROUP BY 1, 2, 3, a.id, a.name       # CORRECT - expected format
```

### 2. No Spaces Around Operators in HAVING
When HAVING appears inside Jinja, comparison operators lack proper spacing:
```
HAVING COUNT(*)>1 AND SUM(a.amount)>1000           # INCORRECT - inside Jinja
HAVING COUNT(*) > 1 AND SUM(a.amount) > 1000       # CORRECT - expected format
```

### 3. No Multi-line Formatting for Multiple Columns
When GROUP BY has multiple columns inside Jinja, they remain on a single line:
```
GROUP BY 1,2,3,a.id,a.name                         # INCORRECT - inside Jinja
GROUP BY                                           # CORRECT - expected format
  1
  , 2
  , 3
  , a.id
  , a.name
```

## Concrete Example

### Input (with Jinja block)
```sql
{% set grouping_query %}
SELECT a.id, a.name, COUNT(*)
FROM t a
GROUP BY 1,2,3,a.id,a.name
HAVING COUNT(*)>1 AND SUM(a.amount)>1000
{% endset %}
```

### Current Output (BUG - unformatted)
```sql
{% set grouping_query %}SELECT a.id, a.name, COUNT(*) FROM t a GROUP BY 1,2,3,a.id,a.name HAVING COUNT(*)>1 AND SUM(a.amount)>1000{% endset %}
```

### Expected Output (desired formatting)
```sql
{% set grouping_query %}
select a.id, a.name, count(*)
from t a
group by
  1
  , 2
  , 3
  , a.id
  , a.name
having count(*) > 1 and sum(a.amount) > 1000
{% endset %}
```

## Comparison: Formatting Works Correctly Outside Jinja

The same query **without** Jinja block formatting works perfectly:

### Input (without Jinja)
```sql
SELECT a.id, a.name, COUNT(*)
FROM t a
GROUP BY 1,2,3,a.id,a.name
HAVING COUNT(*)>1 AND SUM(a.amount)>1000
```

### Current Output (CORRECT - properly formatted)
```sql
select a.id, a.name, count(*)
from t a
group by
  1
  , 2
  , 3
  , a.id
  , a.name
having count(*) > 1 and sum(a.amount) > 1000
```

## Root Cause Analysis

This is **NOT an isolated GROUP BY bug**—it is **part of the broader Jinja formatting issue** where:

1. **Jinja Extraction**: The formatter extracts Jinja blocks and replaces them with placeholders
2. **SQL Parsing & Formatting**: The remaining SQL is parsed and formatted
3. **Jinja Reintegration**: Jinja blocks are reinserted into the formatted result

**The Problem**: When Jinja blocks contain SQL statements (like `{% set sql_query %}`), the SQL inside those blocks is preserved exactly as-is and **never formatted**. The formatter treats Jinja block contents as opaque content to preserve, not as SQL to format.

## Related Issues

This bug affects all SQL features inside Jinja blocks, not just GROUP BY:
- CTEs (WITH clauses)
- JOIN conditions
- WHERE clauses with multiple conditions
- ORDER BY clauses
- Subqueries
- Window functions

See related bug reports:
- `/Users/john.mccrary/code/labs/sqlflight/WINDOW_FUNCTIONS_IN_JINJA_REPORT.md`
- `/Users/john.mccrary/code/labs/sqlflight/UNION_FORMATTING_ANALYSIS.md`

## Test Coverage

Three new tests have been added to `/Users/john.mccrary/code/labs/sqlflight/tests/jinja_tests.rs` in the `jinja_cte_formatting` module:

1. **`group_by_inside_jinja_set_block_not_formatted`**: Demonstrates the bug with GROUP BY and HAVING formatting
2. **`group_by_with_inline_jinja_variables`**: Shows that formatting works correctly when not inside Jinja blocks
3. **`group_by_multiple_in_jinja_block_multiline_expectation`**: Demonstrates the issue with multi-line GROUP BY not being applied

## Recommendation

This requires a fix to the Jinja extraction/reintegration pipeline:

1. **Option A**: Extract and format SQL from inside Jinja `{% set ... %}` blocks
   - More complex but provides better formatting for complex queries in Jinja
   - Requires careful handling to preserve Jinja variable references like `{{ variable }}`

2. **Option B**: Document as a known limitation
   - Simpler but leaves queries inside `{% set %}` blocks unformatted
   - Users would need to format SQL separately before embedding in Jinja

The proper solution is **Option A**, as users commonly store complex queries in Jinja variables, and these should be formatted for readability.

## Files Modified

- `/Users/john.mccrary/code/labs/sqlflight/tests/jinja_tests.rs` - Added 3 test cases demonstrating the bug

## Files Created

- `/Users/john.mccrary/code/labs/sqlflight/GROUP_BY_JINJA_FORMATTING_REPORT.md` - This report
