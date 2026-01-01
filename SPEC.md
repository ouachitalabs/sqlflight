# sqlflight

A fast, opinionated SQL formatter for Snowflake with first-class Jinja support. Written in Rust.

## Philosophy

sqlflight is to SQL what `gofmt` is to Go: a single, opinionated formatting standard with no configuration. The tool makes formatting decisions so you don't have to debate them.

- **No configuration**: One style, always
- **Fail fast**: If it can't parse your SQL, it tells you immediately
- **Jinja-native**: First-class support for Jinja templating via minijinja
- **Fast**: Written in Rust with parser combinators for speed

## Installation

```bash
cargo install sqlflight
```

## Usage

### Format files

```bash
# Format a single file (output to stdout)
sqlflight fmt query.sql

# Format in place
sqlflight fmt --write query.sql

# Format multiple files
sqlflight fmt --write models/*.sql

# Format directory recursively
sqlflight fmt --write ./models/

# Format from stdin
cat query.sql | sqlflight fmt -
```

### Check formatting

```bash
# Check if files are formatted (exit 0 = formatted, 1 = needs formatting, 2 = parse error)
sqlflight check query.sql
sqlflight check ./models/
```

### File discovery

When given a directory, sqlflight recursively finds `.sql` files. Glob patterns are also supported:

```bash
sqlflight fmt '**/*.sql'
sqlflight fmt ./models/
```

## Formatting Rules

### Keywords

All SQL keywords are **lowercase**:

```sql
-- Before
SELECT ID, NAME FROM USERS WHERE ACTIVE = TRUE

-- After
select id, name from users where active = true
```

### Indentation

2 spaces for all indentation levels.

### Line length

Target line length of 120 characters. Lines are wrapped at logical breakpoints when exceeded.

### Commas

Leading commas for column lists and similar constructs:

```sql
select
  id
  , name
  , email
  , created_at
from users
```

### SELECT columns

Threshold-based formatting:
- 3 or fewer columns that fit within line width: inline
- Otherwise: one column per line with leading commas

```sql
-- Inline (short)
select id, name, email from users

-- Vertical (many columns)
select
  id
  , name
  , email
  , created_at
  , updated_at
from users
```

### CTEs (WITH clauses)

Compact CTE header with body indented:

```sql
with active_users as (
  select *
  from users
  where active = true
)
select * from active_users
```

### JOINs

JOIN at same indentation as FROM, ON clause on its own line:

```sql
select
  u.id
  , u.name
  , o.total
from users u
join orders o
  on u.id = o.user_id
left join payments p
  on o.id = p.order_id
```

### WHERE clauses

Leading AND/OR operators:

```sql
select *
from users
where active = true
  and created_at > '2024-01-01'
  and email is not null
```

### CASE expressions

Each WHEN/THEN on separate lines:

```sql
select
  case
    when status = 'active'
      then 'Active'
    when status = 'pending'
      then 'Pending'
    else 'Unknown'
  end as status_label
from users
```

### Parentheses

Context-sensitive: inline for simple expressions, break for complex/nested ones.

```sql
-- Simple (inline)
select (a + b) * c from numbers

-- Complex (breaks)
select
  (
    case
      when x > 0
        then 'positive'
      else 'negative'
    end
  ) as sign
from values
```

### Semicolons

Preserved as-is. sqlflight does not add or remove trailing semicolons.

### Comments

Comments are preserved in their original position. Whitespace around comments is normalized but the comment text and placement relative to code elements is maintained.

## Jinja Support

sqlflight uses a two-pass approach powered by [minijinja](https://github.com/mitsuhiko/minijinja):

1. **Extract**: Identify and extract Jinja expressions/blocks, replacing with magic identifiers
2. **Format**: Parse and format the resulting SQL
3. **Reintegrate**: Replace placeholders with original Jinja content

### Supported Jinja constructs

- Expressions: `{{ variable }}`, `{{ ref('table') }}`
- Statements: `{% if %}`, `{% for %}`, `{% set %}`
- Comments: `{# comment #}`

### dbt macros

Common dbt macros (`{{ ref() }}`, `{{ source() }}`, `{{ config() }}`) are detected and preserved exactly as written.

```sql
{{ config(materialized='table') }}

select
  {{ dbt_utils.star(ref('users')) }}
from {{ ref('users') }}
where {{ filter_condition }}
```

### Jinja block formatting

Whitespace around Jinja tags is normalized to be consistent with the surrounding SQL context:

```sql
select
  id
  {% if include_name %}
  , name
  {% endif %}
  , email
from users
```

## Snowflake SQL Support

sqlflight targets Snowflake SQL as its primary dialect.

### Core SQL (fully supported)

- SELECT, INSERT, UPDATE, DELETE, MERGE
- CREATE/ALTER/DROP TABLE, VIEW
- CTEs (WITH clause)
- JOINs (INNER, LEFT, RIGHT, FULL, CROSS)
- Subqueries
- UNION, INTERSECT, EXCEPT
- ORDER BY, GROUP BY, HAVING
- LIMIT, OFFSET

### Snowflake-specific (supported)

- QUALIFY clause
- FLATTEN / LATERAL
- PIVOT / UNPIVOT
- MATCH_RECOGNIZE
- SAMPLE / TABLESAMPLE
- RESULT_SCAN, GENERATOR
- Semi-structured data access (`:`, `::`, `PARSE_JSON`, etc.)
- VARIANT, OBJECT, ARRAY types

### Not in scope for v1

- Stored procedures
- JavaScript/Python UDFs
- Tasks, streams, pipes
- Snowflake scripting (BEGIN/END blocks)

## Technical Architecture

### Parser

- **Library**: winnow (or nom) parser combinators
- **Approach**: Hand-crafted combinators for SQL grammar, not ANTLR
- **Error handling**: Fail fast with descriptive error messages

### Jinja parsing

- **Library**: minijinja crate
- **Placeholder strategy**: Magic identifiers (`__SQLFLIGHT_JINJA_001__`)

### AST

A unified AST representing SQL structure with holes for Jinja expressions. The formatter walks this AST to produce output.

### Output

Pretty-printer using a width-aware algorithm that respects the 120-character target.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (all files formatted / already formatted) |
| 1 | Files need formatting (check mode only) |
| 2 | Parse error |

## CLI Reference

```
sqlflight - An opinionated SQL formatter for Snowflake

USAGE:
    sqlflight <COMMAND>

COMMANDS:
    fmt      Format SQL files
    check    Check if files are formatted

OPTIONS:
    -h, --help       Print help
    -V, --version    Print version

FMT OPTIONS:
    -w, --write      Write formatted output back to files
    <FILES>          Files or directories to format (use - for stdin)

CHECK OPTIONS:
    <FILES>          Files or directories to check
```

## Project Structure

```
sqlflight/
├── Cargo.toml
├── src/
│   ├── main.rs           # CLI entry point
│   ├── lib.rs            # Library root
│   ├── cli/              # Argument parsing, file discovery
│   ├── parser/           # SQL parser (winnow combinators)
│   │   ├── mod.rs
│   │   ├── lexer.rs      # Tokenization
│   │   ├── expr.rs       # Expression parsing
│   │   ├── stmt.rs       # Statement parsing
│   │   └── snowflake.rs  # Snowflake-specific syntax
│   ├── jinja/            # Jinja extraction and reintegration
│   │   ├── mod.rs
│   │   ├── extract.rs    # Two-pass extraction
│   │   └── reintegrate.rs
│   ├── ast/              # AST node definitions
│   ├── formatter/        # Pretty-printing logic
│   │   ├── mod.rs
│   │   ├── printer.rs    # Width-aware printer
│   │   └── rules.rs      # Formatting rules
│   └── error.rs          # Error types
└── tests/
    ├── snapshots/        # Input/output test cases
    └── integration/      # End-to-end tests
```

## Rust Configuration

- **Edition**: 2024
- **MSRV**: Latest stable
- **Dependencies**:
  - `winnow` - Parser combinators
  - `minijinja` - Jinja parsing
  - `clap` - CLI argument parsing
  - `walkdir` - Directory traversal
  - `glob` - Glob pattern matching
  - `miette` or `ariadne` - Error reporting

## Future Considerations

While v1 has no configuration, the CLI and internal design should not preclude future additions:

- `.sqlflight` or `.sqlflight.toml` configuration file
- Ignore file patterns (`.sqlflightignore`)
- Additional SQL dialects
- Editor integrations (LSP)
- Pre-commit hook

These are explicitly out of scope for v1 but the architecture should not make them impossible.
