# ydb-sqlglot-plugin

YDB dialect plugin for [sqlglot](https://github.com/tobymao/sqlglot).

This package provides a YDB dialect for sqlglot, enabling SQL parsing and generation with YDB-specific syntax and features.

## Installation

```bash
pip install ydb-sqlglot-plugin
```

## Usage

After installing the package, the `ydb` dialect is automatically registered with sqlglot:

```python
import sqlglot

# Transpile from another dialect to YDB
sql = "SELECT * FROM users WHERE id = 1"
result = sqlglot.transpile(sql, read="mysql", write="ydb")[0]
print(result)
# Output: SELECT * FROM `users` WHERE id = 1

# Parse directly with YDB dialect
parsed = sqlglot.parse_one("SELECT * FROM `users`", dialect="ydb")
print(parsed.sql(dialect="ydb"))
```

## Features

- **Table name escaping**: Table names are automatically wrapped in backticks
- **CTE to variables**: Common Table Expressions are converted to YDB-style variables (`$name = (...)`)
- **Date/Time functions**: Proper mapping of date/time functions to YDB's DateTime module
- **Type mapping**: SQL types are mapped to YDB-specific types (e.g., `VARCHAR` → `Utf8`, `INT` → `INT32`)
- **Subquery decorrelation**: Correlated subqueries are transformed into JOINs for better YDB compatibility
- **Lambda expressions**: Support for YDB-style lambda expressions in array operations

## Development

### Prerequisites

- Python 3.9+
- pip

### Setup

Clone the repository and install in development mode:

```bash
git clone https://github.com/ydb-platform/ydb-sqlglot-plugin.git
cd ydb-sqlglot-plugin

# Create virtual environment (optional but recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install with dev dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest
```
