# Contributing to medicaid-utils

## Setup

```bash
git clone <your-fork-url>
cd medicaid-utils
pip install -e .
pip install pylint pytest
```

## Running Tests

```bash
pytest tests/                        # full suite (316 tests)
pytest tests/preprocessing/          # single module
pytest tests/adapted_algorithms/     # single module
```

## Linting

```bash
pylint medicaid_utils
```

## CI

CI runs `pylint` and `pytest` on Python 3.11, 3.12, and 3.13.

## Workflow

1. Fork the repo and create a feature branch.
2. Make your changes.
3. Run tests (`pytest tests/`) and the linter (`pylint medicaid_utils`).
4. Commit, push, and open a pull request.

## Reporting Bugs

Open a GitHub issue with:

- Python version and medicaid-utils version
- Minimal reproduction steps
- Full traceback
- Expected vs. actual behavior

## Code Style

- Follow existing patterns in the codebase.
- Use type hints.
- Write NumPy-style docstrings.
- Keep functions focused and single-purpose.
