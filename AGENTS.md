# Agent Instructions for cluster-pack

## Code Style
- Do not add unnecessary blank lines or whitespace
- Follow pylama.ini style guide
- Always add type hints to function signatures
- Use f-strings for string formatting

## Testing
- When modifying code, always update or add corresponding tests
- Tests are located in `tests/` directory
- Run tests with: `pytest -m "not hadoop and not conda" -s tests`

## Linting & Type Checking
- Follow PEP8 and pycodestyle rules proactively (do not run pylama/mypy at each iteration)
- Linter command (when needed): `pylama`
- Type checker command (when needed): `mypy --ignore-missing-imports --config-file setup.cfg`

## Project Structure
- Main package: `cluster_pack/`
- Tests: `tests/`
- Integration tests: `tests/integration/`
- Examples: `examples/` Some of these are used as part of integration testing
