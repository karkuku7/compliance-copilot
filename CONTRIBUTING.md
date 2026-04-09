# Contributing to Compliance Copilot

Thanks for your interest in contributing!

## Development Setup

```bash
git clone https://github.com/YOUR_USERNAME/compliance-copilot.git
cd compliance-copilot
python3.11 -m pip install -e ".[dev]"
```

## Running Tests

```bash
python -m pytest                    # All tests
python -m pytest -x                 # Stop on first failure
python -m pytest tests/test_transform_properties.py  # Property-based tests only
python -m pytest --cov              # With coverage
```

## Code Style

- Type hints on all public functions
- Docstrings on all public classes and functions
- No hardcoded values — use `constants.py` or environment variables
- Custom exceptions should carry `recoverable` and `suggested_action`

## Pull Request Process

1. Fork the repo and create a feature branch
2. Add tests for new functionality (prefer property-based tests for data transformations)
3. Ensure all tests pass: `python -m pytest`
4. Update documentation if you've changed interfaces
5. Submit a PR with a clear description of what and why

## Architecture Decisions

If your change involves a significant design decision, please document it in `docs/ARCHITECTURE.md` with:
- What the decision is
- What alternatives were considered
- Why this approach was chosen
