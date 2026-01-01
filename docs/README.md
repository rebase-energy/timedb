# timedb Documentation

This directory contains the Sphinx documentation for timedb.

## Building the Documentation Locally

### Prerequisites

Install the documentation dependencies:

```bash
pip install -r requirements.txt
```

Or install timedb with documentation extras (if configured):

```bash
pip install timedb[docs]
```

### Building

From the `docs` directory:

```bash
# Using make (Linux/macOS)
make html

# Or using sphinx-build directly
sphinx-build -b html . _build/html
```

The built documentation will be in `_build/html/`. Open `_build/html/index.html` in your browser.

### Cleaning

To clean the build directory:

```bash
make clean
```

## Documentation Structure

- `index.rst` - Main documentation index
- `installation.rst` - Installation instructions
- `cli.rst` - Command Line Interface documentation
- `sdk.rst` - SDK usage documentation
- `api_setup.rst` - API server setup documentation
- `conf.py` - Sphinx configuration

## Read the Docs

This documentation is configured for Read the Docs. The configuration is in `.readthedocs.yml` at the project root.

To set up on Read the Docs:

1. Connect your GitHub repository to Read the Docs
2. Read the Docs will automatically detect the `.readthedocs.yml` file
3. The documentation will be built and published automatically

## Contributing

When adding or updating documentation:

1. Edit the relevant `.rst` files
2. Build locally to verify the changes
3. Commit and push to trigger Read the Docs build

