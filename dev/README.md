# Development Tools

This directory contains scripts and tools used for Opteryx development, building, and release management.

## Contents

- `build_counter.py` - Manages build version numbering for releases
- `build-wheels.sh` - Script for building Python wheels for distribution
- `requirements_embedded.txt` - Requirements for embedded/minimal installations
- `scripts/` - Additional development scripts and utilities

## Usage

### Building Wheels
```bash
./build-wheels.sh
```

Note: macOS releases are arm64-only; Intel/x86_64 macOS wheels are not built by CI or released.

### Updating Build Counter
```bash
python build_counter.py
```

## Notes

These tools are primarily used in CI/CD pipelines and for release preparation. Most day-to-day development doesn't require running these scripts directly.