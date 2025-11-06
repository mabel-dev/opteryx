# Vendored Compression Libraries

This directory contains source code from third-party compression libraries,
included to provide zero-dependency builds of rugo.

## Snappy (Apache License 2.0)
- Version: 1.1.10
- Source: https://github.com/google/snappy
- License: Apache License 2.0

## Zstandard (BSD License)  
- Version: 1.5.5
- Source: https://github.com/facebook/zstd
- License: BSD License

See individual library directories for full license texts.

## Integration
These libraries are compiled directly into rugo for:
- Zero runtime dependencies
- Consistent cross-platform behavior  
- Simplified deployment
- Better performance through static linking
