#!/bin/bash
# Build script for rust_accelerated_ops extension

set -e

cd "$(dirname "$0")"

echo "Building Rust extension with maturin..."

# Check if maturin is installed
if ! command -v maturin &> /dev/null && ! python -c "import maturin" 2>/dev/null; then
    echo "maturin not found. Installing..."
    uv pip install maturin
fi

# Build the wheel
source ../.venv/bin/activate 2>/dev/null || true
maturin build --release -o ../dist

echo ""
echo "Build complete!"
echo ""
echo "To install:"
echo "  uv pip install ../dist/rust_accelerated_ops-*.whl"
