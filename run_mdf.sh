#!/bin/bash
# Run mdf without uv to avoid Ray runtime_env conflicts
exec "$(dirname "$0")/.venv/bin/python" -m mega_data_factory.cli "$@"
