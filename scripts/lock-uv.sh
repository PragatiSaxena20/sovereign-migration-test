#!/usr/bin/env bash
cd /proj || exit 1
export PATH="$HOME/.local/bin:$PATH"
pip install uv
uv lock
