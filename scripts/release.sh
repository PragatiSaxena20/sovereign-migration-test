#!/usr/bin/env bash

set -eux

uv build
uv publish --username ${TWINE_USERNAME} --password ${TWINE_PASSWORD}
