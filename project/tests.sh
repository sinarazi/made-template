#!/bin/bash

# Navigate to the project directory
cd "$(dirname "$0")"

python pipeline.py

pytest test_pipeline.py

# Capture the exit code of pytest
exit_code=$?

exit $exit_code
