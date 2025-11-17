#!/usr/bin/env bash
set -euxo pipefail

# Example: convert and run a notebook
python ci/nb2py.py content/asynchronous-inference.ipynb /tmp/asynchronous-inference.py --ignore-cmds
python /tmp/asynchronous-inference.py

python content/client.py

rm /tmp/asynchronous-inference.py
