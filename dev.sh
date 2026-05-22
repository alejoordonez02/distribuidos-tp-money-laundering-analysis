#!/bin/bash
set -e

python3 gen_compose.py
make down
make up
