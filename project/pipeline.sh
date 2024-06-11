#!/bin/bash

cd project
# Installing the required libraries
python -m pip install --upgrade pip
pip install -r ../requirements.txt

python pipeline.py
