#!/bin/bash

# Install requirements from requirements.txt
pip install -r requirements.txt

# Install networkx 2.5 using conda
conda install -c conda-forge networkx=2.5 -y

# Uninstall bson
pip uninstall bson -y

# Uninstall pymongo
pip uninstall pymongo -y

# Install pymongo 3.10.1
pip install pymongo==3.10.1

echo "Remember to set SECRETS_JSON environment variable."