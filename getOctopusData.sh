#!/bin/bash
source ~/.bashrc
here="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd $here
conda activate openhabstuff
python $here/octopus.py
