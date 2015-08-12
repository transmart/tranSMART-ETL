#!/bin/bash -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

## load_analysis_batch and nightly
bash "$DIR/ETL/load_analysis_batch.sh"
bash "$DIR/ETL/nightly_processing.sh"

