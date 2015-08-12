#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
set -x
fn=${1}
log_date=`date +%Y%m%d%H%M%S`
kitchen.sh -norep=Y -file="$DIR/process_analysis_files.kjb" \
-param:DATA_LOCATION="$DIR/../gwasData" \
-param:LOAD_TYPE=I \
-param:SORT_DIR=/tmp/ \
-param:LOADER_PATH="$SQLLDR" \
-param:METADATA_FILE="MagicDataSet.txt"
#mv /old4/23ME/transmart/ETL/Analysis_Metadata/$fn /old4/23ME/transmart/ETL/Analysis_Metadata/$fn.loaded

