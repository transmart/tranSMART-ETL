#!/bin/sh
etl_id=${1}
BASEDIR=$(dirname $0)
$BASEDIR/data-integration/kitchen.sh -log=load_analysis_stage.log -level=Debug -norep=Y -file=$BASEDIR/Kettle-ETL/load_analysis_from_lz_to_staging.kjb \
-param:ETL_ID=$etl_id \
-param:SORT_DIR='/tmp'
exit
