#!/bin/sh
BASEDIR=$(dirname $0)
$BASEDIR/data-integration/kitchen.sh -log=move_analysis_to_prod.log -level=Debug -norep=Y -file=$BASEDIR/Kettle-ETL/nightly_processing.kjb
exit
