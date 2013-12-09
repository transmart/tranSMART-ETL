#! /bin/bash
./data-integration/kitchen.sh \
-norep=Y \
-file=Kettle-ETL/Req5_RBM_ANNOTATION/load_rbm_annotation.kjb \
-log=load_RBM_ANNOTATION_data.log \
-param:DATA_LOCATION=exam \
-param:SORT_DIR=/tmp \
-param:GPL_ID=RBM100 \
-param:LOAD_TYPE=I \
-param:ANNOTATION_TITLE='RBM'