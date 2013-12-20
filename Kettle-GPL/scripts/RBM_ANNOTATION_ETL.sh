#! /bin/bash
./data-integration/kitchen.sh \
-norep=Y \
-file=../Kettle-ETL/load_rbm_annotation.kjb \
-log=load_RBM_ANNOTATION_data.log \
-param:DATA_LOCATION=../data/rbm-annotation \
-param:SORT_DIR=/tmp \
-param:GPL_ID=RBM \
-param:LOAD_TYPE=I \
-param:ANNOTATION_TITLE='RBM'
