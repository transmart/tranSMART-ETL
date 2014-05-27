#! /bin/bash
./data-integration/kitchen.sh \
-norep=Y \
-file=../Kettle-ETL/load_rbm_data.kjb \
-log=load_rbm_data.log \
-param:DATA_LOCATION=../data/rbm \
-param:STUDY_ID=TESTNEWDATATYPESRBM \
-param:MAP_FILENAME=rbm_subject_mapping_data.txt \
-param:DATA_TYPE='R' \
-param:SORT_DIR=/tmp \
-param:TOP_NODE='\Public Studies\TESTNEWDATATYPESRBM' \
-param:LOAD_TYPE=L \
-param:DATA_FILENAME=rbm_sample_data.txt
