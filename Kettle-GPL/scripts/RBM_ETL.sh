#! /bin/bash
./data-integration/kitchen.sh \
-norep=Y \
-file=Kettle-ETL/Req5_rbm/load_rbm_data.kjb \
-log=load_rbm_data.log \
-param:DATA_LOCATION=exam \
-param:STUDY_ID=TEST111 \
-param:MAP_FILENAME=rbm_subject_mapping_data.txt \
-param:DATA_TYPE='R' \
-param:SORT_DIR=/tmp \
-param:TOP_NODE='\Public Studies\ExampleStudy\TEST111' \
-param:LOAD_TYPE=L \
-param:DATA_FILENAME=rbm_sample_data.txt
