#! /bin/bash
./data-integration/kitchen.sh \
-norep=Y \
-file=Kettle-ETL/Req7_Final_2/load_QPCR_MIRNA_data.kjb \
-log=load_QPCR_MIRNA_data.log \
-param:DATA_LOCATION=exam \
-param:STUDY_ID=GSE37425 \
-param:MIRNA_TYPE='MIRNA_QPCR' \
-param:MAP_FILENAME=GSE37425_subject_sample_mapping.txt \
-param:DATA_TYPE='R' \
-param:SORT_DIR=/tmp \
-param:TOP_NODE='\Public Studies\ExampleStudy' \
-param:LOAD_TYPE=I \
-param:DATA_FILE_PREFIX=GSE37425_series_matrix
