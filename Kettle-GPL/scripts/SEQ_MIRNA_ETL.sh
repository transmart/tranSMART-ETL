#! /bin/bash
/projects/data-integration/kitchen.sh \
-norep=Y \
-file=../Kettle-ETL/load_QPCR_MIRNA_data.kjb \
-log=load_QPCR_MIRNA_data_n.log \
-param:DATA_LOCATION=../data/mirna-seq/ \
-param:STUDY_ID=mirnaseq \
-param:MIRNA_TYPE='MIRNA_SEQ' \
-param:MAP_FILENAME=GSE37425_subject_sample_mapping.txt \
-param:DATA_TYPE='R' \
-param:SORT_DIR=/tmp \
-param:DATA_FILE=GSE37425_series_matrix.txt \
-param:TOP_NODE='\Public Studies\mirnaseq\' \
-param:LOAD_TYPE=I \
-param:DATA_FILE_PREFIX=GSE37425_series_matrix
