#! /bin/bash
./data-integration/kitchen.sh \
-norep=Y \
-file=Kettle-ETL/Req6_SEQ_MIRNA/load_QPCR_MIRNA_data.kjb \
-log=load_QPCR_MIRNA_data_n.log \
-param:DATA_LOCATION=exam \
-param:STUDY_ID=TEST005 \
-param:MIRNA_TYPE='MIRNA_SEQ' \
-param:MAP_FILENAME=Sample_QPCR_MIRNA_mapping_file.txt \
-param:DATA_TYPE='R' \
-param:SORT_DIR=/tmp \
-param:DATA_FILE=dSample_QPCR_MIRNA.txt \
-param:TOP_NODE='\Public Studies\ExampleStudy\Test\' \
-param:LOAD_TYPE=I \
-param:DATA_FILE_PREFIX=dSample_QPCR_MIRNA
