#! /bin/bash
/projects/transmart-data/env/data-integration/kitchen.sh \
-norep=Y \
-file=load_proteomics_annotation.kjb \
-log=load_proteomics_annotation_data.log \
-param:DATA_LOCATION=/projects/proteomics-etl/data/1/annot \
-param:SORT_DIR=/tmp \
-param:GPL_ID=PROTEOMICS \
-param:LOAD_TYPE=I \
-param:ANNOTATION_TITLE='PROTEINS'
