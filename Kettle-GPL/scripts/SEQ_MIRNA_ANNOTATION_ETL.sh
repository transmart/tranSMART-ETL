#! /bin/bash
 /projects/data-integration/kitchen.sh \
-norep=Y \
-file=../Kettle-ETL/load_QPCR_MIRNA_annotation.kjb \
-log=load_QPCR_MIRNA_ANNOTATION_data.log \
-param:DATA_LOCATION=../data/mirna-seq-annotation \
-param:SORT_DIR=/tmp \
-param:GPL_ID=GPL15467seqbased \
-param:LOAD_TYPE=I \
-param:MIRNA_TYPE='MIRNA_SEQ' \
-param:ANNOTATION_TITLE='MIRNA_SEQ'