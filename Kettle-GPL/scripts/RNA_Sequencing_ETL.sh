#! /bin/bash
/projects/data-integration/kitchen.sh \
-norep=Y \
-file=/projects/tranSMART-ETL/Kettle-GPL/Kettle-ETL/load_RNA_sequencing_data.kjb \
-log=load_RNA_sequencing_data.log \
-param:DATA_LOCATION=Samples \
-param:STUDY_ID=TEST005 \
-param:MAP_FILENAME=Sample_RNA_Seq_mapping_file.txt \
-param:DATA_TYPE='R' \
-param:SORT_DIR=/tmp \
-param:DATA_FILE=dSample_RNA_Sequencing.txt \
-param:TOP_NODE='\Public Studies\ExampleStudy\TEST1\' \
-param:LOAD_TYPE=I \
-param:DATA_FILE_PREFIX=dSample_RNA_Sequencing
