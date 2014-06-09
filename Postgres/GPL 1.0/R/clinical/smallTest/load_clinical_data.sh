#!/bin/sh

StudyID="smallTest"
TopNode="\\Public Studies\\$StudyID\\"
ColumnMapFile="ColumnMap.tsv"
WordMapFile="WordMap.tsv"
Security="N"

# The pivot-file which will be uploaded into the database
OutputFile="output.tsv"

###########################################################################
# Going to do the work. You should not have the change anything below here.

echo "Start re-arranging input..."
Rscript load_clinical_data.R    studyID=${StudyID} \
				columnMapFile=${ColumnMapFile} \
				wordMapFile=${WordMapFile} \
				outputFile=${OutputFile}
echo "re-arranged input stored in file: ${OutputFile}"
echo ""

echo "Start uploading file: ${OutputFile} into database (tm_lz.lt_src_clinical_data)"
psql -U tm_lz -w -d transmart <<EIND
  truncate TABLE tm_lz.lt_src_clinical_data;
  \\copy tm_lz.lt_src_clinical_data FROM '${OutputFile}' WITH (FORMAT CSV, DELIMITER E'\t', HEADER);
  select count(*) from tm_lz.lt_src_clinical_data;
EIND
echo "File: ${OutputFile} uploaded into database"
echo ""

echo "Call stored-procedure to put data into i2b2 tables"
psql -U tm_cz -w -d transmart <<END
select tm_cz.i2b2_load_clinical_data('${StudyID}', '${TopNode}', '${Security}') 
END
echo "All done."

