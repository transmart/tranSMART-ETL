#*************************************************************************
# Copyright 2008-2012 Janssen Research & Development, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*****************************************************************/
#  JAVA_HOME variable must be set if none exists globally
# export JAVA_HOME=/usr/local/jdk1.6.0_27
#
#	the parameters are set for using an annotation file from Affymetrix (convert csv to tab-delimited text in Excel and delete all of the top rows except
#	for the column header row (right before the data)
#	Be sure to check the columns before running.
#	Also check the gene_id column for a date and remove all dates
set -x
./data-integration/kitchen.sh -norep=Y -file=Kettle-ETL/load_annotation.kjb -log=load_annotation.log \
-param:DATA_LOCATION=$HOME/Annotation \
-param:SOURCE_FILENAME=x \
-param:GPL_ID=GPLxxx \
-param:ANNOTATION_TITLE= \
-param:ANNOTATION_DATE=YYYY/MM/DD \
-param:ANNOTATION_RELEASE= \
-param:DATA_SOURCE=A  \
-param:PROBE_COL=1 \
-param:GENE_ID_COL=19 \
-param:GENE_SYMBOL_COL=15 \
-param:ORGANISM_COL=3 \
-param:SKIP_ROWS=1 \
-param:SORT_DIR=$HOME \
-param:LOAD_TYPE=L \
-param:SQLLDR_PATH=$ORACLE_HOME/bin/sqlldr \
-param:EMBEDDED_GENE_TABLE=N \
-param:GENETAB_DELIM=// \
-param:GENETAB_ID_COL=-1 \
-param:GENETAB_REC_DELIM=/// \
-param_GENETAB_SYMBOL_COL=-1 
log_date=$(date +"%Y%m%d%H%M")
mv load_annotation.log logs/load_annotation_${log_date}.log
