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
 export JAVA_HOME=/usr/local/jdk1.6.0_31
set -x
./data-integration/kitchen.sh -norep=Y /file=Kettle-ETL/load_gene_expression_data.kjb /log=load_gene_expression_data.log \
/param:DATA_FILE_PREFIX=xxx \
/param:DATA_LOCATION=$HOME/ETL/PostgreSQL/Data \
/param:DATA_TYPE=R \
/param:FilePivot_LOCATION=$HOME \
/param:JAVA_LOCATION=/usr/local/jdk1.6.0_31/bin \
/param:LOAD_TYPE=L \
/param:LOG_BASE=2 \
/param:MAP_FILENAME=xxx.txt \
/param:SAMPLE_REMAP_FILENAME=NOSAMPLEREMAP \
/param:SAMPLE_SUFFIX=.rma-Signal \
/param:SECURITY_REQUIRED=N \
/param:SORT_DIR=$HOME \
/param:SOURCE_CD=STD \
/param:BULK_LOADER_PATH=/usr/pgsql-9.1/bin/psql \
/param:STUDY_ID=GSE1234 \
/param:TOP_NODE='\Public Studies\GSE1234\' 
log_date=$(date +"%Y%m%d%H%M")
mv load_gene_expression_data.log logs\load_gene_expression_data_${log_date}.log
