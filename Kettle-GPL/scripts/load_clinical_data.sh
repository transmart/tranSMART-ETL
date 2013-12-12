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
set -x
./data-integration/kitchen.sh -norep=Y -file=Kettle-ETL/create_clinical_data.kjb -log=load_clinical_data.log \
-param:COLUMN_MAP_FILE=x \
-param:DATA_LOCATION=/data/directory  \
-param:LOAD_TYPE=I \
-param:SECURITY_REQUIRED=N \
-param:SORT_DIR=$HOME \
-param:SQLLDR_PATH=$ORACLE_HOME/bin/sqlldr \
-param:STUDY_ID=GSE1234 \
-param:TOP_NODE='\Public Studies\GSE1234\' \
-param:WORD_MAP_FILE=x \
-param:RECORD_EXCLUSION_FILE=x
log_date=$(date +"%Y%m%d%H%M")
mv load_clinical_data.log logs/load_clinical_data_${log_date}.log
