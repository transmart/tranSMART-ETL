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
#	replace the values between the <...> with the appropriate values for your installation
#
set -x
export KETTLE_HOME=<fully-qualified path to directory where the .kettle directory is located>
export KETTLE_DIR=<fully-qualified path to Kettle data-integration directory>
log_date=$(date +"%Y%m%d%H%M")
$KETTLE_DIR/kitchen.sh /rep:1 /dir="/DSE" /job:"ETL.clinical.create_clinical_data" /user:admin /pass:admin -log=<path to your logs folder>/load_clinical_data_${log_date}.log \
-param:COLUMN_MAP_FILE=<name of column mapping file. \
-param:DATA_LOCATION=<fully-qualified path to directory where the data files are located>  \
-param:HIGHLIGHT_STUDY=N \
-param:LOAD_TYPE=I \
-param:RECORD_EXCLUSION_FILE=<name of record exclusion file or x if none> \
-param:SECURITY_REQUIRED=<Y if study is to be secured, N if not> \
-param:SORT_DIR=$KETTLE_HOME \
-param:STUDY_ID=<your study id> \
-param:WORD_MAP_FILE=<name of word mapping file or x if none> \
-param:TOP_NODE='\<your top node for the study>\' 

