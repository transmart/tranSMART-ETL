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
export ORACLE_HOME=/u01/app/oracle/product/11.2.0/dbhome_1
set -x
log_date=$(date +"%Y%m%d%H%M")
../data-integration/kitchen.sh -norep=Y -file=../Kettle-ETL/ETL.search.process_analysis_data.kjb -log=../logs/process_analysis_data_${log_date}.log \
-param:DATA_LOCATION=/home/transmart/ETL/data/GSE4302  \
-param:ANALYSIS_DATA_FILENAME=analysis_data_ext.txt \
-param:ANALYSIS_FILENAME=rwg_analysis_ext.txt \
-param:COHORTS_FILENAME=rwg_cohort_ext.txt \
-param:LOG_FILENAME=x \
-param:SAMPLES_FILENAME=rwg_sample_ext.txt \
-param:STUDY_ID=GSE4302 \
-param:SORT_DIR=/home/transmart/ETL \
-param:STUDY_DATA_CATEGORY=Study \
-param:STUDY_DISPLAY_CATEGORY=GEO
