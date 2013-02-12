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
$KETTLE_DIR/kitchen.sh /rep:1 /dir="/Search" /job:"ETL.cohortAnalysis.process_analysis_data" /user:admin /pass:admin \
-log=<path to your logs folder>/process_analysis_data_${log_date}.log \
-param:DATA_LOCATION=<fully-qualified path to the directory where the data files are located>  \
-param:ANALYSIS_DATA_FILENAME=<filename for the analysis data file> \
-param:ANALYSIS_FILENAME=<filename for the analysis file> \
-param:COHORTS_FILENAME=<filename for the cohorts file> \
-param:LOG_FILENAME=<filename for the log file or x to use default log filename> \
-param:SAMPLES_FILENAME=<filename for the samples file> \
-param:STUDY_ID=<study id> \
-param:SORT_DIR=$KETTLE_HOME \
-param:STUDY_DATA_CATEGORY=Study \
-param:STUDY_DISPLAY_CATEGORY=<value that will be used to prefix the study in search all or GEO for Public Studies>
