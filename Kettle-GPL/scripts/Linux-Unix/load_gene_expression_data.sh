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
$KETTLE_DIR/kitchen.sh /rep:1 /dir="/DSE" /job:"ETL.gex.load_gene_expression_data" /user:admin /pass:admin /log=<path to your logs folder>/load_gene_expression_data_${log_date}.log \
/param:DATA_FILE_PREFIX=<common prefix of gene expression data files> \
/param:DATA_LOCATION=<fully-qualified path to the directory where the data files are located> \
/param:DATA_TYPE=<R-Raw data, L-log-transformed data, T-fully transformed data> \
/param:FilePivot_LOCATION=<fully-qualified path to the directory where FilePivot.jar is located> \
/param:JAVA_LOCATION=$JAVA_HOME \
/param:LOAD_TYPE=I \
/param:LOG_BASE=<log base for log-transformed data> \
/param:MAP_FILENAME=<name of subject-sample mapping file> \
/param:SAMPLE_REMAP_FILENAME=<name of sample remap file or NOSAMPLEREMAP if none> \
/param:SAMPLE_SUFFIX=<string to be removed from the sample_cd in the gene expression data or .rma-Signal if none> \
/param:SECURITY_REQUIRED=<Y if study is to be secured, N if not> \
/param:SORT_DIR=$KETTLE_HOME \
/param:SOURCE_CD=<unique identifier if multiple analyses of gene expression data are loaded for study, STD if not> \
/param:STUDY_ID=<your study id> \
/param:TOP_NODE='\<your top node for the study>\' 
