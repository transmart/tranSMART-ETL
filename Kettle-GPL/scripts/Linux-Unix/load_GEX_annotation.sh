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
#
#	replace the values between the <...> with the appropriate values for your installation
#
set -x
export KETTLE_HOME=<fully-qualified path to directory where the .kettle directory is located>
export KETTLE_DIR=<fully-qualified path to Kettle data-integration directory>
log_date=$(date +"%Y%m%d%H%M")
$KETTLE_DIR/kitchen.sh /rep:1 /dir="/Annotation" /job:"ETL.gex.load_GEX_annotation" /user:admin /pass:admin \
-log=<path to your logs folder>/load_GEX_annotation_${log_date}.log \
-param:DATA_LOCATION=<fully-qualified folder where data files are located> \
-param:SOURCE_FILENAME=<filename of the annotation file> \
-param:GPL_ID=<GPL number or other unique identifier for annotation> \
-param:ANNOTATION_TITLE=<Enter the title of the annotation>\
-param:ANNOTATION_DATE=<Enter the annotation date format YYYY/MM/DD> \
-param:ANNOTATION_RELEASE=<Enter the release number or leave empty> \
-param:DATA_SOURCE=<A for annotation file, P for standard format input file>  \
-param:PROBE_COL=<Enter the column number for the probe id> \
-param:GENE_ID_COL=<Enter the column number for the gene id or -1 if not present> \
-param:GENE_SYMBOL_COL=<Enter the column number for the gene symbol or -1 if not present> \
-param:ORGANISM_COL=<Enter the column number for the organism or -1 if not present> \
-param:SKIP_ROWS=<Enter number of rows to skip before processing annotation file> \
-param:SORT_DIR=$KETTLE_HOME \
-param:LOAD_TYPE=I \
-param:EMBEDDED_GENE_TABLE=<Y if gene symbol/id are in an embedded table, N if not> \
-param:GENETAB_DELIM=<Enter the string that delimits fields in a embedded gene table, usually //> \
-param:GENETAB_ID_COL=<Enter the column number of the gene id in the embedded gene table or -1 if not present> \
-param:GENETAB_REC_DELIM=<Enter the string that delimits records in a embedded gene table, usually ///> \
-param_GENETAB_SYMBOL_COL=<Enter the column number of the gene symbol in the embedded gene table or -1 if not present> 
