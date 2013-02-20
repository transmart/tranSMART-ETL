rem *************************************************************************
rem  Copyright 2008-2012 Janssen Research & Development, LLC.
rem 
rem  Licensed under the Apache License, Version 2.0 (the "License");
rem  you may not use this file except in compliance with the License.
rem  You may obtain a copy of the License at
rem 
rem  http://www.apache.org/licenses/LICENSE-2.0
rem 
rem  Unless required by applicable law or agreed to in writing, software
rem  distributed under the License is distributed on an "AS IS" BASIS,
rem  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem  See the License for the specific language governing permissions and
rem  limitations under the License.
rem *****************************************************************/
rem   JAVA_HOME variable must be set if none exists globally
rem  set JAVA_HOME="C:\Program Files\Java\jdk1.6.0_22"
rem 
rem
rem		replace the values between the <...> with the appropriate values for your installation
rem
echo on
set KETTLE_HOME=<fully-qualified path to folder where the .kettle folder is located>
set KETTLE_DIR=<fully-qualified path to Kettle data-integration folder>
SET HOUR=%time:~0,2%
SET dtStamp9=%date:~-4%%date:~4,2%%date:~7,2%_0%time:~1,1%%time:~3,2%%time:~6,2% 
SET dtStamp24=%date:~-4%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%%time:~6,2%
if "%HOUR:~0,1%" == " " (SET dtStamp=%dtStamp9%) else (SET dtStamp=%dtStamp24%)
rem trim trailing spaces
set dtStamp=%dtStamp:~0,15%
%KETTLE_DIR%\kitchen.bat /rep:1 /dir="/Annotation" /job:"ETL.gex.load_GEX_annotation" /user:admin /pass:admin ^
-log=<fully-qualified name of your logs folder>\load_annotation_data_%dtStamp%.log ^
-param:DATA_LOCATION=<fully-qualified folder where data files are located> ^
-param:SOURCE_FILENAME=<filename of the annotation file> ^
-param:GPL_ID=<GPL number or other unique identifier for annotation> ^
-param:ANNOTATION_TITLE=<Enter the title of the annotation>^
-param:ANNOTATION_DATE=<Enter the annotation date format YYYY/MM/DD> ^
-param:ANNOTATION_RELEASE=<Enter the release number or leave empty> ^
-param:DATA_SOURCE=<A for annotation file, P for standard format input file>  ^
-param:PROBE_COL=<Enter the column number for the probe id> ^
-param:GENE_ID_COL=<Enter the column number for the gene id or -1 if not present> ^
-param:GENE_SYMBOL_COL=<Enter the column number for the gene symbol or -1 if not present> ^
-param:ORGANISM_COL=<Enter the column number for the organism or -1 if not present> ^
-param:SKIP_ROWS=<Enter number of rows to skip before processing annotation file> ^
-param:SORT_DIR=%KETTLE_HOME% ^
-param:LOAD_TYPE=I ^
-param:EMBEDDED_GENE_TABLE=<Y if gene symbol/id are in an embedded table, N if not> ^
-param:GENETAB_DELIM=<Enter the string that delimits fields in a embedded gene table, usually //> ^
-param:GENETAB_ID_COL=<Enter the column number of the gene id in the embedded gene table or -1 if not present> ^
-param:GENETAB_REC_DELIM=<Enter the string that delimits records in a embedded gene table, usually ///> ^
-param_GENETAB_SYMBOL_COL=<Enter the column number of the gene symbol in the embedded gene table or -1 if not present> 
