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
set KETTLE_HOME=<fully-qualified path to directory where the .kettle directory is located>
set KETTLE_DIR=<fully-qualified path to Kettle data-integration directory>
SET HOUR=%time:~0,2%
SET dtStamp9=%date:~-4%%date:~4,2%%date:~7,2%_0%time:~1,1%%time:~3,2%%time:~6,2% 
SET dtStamp24=%date:~-4%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%%time:~6,2%
if "%HOUR:~0,1%" == " " (SET dtStamp=%dtStamp9%) else (SET dtStamp=%dtStamp24%)
rem trim trailing spaces
set dtStamp=%dtStamp:~0,15%
%KETTLE_DIR%\kitchen.bat /rep:1 /dir="/DSE" /job:"ETL.gex.load_gene_expression_data" /user:admin /pass:admin -log=C:\Users\javitabile\Documents\tranSMART_GPL\Test_Data\logs\load_gex_data_%dtStamp%.log ^
-param:DATA_FILE_PREFIX=<common prefix of gene expression data files> ^
-param:DATA_LOCATION=<fully-qualified path to the directory where the data files are located> ^
-param:DATA_TYPE=<R-Raw data, L-log-transformed data, T-fully transformed data> ^
-param:FilePivot_LOCATION=<fully-qualified path to the directory where FilePivot.jar is located> ^
-param:JAVA_LOCATION=<fully-qualified path to Java bin folder> ^
-param:LOAD_TYPE=I ^
-param:LOG_BASE=<log base for log-transformed data> ^
-param:MAP_FILENAME=<name of subject-sample mapping file> ^
-param:SAMPLE_REMAP_FILENAME=<name of sample remap file or NOSAMPLEREMAP if none> ^
-param:SAMPLE_SUFFIX=<string to be removed from the sample_cd in the gene expression data or .rma-Signal if none> ^
-param:SECURITY_REQUIRED=<Y if study is to be secured, N if not> ^
-param:SORT_DIR=%KETTLE_HOME% ^
-param:SOURCE_CD=<unique identifier if multiple analyses of gene expression data are loaded for study, STD if not> ^
-param:STUDY_ID=<your study id> ^
-param:TOP_NODE="<your top node for the study>\\" 
rem NOTE NOTE NOTE
rem TOP_NODE must end with \\ because of Windows escape handling
