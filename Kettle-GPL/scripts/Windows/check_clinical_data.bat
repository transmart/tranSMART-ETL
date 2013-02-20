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
%KETTLE_DIR%\kitchen.bat /rep:1 /dir="/DSE" /job:"ETL.clinical.create_clinical_data" /user:admin /pass:admin ^
-log=<fully-qualified name of your logs folder>\load_clinical_data_%dtStamp%.log ^
-param:COLUMN_MAP_FILE=<filename of column mapping file> ^
-param:DATA_LOCATION=<fully-qualified folder where data files are located>  ^
-param:HIGHLIGHT_STUDY=N ^
-param:LOAD_TYPE=F ^
-param:RECORD_EXCLUSION_FILE=<filename of record exclusion file or x if none> ^
-param:SECURITY_REQUIRED=<Y if study is to be secured, N if not> ^
-param:SORT_DIR=%KETTLE_DIR% ^
-param:STUDY_ID=<your study id> ^
-param:WORD_MAP_FILE=<filename of word mapping file or x if none> ^
-param:TOP_NODE="\<your top node for the study>\\" 
rem NOTE NOTE NOTE
rem TOP_NODE must end with \\ because of Windows escape handling
