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
set KETTLE_HOME=C:\Users\javitabile
set KETTLE_DIR=C:\Users\javitabile\Documents\Kettle-4.4\data-integration
SET HOUR=%time:~0,2%
SET dtStamp9=%date:~-4%%date:~4,2%%date:~7,2%_0%time:~1,1%%time:~3,2%%time:~6,2% 
SET dtStamp24=%date:~-4%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%%time:~6,2%
if "%HOUR:~0,1%" == " " (SET dtStamp=%dtStamp9%) else (SET dtStamp=%dtStamp24%)
rem trim trailing spaces
set dtStamp=%dtStamp:~0,15%
%KETTLE_DIR%\kitchen.bat /rep:1 /dir="/Metadata" /job:"ETL.metadata.Load_Study_Metadata" /user:admin /pass:admin ^
-log=<fully-qualified name of your logs folder>\load_study_metadata_%dtStamp%.log ^
-param:METADATA_FILENAME=<filename of metadata file> ^
-param:METADATA_LOCATION=<fully-qualified folder where data files are located> ^
-param:SORT_DIR=%KETTLE_HOME% 
