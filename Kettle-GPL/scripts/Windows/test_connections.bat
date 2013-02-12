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
%KETTLE_DIR%\kitchen.bat /rep:1 /dir="/Util" /job:"ETL.Util.test_connections" /user:admin /pass:admin 

