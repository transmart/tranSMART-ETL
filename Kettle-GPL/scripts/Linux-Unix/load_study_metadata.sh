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
$KETTLE_DIR/kitchen.sh /rep:1 /dir="/Metadata" /job:"ETL.metadata.Load_Study_Metadata" /user:admin /pass:admin -log=<path to your logs folder> \
-param:METADATA_FILENAME=<filename of metadata file> \
-param:METADATA_LOCATION=<fully-qualified path to directory where data files are located> \
-param:SORT_DIR=$KETTLE_HOME 
