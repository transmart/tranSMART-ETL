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
#  finds DOS in any transformation and changes it to UNIX
#  This is used when transformations are developed on Windows but are executed on UNIX/Linux systems
#  The Windows line terminators are CRLF, for UNIX/Linux, the termination is only the LF
for f in `find . -name "*.ktr" -type f -exec grep -l '<format>DOS' {} \;`
do
        sed -i 's/<format>DOS/<format>UNIX/' $f
        echo updated $f
done
#
#	added dos2unix for Kettle jobs and transformations in case they were unzipped from a Windows zip file
#
for f in `ls *.k*`
do
	dos2unix $f
done
