********************************************************************************************************************************
*
*	This document pertains to the Kettle ETL pipeline February, 2013 release
*
*************************************************************************
* Copyright 2008-2012 Janssen Research & Development, LLC.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
******************************************************************

1.	The Kettle jobs and transformations have been tested with Pentaho Kettle Stable Release - 4.4.0.  
	Source is available from http://sourceforge.net/projects/pentaho/files/Data%20Integration/
	
2.	With this release, the Kettle ETL jobs and transformations have been updated to use a Kettle file-based repository
	The repository contains 5 directories/folders, Annotation, DSE, Metadata, Search, and Util.  These names are required.
	The recommended directory/folder structure for Kettle is:
		<Any directory/folder in your filesystem>
			.kettle				- directory/folder that contains the Kettle properties, schemas, and repository configuration files
				kettle.properties	from Kettle-Properties GitHub directory, update with your installation values
				schema.xml			from Kettle-Properties GitHub directory, no updates required
				repositories.xml	from Kettle-Properties GitHub directory, update with your installation values	
			data-integration	- directory/folder where Kettle application was unzipped
			Kettle-ETL
				Annotation
					jobs and transformations from the Annotation GitHub directory
				DSE
					jobs and transformations from the DSE GitHub directory
				Metadata
					jobs and transformations from the Metadata GitHub directory
				Search
					jobs and transformations from the Search GitHub directory
				Util
					jobs and transformations from the Util GitHub directory
			logs				- directory/folder where log files from Kettle job executions are stored
			scripts				- directory/folder where scripts to run Kettle jobs are located, .sh (Unix/Linux) or .bat (Windows) files
			
3.	Scripts have been updated to use the repository.  Each script has comments identifying the values that must be changed for your
	installation.  Windows .bat files have been added for Windows implementations.
	
				