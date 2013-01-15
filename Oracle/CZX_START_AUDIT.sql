set define off;
create or replace
PROCEDURE         "CZX_START_AUDIT" 
(V_JOB_NAME IN VARCHAR2 DEFAULT NULL ,
  V_DATABASE_NAME IN VARCHAR2 DEFAULT NULL ,
  V_JOB_ID OUT NUMBER)
  AUTHID CURRENT_USER  
IS 
  PRAGMA AUTONOMOUS_TRANSACTION;
/*************************************************************************
* Copyright 2008-2012 Janssen Research and Development, LLC.
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
******************************************************************/

BEGIN

   INSERT INTO CZ_JOB_MASTER
     ( START_DATE, 
		ACTIVE, 
		DATABASE_NAME, 
		JOB_NAME, 
		JOB_STATUS )
     VALUES (
		SYSDATE, 
		'Y', 
		V_DATABASE_NAME, 
		V_JOB_NAME, 
		'Running' )

	RETURNING JOB_ID INTO V_JOB_ID;
	
	COMMIT;
  
EXCEPTION
    WHEN OTHERS THEN ROLLBACK;
END;